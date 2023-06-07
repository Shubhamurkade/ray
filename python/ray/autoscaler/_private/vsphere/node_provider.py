import copy
from email import headerregistry
import logging
from socket import if_indextoname
import sys
import threading
import time
from collections import OrderedDict, defaultdict
from typing import Any, Dict, List

import ray._private.ray_constants as ray_constants

from ray.autoscaler._private.cli_logger import cf, cli_logger
from ray.autoscaler._private.constants import VSPHERE_MAX_RETRIES
from ray.autoscaler._private.log_timer import LogTimer
from ray.autoscaler.node_launch_exception import NodeLaunchException
from ray.autoscaler.node_provider import NodeProvider
from ray.autoscaler.tags import (
    TAG_RAY_CLUSTER_NAME,
    TAG_RAY_LAUNCH_CONFIG,
    TAG_RAY_NODE_KIND,
    TAG_RAY_NODE_NAME,
    TAG_RAY_USER_NODE_TYPE,
)
from vmware.vapi.vsphere.client import create_vsphere_client
import ssl
import requests
from com.vmware.content_client import Library
import json
from com.vmware.content.library_client import ItemModel
from com.vmware.vcenter_client import ResourcePool, Folder
from com.vmware.vcenter.ovf_client import LibraryItem
import uuid
from com.vmware.vcenter_client import VM
from com.vmware.vcenter.vm_client import Power as HardPower
from com.vmware.vapi.std_client import DynamicID

from ray.autoscaler.tags import NODE_KIND_HEAD, NODE_KIND_WORKER

logger = logging.getLogger(__name__)

TAG_BATCH_DELAY = 1

def get_unverified_session():
    """
    Get a requests session with cert verification disabled.
    Also disable the insecure warnings message.
    Note this is not recommended in production code.
    @return: a requests session with verification disabled.
    """
    session = requests.session()
    session.verify = False
    requests.packages.urllib3.disable_warnings()
    return session

def to_aws_format(tags):
    """Convert the Ray node name tag to the AWS-specific 'Name' tag."""

    if TAG_RAY_NODE_NAME in tags:
        tags["Name"] = tags[TAG_RAY_NODE_NAME]
        del tags[TAG_RAY_NODE_NAME]
    return tags


def from_aws_format(tags):
    """Convert the AWS-specific 'Name' tag to the Ray node name tag."""

    if "Name" in tags:
        tags[TAG_RAY_NODE_NAME] = tags["Name"]
        del tags["Name"]
    return tags

@staticmethod
def bootstrap_config(cluster_config):
    return cluster_config
    
def make_vsphere_vm():
    pass

def list_ec2_instances(
    region: str, aws_credentials: Dict[str, Any] = None
) -> List[Dict[str, Any]]:
    pass

RAY_HEAD_TAG = "ray-head"
RAY_WORKER_TAG = "ray-worker"
tag_to_id = {RAY_HEAD_TAG: "urn:vmomi:InventoryServiceTag:c24d3cc8-8ca3-422c-8836-1623e94f7d07:GLOBAL", 
             RAY_WORKER_TAG: "urn:vmomi:InventoryServiceTag:028b3f5a-21ab-4b4f-968b-4c8869ef1b39:GLOBAL"
             }

tag_id_to_tag = {
    "urn:vmomi:InventoryServiceTag:c24d3cc8-8ca3-422c-8836-1623e94f7d07:GLOBAL": RAY_HEAD_TAG, 
    "urn:vmomi:InventoryServiceTag:028b3f5a-21ab-4b4f-968b-4c8869ef1b39:GLOBAL": RAY_WORKER_TAG
}

class VsphereNodeProvider(NodeProvider):
    max_terminate_nodes = 1000

    def __init__(self, provider_config, cluster_name):
        NodeProvider.__init__(self, provider_config, cluster_name)
        self.cache_stopped_nodes = provider_config.get("cache_stopped_nodes", True)
        vsphere_credentials = provider_config.get("vsphere_config")

        session = get_unverified_session()
        self.vsphere_client = create_vsphere_client(
            server=vsphere_credentials["server"],
            username=vsphere_credentials["admin_user"],
            password=vsphere_credentials["admin_password"],
            session=session
        )

        # Tags that we believe to actually be on EC2.
        self.tag_cache = {}
        # Tags that we will soon upload.
        self.tag_cache_pending = defaultdict(dict)
        # Number of threads waiting for a batched tag update.
        self.batch_thread_count = 0
        self.batch_update_done = threading.Event()
        self.batch_update_done.set()
        self.ready_for_new_batch = threading.Event()
        self.ready_for_new_batch.set()
        self.tag_cache_lock = threading.Lock()
        self.count_lock = threading.Lock()

        # Cache of node objects from the last nodes() call. This avoids
        # excessive DescribeInstances requests.
        self.cached_nodes = {}

    def non_terminated_nodes(self, tag_filters):
        
        nodes = []
        vms = self.vsphere_client.vcenter.VM.list(VM.FilterSpec())

        for vm in vms:

            vm_param = vm.vm
            yn_id = DynamicID(type="VirtualMachine", id=vm_param)

            print("vm is %s"%(vm_param))
            vm_info = self.vsphere_client.vcenter.VM.get(vm_param)
            for tag_id in self.vsphere_client.tagging.TagAssociation.list_attached_tags(yn_id):
                print("tag attached: %s"%(tag_id))

                if len(tag_filters) == 0:
                    nodes.append(vm_info.name)
                elif tag_id in tag_id_to_tag and tag_id_to_tag[tag_id] == tag_filters[0]:
                    nodes.append(vm_info.name)

        return nodes

    def is_running(self, node_id):
        node = self._get_cached_node(node_id)
        return node.state["Name"] == "running"

    def is_terminated(self, node_id):
        return False
        node = self._get_cached_node(node_id)
        state = node.state["Name"]
        return state not in ["running", "pending"]

    def does_vm_contain_tag(self, node_id, tag_id_to_compare):
        vms = self.vsphere_client.vcenter.VM.list(VM.FilterSpec(names={node_id}))
 
        vm_param = vms[0].vm

        yn_id = DynamicID(type="VirtualMachine", id=vm_param)

        for tag_id in self.vsphere_client.tagging.TagAssociation.list_attached_tags(yn_id):
            if tag_id == tag_id_to_compare:
                return True
        
        return False
            
    def node_tags(self, node_id):
        vms = self.vsphere_client.vcenter.VM.list(VM.FilterSpec(names={node_id}))

        vm_param = vms[0].vm
        yn_id = DynamicID(type="VirtualMachine", id=vm_param)
        for tag_id in self.vsphere_client.tagging.TagAssociation.list_attached_tags(yn_id):
            print("tag attached: %s on vm: %s"%(tag_id, node_id))
            if tag_id == "urn:vmomi:InventoryServiceTag:c24d3cc8-8ca3-422c-8836-1623e94f7d07:GLOBAL":
                return NODE_KIND_HEAD
            elif tag_id == "urn:vmomi:InventoryServiceTag:028b3f5a-21ab-4b4f-968b-4c8869ef1b39:GLOBAL":
                return NODE_KIND_WORKER

    def external_ip(self, node_id):

        # node_id and vm_name are same for vsphere
        vm = self.get_vm(node_id)

        # Return the external IP of the VM
        print("External ip vm %s"%(vm))
        vm = self.vsphere_client.vcenter.vm.guest.Identity.get(vm)

        return vm.ip_address

    def internal_ip(self, node_id):
        if node.private_ip_address is None:
            node = self._get_node(node_id)

        return node.private_ip_address

    def set_node_tags(self, node_id, tags):

        return
        is_batching_thread = False
        with self.tag_cache_lock:
            if not self.tag_cache_pending:
                is_batching_thread = True
                # Wait for threads in the last batch to exit
                self.ready_for_new_batch.wait()
                self.ready_for_new_batch.clear()
                self.batch_update_done.clear()
            self.tag_cache_pending[node_id].update(tags)

        if is_batching_thread:
            time.sleep(TAG_BATCH_DELAY)
            with self.tag_cache_lock:
                self._update_node_tags()
                self.batch_update_done.set()

        with self.count_lock:
            self.batch_thread_count += 1
        self.batch_update_done.wait()

        with self.count_lock:
            self.batch_thread_count -= 1
            if self.batch_thread_count == 0:
                self.ready_for_new_batch.set()

    def _update_node_tags(self):
        batch_updates = defaultdict(list)

        for node_id, tags in self.tag_cache_pending.items():
            for x in tags.items():
                batch_updates[x].append(node_id)
            self.tag_cache[node_id].update(tags)

        self.tag_cache_pending = defaultdict(dict)

        self._create_tags(batch_updates)

    def _create_tags(self, batch_updates):
        for (k, v), node_ids in batch_updates.items():
            m = "Set tag {}={} on {}".format(k, v, node_ids)
            with LogTimer("AWSNodeProvider: {}".format(m)):
                if k == TAG_RAY_NODE_NAME:
                    k = "Name"
                self.ec2.meta.client.create_tags(
                    Resources=node_ids,
                    Tags=[{"Key": k, "Value": v}],
                )

    def create_node(self, node_config, tags, count) -> Dict[str, Any]:
        """Creates instances.

        Returns dict mapping instance id to ec2.Instance object for the created
        instances.
        """
        
        if count:
            created_nodes_dict = self._create_node(node_config, tags, count)

        # all_created_nodes = reused_nodes_dict
        all_created_nodes = {}
        all_created_nodes.update(created_nodes_dict)
        return all_created_nodes

    @staticmethod
    def _merge_tag_specs(
        tag_specs: List[Dict[str, Any]], user_tag_specs: List[Dict[str, Any]]
    ) -> None:
        """
        Merges user-provided node config tag specifications into a base
        list of node provider tag specifications. The base list of
        node provider tag specs is modified in-place.

        This allows users to add tags and override values of existing
        tags with their own, and only applies to the resource type
        "instance". All other resource types are appended to the list of
        tag specs.

        Args:
            tag_specs (List[Dict[str, Any]]): base node provider tag specs
            user_tag_specs (List[Dict[str, Any]]): user's node config tag specs
        """

        for user_tag_spec in user_tag_specs:
            if user_tag_spec["ResourceType"] == "instance":
                for user_tag in user_tag_spec["Tags"]:
                    exists = False
                    for tag in tag_specs[0]["Tags"]:
                        if user_tag["Key"] == tag["Key"]:
                            exists = True
                            tag["Value"] = user_tag["Value"]
                            break
                    if not exists:
                        tag_specs[0]["Tags"] += [user_tag]
            else:
                tag_specs += [user_tag_spec]

    def get_vm(self, vm_name):
        """
        Return the identifier of a vm
        Note: The method assumes that there is only one vm with the mentioned name.
        """
        names = set([vm_name])
        vms = self.vsphere_client.vcenter.VM.list(VM.FilterSpec(names=names))

        if len(vms) == 0:
            print("VM with name ({}) not found".format(vm_name))
            return None

        vm = vms[0].vm
        print("Found VM '{}' ({})".format(vm_name, vm))

        return vm

    def attach_tag(self, inv_obj, inv_type, tag_id):
        dyn_id = DynamicID(type=inv_type, id=inv_obj)
        try:
            self.vsphere_client.tagging.TagAssociation.attach(tag_id, dyn_id)
        except Exception as e:
            print("Check that the tag is associable to {}".format(inv_type))
            raise e
        
    def _create_node(self, node_config, tags, count):
        created_nodes_dict = {}

        for i in range(count):
        
            folder_filter_spec = Folder.FilterSpec(names=set(["folder-WCP_DC"]))
            folder_summaries = self.vsphere_client.vcenter.Folder.list(folder_filter_spec)
            if not folder_summaries:
                raise ValueError("Folder with name '{}' not found".
                                format(self.foldername))
            folder_id = folder_summaries[0].folder
            cli_logger.print('Folder ID: {}'.format(folder_id))
        
            rp_filter_spec = ResourcePool.FilterSpec(names=set(["ray"]))
            resource_pool_summaries = self.vsphere_client.vcenter.ResourcePool.list(rp_filter_spec)
            if not resource_pool_summaries:
                raise ValueError("Resource pool with name '{}' not found".
                                format(self.resourcepoolname))
            resource_pool_id = resource_pool_summaries[0].resource_pool

            cli_logger.print('Resource pool ID: {}'.format(resource_pool_id))

            deployment_target = LibraryItem.DeploymentTarget(
                resource_pool_id=resource_pool_id
            )

            lib_item_id = "7534d35a-2d9c-4a4f-9ecd-0e1f4f6363ec"
            ovf_summary = self.vsphere_client.vcenter.ovf.LibraryItem.filter(
                ovf_library_item_id=lib_item_id,
                target=deployment_target)
            cli_logger.print('Found an OVF template: {} to deploy.'.format(ovf_summary.name))

            vm_name = "ray-node"+str(uuid.uuid4())
            # Build the deployment spec
            deployment_spec = LibraryItem.ResourcePoolDeploymentSpec(
                name=vm_name,
                annotation=ovf_summary.annotation,
                accept_all_eula=True,
                network_mappings=None,
                storage_mappings=None,
                storage_provisioning=None,
                storage_profile_id=None,
                locale=None,
                flags=None,
                additional_parameters=None,
                default_datastore_id=None)

            # Deploy the ovf template
            result = self.vsphere_client.vcenter.ovf.LibraryItem.deploy(
                lib_item_id,
                deployment_target,
                deployment_spec,
                client_token=str(uuid.uuid4()))

            # The type and ID of the target deployment is available in the deployment result.
            if result.succeeded:
                cli_logger.print('Deployment successful. VM Name: "{}", ID: "{}"'
                    .format(vm_name, result.resource_id.id))
                self.vm_id = result.resource_id.id
                error = result.error
                if error is not None:
                    for warning in error.warnings:
                        cli_logger.print('OVF warning: {}'.format(warning.message))

            else:
                cli_logger.print('Deployment failed.')
                for error in result.error.errors:
                    cli_logger.print('OVF error: {}'.format(error.message))
                
            vm = self.get_vm(vm_name)
            status = self.vsphere_client.vcenter.vm.Power.get(vm)
            if status != HardPower.Info(state=HardPower.State.POWERED_ON):
                cli_logger.print("Powering on VM")
                self.vsphere_client.vcenter.vm.Power.start(vm)
                cli_logger.print('vm.Power.start({})'.format(vm))

            for tag in tags:
                self.attach_tag(vm, "VirtualMachine", tag_to_id[tag])
           
            created_nodes_dict[vm_name] = vm
        
        return created_nodes_dict

    def terminate_node(self, node_id):
        node = self._get_cached_node(node_id)
        if self.cache_stopped_nodes:
            if node.spot_instance_request_id:
                cli_logger.print(
                    "Terminating instance {} "
                    + cf.dimmed("(cannot stop spot instances, only terminate)"),
                    node_id,
                )  # todo: show node name?
                node.terminate()
            else:
                cli_logger.print(
                    "Stopping instance {} "
                    + cf.dimmed(
                        "(to terminate instead, "
                        "set `cache_stopped_nodes: False` "
                        "under `provider` in the cluster configuration)"
                    ),
                    node_id,
                )  # todo: show node name?
                node.stop()
        else:
            node.terminate()


    def terminate_nodes(self, node_ids):
        if not node_ids:
            return

        terminate_instances_func = self.ec2.meta.client.terminate_instances
        stop_instances_func = self.ec2.meta.client.stop_instances

        # In some cases, this function stops some nodes, but terminates others.
        # Each of these requires a different EC2 API call. So, we use the
        # "nodes_to_terminate" dict below to keep track of exactly which API
        # call will be used to stop/terminate which set of nodes. The key is
        # the function to use, and the value is the list of nodes to terminate
        # with that function.
        nodes_to_terminate = {terminate_instances_func: [], stop_instances_func: []}

        if self.cache_stopped_nodes:
            spot_ids = []
            on_demand_ids = []

            for node_id in node_ids:
                if self._get_cached_node(node_id).spot_instance_request_id:
                    spotð_ids += [node_id]
                else:
                    on_demand_ids += [node_id]

            if on_demand_ids:
                # todo: show node names?
                cli_logger.print(
                    "Stopping instances {} "
                    + cf.dimmed(
                        "(to terminate instead, "
                        "set `cache_stopped_nodes: False` "
                        "under `provider` in the cluster configuration)"
                    ),
                    cli_logger.render_list(on_demand_ids),
                )

            if spot_ids:
                cli_logger.print(
                    "Terminating instances {} "
                    + cf.dimmed("(cannot stop spot instances, only terminate)"),
                    cli_logger.render_list(spot_ids),
                )

            nodes_to_terminate[stop_instances_func] = on_demand_ids
            nodes_to_terminate[terminate_instances_func] = spot_ids
        else:
            nodes_to_terminate[terminate_instances_func] = node_ids

        max_terminate_nodes = (
            self.max_terminate_nodes
            if self.max_terminate_nodes is not None
            else len(node_ids)
        )

        for terminate_func, nodes in nodes_to_terminate.items():
            for start in range(0, len(nodes), max_terminate_nodes):
                terminate_func(InstanceIds=nodes[start : start + max_terminate_nodes])

    def _get_node(self, node_id):
        """Refresh and get info for this node, updating the cache."""
        self.non_terminated_nodes({})  # Side effect: updates cache

        if node_id in self.cached_nodes:
            return self.cached_nodes[node_id]

        # Node not in {pending, running} -- retry with a point query. This
        # usually means the node was recently preempted or terminated.
        matches = list(self.ec2.instances.filter(InstanceIds=[node_id]))
        assert len(matches) == 1, "Invalid instance id {}".format(node_id)
        return matches[0]

    def _get_cached_node(self, node_id):
        """Return node info from cache if possible, otherwise fetches it."""
        if node_id in self.cached_nodes:
            return self.cached_nodes[node_id]

        return self._get_node(node_id)

    @staticmethod
    def fillout_available_node_types_resources(
        cluster_config: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Fills out missing "resources" field for available_node_types."""
        if "available_node_types" not in cluster_config:
            return cluster_config
        cluster_config = copy.deepcopy(cluster_config)

        instances_list = list_ec2_instances(
            cluster_config["provider"]["region"],
            cluster_config["provider"].get("aws_credentials"),
        )
        instances_dict = {
            instance["InstanceType"]: instance for instance in instances_list
        }
        available_node_types = cluster_config["available_node_types"]
        head_node_type = cluster_config["head_node_type"]
        for node_type in available_node_types:
            instance_type = available_node_types[node_type]["node_config"][
                "InstanceType"
            ]
            if instance_type in instances_dict:
                cpus = instances_dict[instance_type]["VCpuInfo"]["DefaultVCpus"]

                autodetected_resources = {"CPU": cpus}
                if node_type != head_node_type:
                    # we only autodetect worker node type memory resource
                    memory_total = instances_dict[instance_type]["MemoryInfo"][
                        "SizeInMiB"
                    ]
                    memory_total = int(memory_total) * 1024 * 1024
                    prop = 1 - ray_constants.DEFAULT_OBJECT_STORE_MEMORY_PROPORTION
                    memory_resources = int(memory_total * prop)
                    autodetected_resources["memory"] = memory_resources

                gpus = instances_dict[instance_type].get("GpuInfo", {}).get("Gpus")
                if gpus is not None:
                    # TODO(ameer): currently we support one gpu type per node.
                    assert len(gpus) == 1
                    gpu_name = gpus[0]["Name"]
                    autodetected_resources.update(
                        {"GPU": gpus[0]["Count"], f"accelerator_type:{gpu_name}": 1}
                    )
                autodetected_resources.update(
                    available_node_types[node_type].get("resources", {})
                )
                if autodetected_resources != available_node_types[node_type].get(
                    "resources", {}
                ):
                    available_node_types[node_type][
                        "resources"
                    ] = autodetected_resources
                    logger.debug(
                        "Updating the resources of {} to {}.".format(
                            node_type, autodetected_resources
                        )
                    )
            else:
                raise ValueError(
                    "Instance type "
                    + instance_type
                    + " is not available in AWS region: "
                    + cluster_config["provider"]["region"]
                    + "."
                )
        return cluster_config
