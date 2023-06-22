import copy
import logging
from socket import if_indextoname
import threading
import time
from collections import OrderedDict, defaultdict
from typing import Any, Dict, List
from threading import RLock

import ray._private.ray_constants as ray_constants

from ray.autoscaler._private.cli_logger import cf, cli_logger
from ray.autoscaler._private.constants import VSPHERE_MAX_RETRIES
from ray.autoscaler._private.log_timer import LogTimer
from ray.autoscaler.node_provider import NodeProvider
from ray.autoscaler.tags import (
    TAG_RAY_CLUSTER_NAME,
    TAG_RAY_LAUNCH_CONFIG,
    TAG_RAY_NODE_KIND,
    TAG_RAY_NODE_NAME,
    TAG_RAY_USER_NODE_TYPE,
)
from vmware.vapi.vsphere.client import create_vsphere_client
import requests
from com.vmware.vcenter_client import ResourcePool, Folder
from com.vmware.vcenter.ovf_client import LibraryItem
import uuid
from com.vmware.vcenter_client import VM
from com.vmware.vcenter.vm_client import Power as HardPower
from com.vmware.vapi.std_client import DynamicID
import com.vmware.vapi.std.errors_client as ErrorClients


logger = logging.getLogger(__name__)

TAG_BATCH_DELAY = 1
TYPE_OF_RESOURCE = "VirtualMachine"
NODE_CATEGORY = "ray"

def to_vsphere_format(tags):
    """Convert the Ray node name tag to the vSphere-specific 'Name' tag."""

    if TAG_RAY_NODE_NAME in tags:
        tags["Name"] = tags[TAG_RAY_NODE_NAME]
        del tags[TAG_RAY_NODE_NAME]
    return tags


def from_vsphere_format(tags):
    """Convert the vSphere-specific 'Name' tag to the Ray node name tag."""

    if "Name" in tags:
        tags[TAG_RAY_NODE_NAME] = tags["Name"]
        del tags["Name"]
    return tags

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

@staticmethod
def bootstrap_config(cluster_config):
    return cluster_config
    
def make_vsphere_vm():
    pass


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

        # Tags that we believe to actually be on VM.
        self.tag_cache = {}
        # Tags that we will soon upload.
        self.tag_cache_pending = defaultdict(dict)
        self.lock = RLock()

        # Cache of node objects from the last nodes() call. This avoids
        # excessive DescribeInstances requests.
        self.cached_nodes : Dict[str, VM] = {}

    def non_terminated_nodes(self, tag_filters):
        with self.lock:
            nodes = []
            # Get all POWERED_ON VMs
            cli_logger.info('Getting non terminated nodes...')
            vms = self.vsphere_client.vcenter.VM.list(VM.FilterSpec(power_states={HardPower.State.POWERED_ON}))
            cli_logger.info(f'Got {len(vms)} non terminated nodes.')
            filters = tag_filters.copy()
            if TAG_RAY_CLUSTER_NAME not in tag_filters:
                filters[TAG_RAY_CLUSTER_NAME] = self.cluster_name
            
            for vm in vms:
                vm_id = vm.vm
                yn_id = DynamicID(type=TYPE_OF_RESOURCE, id=vm.vm)
                
                tags = {}
                matched_tags = 0
                # If the VM has a tag from tag_filter then select it 
                for tag_id in self.vsphere_client.tagging.TagAssociation.list_attached_tags(yn_id):
                    vsphere_vm_tag = self.vsphere_client.tagging.Tag.get(tag_id=tag_id).name 
                    if ':' in vsphere_vm_tag:
                        tag_key_value = vsphere_vm_tag.split(':')
                        tag_key = tag_key_value[0]
                        tag_value = tag_key_value[1]
                        if tag_key in filters and tag_value == filters[tag_key]:
                            matched_tags += 1
                            tags[tag_key] = tag_value
                        
                if matched_tags == len(filters):
                    self.tag_cache[vm_id] = tags
                    # refresh cached_nodes with latest information e.g external ip 
                    if vm_id in self.cached_nodes:
                        del self.cached_nodes[vm_id]
                    self.cached_nodes[vm_id] = vm
                    nodes.append(vm_id)
            
            cli_logger.info(f'Nodes are {nodes}') 
            return nodes

    def is_running(self, node_id):
        node = self._get_cached_node(node_id)
        return node.power_state in {HardPower.State.POWERED_ON}

    def is_terminated(self, node_id):
        node = self._get_cached_node(node_id)
        return node.power_state not in {HardPower.State.POWERED_ON}

    def node_tags(self, node_id):
        with self.tag_cache_lock:
            d1 = self.tag_cache[node_id]
            return dict(d1)

    def external_ip(self, node_id):
        # node_id and vm_name are same for vsphere
        vm = self.get_vm(self.vsphere_client, node_id)

        # Return the external IP of the VM
        cli_logger.info("External ip vm %s"%(vm))
        vm = self.vsphere_client.vcenter.vm.guest.Identity.get(vm.vm)

        return vm.ip_address

    def internal_ip(self, node_id):
        if node.private_ip_address is None:
            node = self._get_node(node_id)

        return node.private_ip_address

    def set_node_tags(self, node_id, tags):
        # This method gets called from the Ray and it passes
        # node_id which needs to be vm.vm and not vm.name
        with self.lock:
            category_id = self.get_category()
            if not category_id:
                category_id = self.create_category()

            for key, value in tags.items():
                tag = key+':'+value
                tag_id = self.get_tag(tag, category_id )
                if not tag_id:
                    tag_id = self.create_node_tag(tag, category_id)
                self.attach_tag(node_id, TYPE_OF_RESOURCE, tag_id=tag_id)

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

        Returns dict mapping instance id to VM object for the created
        instances.
        """
        filters = tags.copy()
        if TAG_RAY_CLUSTER_NAME not in tags:
            filters[TAG_RAY_CLUSTER_NAME] = self.cluster_name

        counter = count
        
        cli_logger.info(f'Create Node tags : {filters}')
        reused_nodes_dict = {}
        reuse_nodes = list()
        reuse_node_ids = []
        number_of_reused_nodes = 0
        # Try to reuse previously stopped nodes with compatible configs
        if self.cache_stopped_nodes:
            vms = self.vsphere_client.vcenter.VM.list(
                VM.FilterSpec(power_states={HardPower.State.POWERED_OFF, HardPower.State.SUSPENDED},
                            clusters={self.cluster_name}))
           
            # Select POWERED_OFF or SUSENDED vms which has ray-node-type,
            # ray-launch-config, ray-user-node-type tags
            for vm in vms and number_of_reused_nodes < counter:
                vm_id = vm.name
                yn_id = DynamicID(type=TYPE_OF_RESOURCE, id=vm.vm)
                filter_matched_count = 0
                for tag_id in self.vsphere_client.tagging.TagAssociation.list_attached_tags(yn_id):
                    vsphere_vm_tag = self.vsphere_client.tagging.Tag.get(tag_id=tag_id).name
                    
                    if ':' in vsphere_vm_tag:
                        tag_key_value = vsphere_vm_tag.split(':')
                        tag_key = tag_key_value[0]
                        tag_value = tag_key_value[1]
                        if tag_key in tags and tag_value == filters[tag_key]:
                            filter_matched_count +=1
                if filter_matched_count == len(filters):
                    reuse_nodes.append(vm)
                    reused_nodes_dict[vm_id] = vm
                    # Tag needs vm.vm and not vm.name as id
                    reuse_node_ids.append(vm.vm)
                    number_of_reused_nodes += 1
            
            if reuse_nodes:
                cli_logger.info(
                    "Reusing nodes {}. "
                    "To disable reuse, set `cache_stopped_nodes: False` "
                    "under `provider` in the cluster configuration.",
                    cli_logger.render_list(reuse_node_ids),
                )
                self.vsphere_client.vcenter.vm.Power.start(reuse_nodes)
                for node_id in reuse_node_ids:
                    self.set_node_tags(node_id, tags)
                counter -= len(reuse_node_ids)

        created_nodes_dict = {}
        if counter:
            created_nodes_dict = self._create_node(node_config, filters, counter)

        all_created_nodes = reused_nodes_dict
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

    def get_vm(self, client, vm_name):
        """
        Return the identifier of a vm
        Note: The method assumes that there is only one vm with the mentioned name.
        """
        vm = self._get_cached_node(vm_name)
        cli_logger.info(f'VM {vm_name} found')
        
        return vm

    def attach_tag(self, inv_obj, inv_type, tag_id):
        dyn_id = DynamicID(type=inv_type, id=inv_obj)
        try:
            cli_logger.info(f'Attaching tag {tag_id} to {inv_obj}')
            self.vsphere_client.tagging.TagAssociation.attach(tag_id, dyn_id)
            cli_logger.info('Tag attached')
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
            cli_logger.info('Folder ID: {}'.format(folder_id))
        
            rp_filter_spec = ResourcePool.FilterSpec(names=set(["ray"]))
            resource_pool_summaries = self.vsphere_client.vcenter.ResourcePool.list(rp_filter_spec)
            if not resource_pool_summaries:
                raise ValueError("Resource pool with name '{}' not found".
                                format(self.resourcepoolname))
            resource_pool_id = resource_pool_summaries[0].resource_pool

            cli_logger.info('Resource pool ID: {}'.format(resource_pool_id))

            deployment_target = LibraryItem.DeploymentTarget(
                resource_pool_id=resource_pool_id
            )
            # TODO: How to get this id?
            lib_item_id = "7534d35a-2d9c-4a4f-9ecd-0e1f4f6363ec"
            ovf_summary = self.vsphere_client.vcenter.ovf.LibraryItem.filter(
                ovf_library_item_id=lib_item_id,
                target=deployment_target)
            cli_logger.info('Found an OVF template: {} to deploy.'.format(ovf_summary.name))

            vm_name = "ray-node-pandora-"+str(uuid.uuid4())
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
                cli_logger.info('Deployment successful. VM Name: "{}", ID: "{}"'
                    .format(vm_name, result.resource_id.id))
                self.vm_id = result.resource_id.id
                error = result.error
                if error is not None:
                    for warning in error.warnings:
                        cli_logger.info('OVF warning: {}'.format(warning.message))

            else:
                cli_logger.info('Deployment failed.')
                for error in result.error.errors:
                    cli_logger.abort('OVF error: {}'.format(error.message))
                
            vm = self.get_vm(self.vsphere_client, vm_name)
            vm_id = vm.vm
            status = self.vsphere_client.vcenter.vm.Power.get(vm_id)
            if status != HardPower.Info(state=HardPower.State.POWERED_ON):
                cli_logger.info("Powering on VM")
                self.vsphere_client.vcenter.vm.Power.start(vm_id)
                cli_logger.info('vm.Power.start({})'.format(vm_id))
            ### TO assign a tag to a node:
            ### 1. Create a category e.g RAY_NODE
            ### 2. Create a tag in the category e.g ray-default-head
            ### 3. Assign the tag to an object
            category_id = self.get_category()
            if not category_id:
                category_id = self.create_category()
            for key, value in tags.items():
                tag = key+':'+value
                tag_id = self.get_tag(tag, category_id )
                if not tag_id:
                    tag_id = self.create_node_tag(tag, category_id)
                self.attach_tag(vm_id, TYPE_OF_RESOURCE, tag_id=tag_id)
                cli_logger.info(f'Tag {tag} attached to VM {vm_name}')
            
            created_nodes_dict[vm_name] = vm
        
        return created_nodes_dict

    def get_tag(self, tag_name, category_id):
        for id in self.vsphere_client.tagging.Tag.list_tags_for_category(category_id):
            if tag_name == self.vsphere_client.tagging.Tag.get(id).name:
                return id
        return None

    def create_node_tag(self, ray_node_tag, category_id):
        cli_logger.info(f'Creating {ray_node_tag} tag')
        tag_spec = self.vsphere_client.tagging.Tag.CreateSpec(ray_node_tag, "Ray node tag", category_id)
        try:
            tag_id = self.vsphere_client.tagging.Tag.create(tag_spec)
        except ErrorClients.Unauthorized as e:
            cli_logger.abort(f'Unathorised to create the tag. Exception: {e.messages}')
        except Exception as e:
            cli_logger.abort(e.messages)

        cli_logger.info(f'Tag {tag_id} created')
        return tag_id

    def get_category(self):
        for id in self.vsphere_client.tagging.Category.list():
            if self.vsphere_client.tagging.Category.get(id).name == NODE_CATEGORY:
                return id
        return None

    def create_category(self):
        # Create RAY_NODE category. This category is associated with VMs and supports multiple tags e.g. "Ray-Head-Node, Ray-Worker-Node-1 etc."
        cli_logger.info(f'Creating {NODE_CATEGORY} category')
        category_spec = self.vsphere_client.tagging.Category.CreateSpec(name=NODE_CATEGORY, 
                                       description="Identifies Ray head node and worker nodes", 
                                       cardinality=self.vsphere_client.tagging.CategoryModel.Cardinality.MULTIPLE,
                                       associable_types=TYPE_OF_RESOURCE)
        try:
            category_id = self.vsphere_client.tagging.Category.create(category_spec)
        except ErrorClients.Unauthorized as e:
            cli_logger.abort(f'Unathorised to create the category. Exception: {e.messages}')
        except Exception as e:
            cli_logger.abort(e.messages)
        cli_logger.info(f'Category {category_id} created')
        return category_id

    def terminate_node(self, vm_id):
        vm = self._get_cached_node(vm_id)
        cli_logger.info(f'terminating node {vm.vm}')
        if vm:
            self.vsphere_client.vcenter.vm.Power.stop(vm.vm)

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
                cli_logger.info(
                    "Stopping instances {} "
                    + cf.dimmed(
                        "(to terminate instead, "
                        "set `cache_stopped_nodes: False` "
                        "under `provider` in the cluster configuration)"
                    ),
                    cli_logger.render_list(on_demand_ids),
                )

            if spot_ids:
                cli_logger.info(
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

        vms  = self.vsphere_client.vcenter.VM.list(VM.FilterSpec(names= set([node_id])))
        if len(vms) == 0:
            print("VM with name ({}) not found".format(node_id))
            return None
        return vms[0]

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
                    cli_logger.info(
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
