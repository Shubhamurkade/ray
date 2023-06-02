import copy
import logging
import sys
import threading
import time
from collections import OrderedDict, defaultdict
from typing import Any, Dict, List

import ray._private.ray_constants as ray_constants
from ray.autoscaler._private.aws.cloudwatch.cloudwatch_helper import (
    CLOUDWATCH_AGENT_INSTALLED_AMI_TAG,
    CLOUDWATCH_AGENT_INSTALLED_TAG,
    CloudwatchHelper,
)
from ray.autoscaler._private.aws.config import bootstrap_aws
from ray.autoscaler._private.aws.utils import (
    boto_exception_handler,
    client_cache,
    resource_cache,
)
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

def make_vsphere_vm():
    pass

def list_ec2_instances(
    region: str, aws_credentials: Dict[str, Any] = None
) -> List[Dict[str, Any]]:
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
        # Note that these filters are acceptable because they are set on
        #       node initialization, and so can never be sitting in the cache.
        tag_filters = to_aws_format(tag_filters)
        filters = [
            {
                "Name": "instance-state-name",
                "Values": ["pending", "running"],
            },
            {
                "Name": "tag:{}".format(TAG_RAY_CLUSTER_NAME),
                "Values": [self.cluster_name],
            },
        ]
        for k, v in tag_filters.items():
            filters.append(
                {
                    "Name": "tag:{}".format(k),
                    "Values": [v],
                }
            )

        with boto_exception_handler("Failed to fetch running instances from AWS."):
            nodes = list(self.ec2.instances.filter(Filters=filters))

        # Populate the tag cache with initial information if necessary
        for node in nodes:
            if node.id in self.tag_cache:
                continue

            self.tag_cache[node.id] = from_aws_format(
                {x["Key"]: x["Value"] for x in node.tags}
            )

        self.cached_nodes = {node.id: node for node in nodes}
        nodes = []
        return [node.id for node in nodes]

    def is_running(self, node_id):
        node = self._get_cached_node(node_id)
        return node.state["Name"] == "running"

    def is_terminated(self, node_id):
        node = self._get_cached_node(node_id)
        state = node.state["Name"]
        return state not in ["running", "pending"]

    def node_tags(self, node_id):
        with self.tag_cache_lock:
            d1 = self.tag_cache[node_id]
            d2 = self.tag_cache_pending.get(node_id, {})
            return dict(d1, **d2)

    def external_ip(self, node_id):
        node = self._get_cached_node(node_id)

        if node.public_ip_address is None:
            node = self._get_node(node_id)

        return node.public_ip_address

    def internal_ip(self, node_id):
        node = self._get_cached_node(node_id)

        if node.private_ip_address is None:
            node = self._get_node(node_id)

        return node.private_ip_address

    def set_node_tags(self, node_id, tags):
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
        all_created_nodes = []
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

    def _create_node(self, node_config, tags, count):
        created_nodes_dict = {}

        folder_filter_spec = Folder.FilterSpec(names=set(["folder-WCP_DC"]))
        folder_summaries = self.client.vcenter.Folder.list(folder_filter_spec)
        if not folder_summaries:
            raise ValueError("Folder with name '{}' not found".
                             format(self.foldername))
        folder_id = folder_summaries[0].folder
        print('Folder ID: {}'.format(folder_id))
    
        rp_filter_spec = ResourcePool.FilterSpec(names=set(["ray"]))
        resource_pool_summaries = self.client.vcenter.ResourcePool.list(rp_filter_spec)
        if not resource_pool_summaries:
            raise ValueError("Resource pool with name '{}' not found".
                             format(self.resourcepoolname))
        resource_pool_id = resource_pool_summaries[0].resource_pool

        print('Resource pool ID: {}'.format(resource_pool_id))

        deployment_target = LibraryItem.DeploymentTarget(
            resource_pool_id=resource_pool_id
        )

        lib_item_id = "f350edec-46f6-4a7b-ad1a-8c7aeef198f6"
        ovf_summary = self.client.vcenter.ovf.LibraryItem.filter(
            ovf_library_item_id=lib_item_id,
            target=deployment_target)
        print('Found an OVF template: {} to deploy.'.format(ovf_summary.name))

        # Build the deployment spec
        deployment_spec = LibraryItem.ResourcePoolDeploymentSpec(
            name="random",
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
        result = self.client.vcenter.ovf.LibraryItem.deploy(
            lib_item_id,
            deployment_target,
            deployment_spec,
            client_token=str(uuid.uuid4()))

        # The type and ID of the target deployment is available in the deployment result.
        if result.succeeded:
            print('Deployment successful. VM Name: "{}", ID: "{}"'
                  .format(self.vm_name, result.resource_id.id))
            self.vm_id = result.resource_id.id
            error = result.error
            if error is not None:
                for warning in error.warnings:
                    print('OVF warning: {}'.format(warning.message))

        else:
            print('Deployment failed.')
            for error in result.error.errors:
                print('OVF error: {}'.format(error.message))

        created_nodes_dict = {result.resource_id.id: result}
        
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

        # TODO (Alex): We are leaking the tag cache here. Naively, we would
        # want to just remove the cache entry here, but terminating can be
        # asyncrhonous or error, which would result in a use after free error.
        # If this leak becomes bad, we can garbage collect the tag cache when
        # the node cache is updated.

    def _check_ami_cwa_installation(self, config):
        response = self.ec2.meta.client.describe_images(ImageIds=[config["ImageId"]])
        cwa_installed = False
        images = response.get("Images")
        if images:
            assert len(images) == 1, (
                f"Expected to find only 1 AMI with the given ID, "
                f"but found {len(images)}."
            )
            image_name = images[0].get("Name", "")
            if CLOUDWATCH_AGENT_INSTALLED_AMI_TAG in image_name:
                cwa_installed = True
        return cwa_installed

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
                    spot_ids += [node_id]
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
    def bootstrap_config(cluster_config):
        return bootstrap_aws(cluster_config)

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
