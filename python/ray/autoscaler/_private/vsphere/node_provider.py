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
import yaml
import os
from com.vmware.vcenter_client import VM
from com.vmware.vcenter.vm_client import Power as HardPower
from com.vmware.vapi.std_client import DynamicID
from com.vmware.content.library_client import Item
from com.vmware.vcenter.guest_client import CustomizationSpec,\
    CloudConfiguration, CloudinitConfiguration, ConfigurationSpec,\
    GlobalDNSSettings
from ray.autoscaler._private.vsphere.config import bootstrap_vsphere
from ray.autoscaler._private.vsphere.config import (PUBLIC_KEY_PATH, USER_DATA_FILE_PATH)
from com.vmware.cis.tagging_client import CategoryModel
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
        self.tag_cache_lock = threading.Lock()
        self.lock = RLock()

        # Cache of node objects from the last nodes() call. This avoids
        # excessive DescribeInstances requests.
        self.cached_nodes : Dict[str, VM] = {}

    @staticmethod
    def bootstrap_config(cluster_config):
        return bootstrap_vsphere(cluster_config)

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
                
                matched_tags = {}
                all_tags = {}
                # If the VM has a tag from tag_filter then select it 
                for tag_id in self.vsphere_client.tagging.TagAssociation.list_attached_tags(yn_id):
                    vsphere_vm_tag = self.vsphere_client.tagging.Tag.get(tag_id=tag_id).name 
                    if ':' in vsphere_vm_tag:
                        tag_key_value = vsphere_vm_tag.split(':')
                        tag_key = tag_key_value[0]
                        tag_value = tag_key_value[1]
                        if tag_key in filters and tag_value == filters[tag_key]:
                            matched_tags[tag_key] = tag_value
                        
                        all_tags[tag_key] = tag_value   

                # Update the tag cache with latest tags
                self.tag_cache[vm_id] = all_tags

                if len(matched_tags) == len(filters):
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
        # Return the external IP of the VM
        vm = self.vsphere_client.vcenter.vm.guest.Identity.get(node_id)

        return vm.ip_address

    def internal_ip(self, node_id):
        # TODO: Currently vSphere VMs do not show an internal IP. Check IP configuration
        # to get internal IP too. Temporary fix is to just work with external IPs
        return self.external_ip(node_id)

    def set_node_tags(self, node_id, tags):

        cli_logger.info("Setting tags for vm {}".format(node_id))
        # This method gets called from the Ray and it passes
        # node_id which needs to be vm.vm and not vm.name
        with self.lock:
            category_id = self.get_category()
            if not category_id:
                category_id = self.create_category()

            for key, value in tags.items():
                # If a tag with a key is present on the VM, then remove it
                # before updating the key with a new value.
                self.remove_tag_from_vm(key, node_id)

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
            for vm in vms:
                if number_of_reused_nodes >= counter:
                    break
                
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
                for node_id in reuse_node_ids:
                    cli_logger.info("Powering on VM")
                    self.vsphere_client.vcenter.vm.Power.start(node_id)
                counter -= len(reuse_node_ids)

        created_nodes_dict = {}
        if counter:
            created_nodes_dict = self._create_node(node_config, filters, counter)

        all_created_nodes = reused_nodes_dict
        all_created_nodes.update(created_nodes_dict)

        # Set tags on the nodes that were created/reused
        for _, vm in all_created_nodes.items():
            self.set_node_tags(vm.vm, filters)

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

    def get_vm(self, node_id):
        """
        Return the VM summary object
        Note: The method assumes that there is only one vm with the mentioned name.
        """
        vm = self._get_cached_node(node_id)
        cli_logger.info(f'VM {node_id} found')
        
        return vm

    def attach_tag(self, vm_id, resource_type, tag_id):
        dyn_id = DynamicID(type=resource_type, id=vm_id)
        try:
            cli_logger.info(f'Attaching tag {tag_id} to {vm_id}')
            self.vsphere_client.tagging.TagAssociation.attach(tag_id, dyn_id)
            cli_logger.info('Tag attached')
        except Exception as e:
            print("Check that the tag is associable to {}".format(resource_type))
            raise e
    
    def set_cloudinit_userdata(self, vm_id):
        logger.info("Setting cloudinit userdata for vm {}".format(vm_id))

        metadata = '{"cloud_name": "vSphere"}'

        # Read the public key that was generated previously.
        with open(PUBLIC_KEY_PATH, 'r') as file:
            public_key = file.read().rstrip('\n')

        # This file contains the userdata with default values.
        # We want to add the key that we generate with create_key_pair 
        # function into authorized_keys section

        f = open(USER_DATA_FILE_PATH, "r")
        data = yaml.load(f, Loader=yaml.FullLoader)
        for _, v in data.items():
            for user in v:
                if isinstance(user, dict):
                    user["ssh_authorized_keys"] = [public_key]
        f.close()

        modified_userdata = yaml.dump(data, default_flow_style=False)

        # The userdata needs to be prefixed with #cloud-config.
        # Without it, the cloudinit spec would get applied but
        # it wouldn't add the userdata on the VM.

        modified_userdata = "#cloud-config\n"+modified_userdata
        logger.info("Successfully modified the userdata file for vm {}".format(vm_id))

        ############# Create cloud-init spec and apply ####################
        cloudinit_config = CloudinitConfiguration(metadata=metadata,
                                            userdata=modified_userdata)
        cloud_config = CloudConfiguration(cloudinit=cloudinit_config,
                            type=CloudConfiguration.Type('CLOUDINIT'))
        config_spec = ConfigurationSpec(cloud_config=cloud_config)
        global_dns_settings = GlobalDNSSettings()
        adapter_mapping_list = []
        customization_spec = CustomizationSpec(configuration_spec=config_spec,
                            global_dns_settings=global_dns_settings,
                            interfaces=adapter_mapping_list)
        
        # create customization specification by CustomizationSpecs service
        specs_svc = self.vsphere_client.vcenter.guest.CustomizationSpecs
        spec_name = str(uuid.uuid4())
        spec_desc = 'This is a customization specification which includes'\
                'raw cloud-init configuration data'
        create_spec = specs_svc.CreateSpec(name=spec_name,
                                        description=spec_desc,
                                        spec=customization_spec)
        specs_svc.create(spec=create_spec)

        vmcust_svc = self.vsphere_client.vcenter.vm.guest.Customization
        set_spec = vmcust_svc.SetSpec(name=spec_name, spec=None)
        vmcust_svc.set(vm=vm_id, spec=set_spec)

        logger.info("Successfully added cloudinit config for vm {}".format(vm_id))
        #####################################################################

    def create_instant_clone_node(self, vm_clone_from, vm_name_target):
        vm_clone_from_id = vm_clone_from.vm
        clone_spec = VM.InstantCloneSpec(source=vm_clone_from_id, name=vm_name_target)
        vm_id = self.vsphere_client.vcenter.VM.instant_clone(clone_spec)

        # Restarting the VM gets a new IP for the VM.
        # TODO: Find a better way to get a new IP without restarting.
        self.vsphere_client.vcenter.vm.Power.stop(vm_id)
        vm = self.get_vm(vm_id)

        return vm

    def create_ovf_node(self, node_config, vm_name_target):

        # Find and use the resource pool defined in the manifest file.
        rp_filter_spec = ResourcePool.FilterSpec(names=set([node_config["resource_pool"]]))
        resource_pool_summaries = self.vsphere_client.vcenter.ResourcePool.list(rp_filter_spec)
        if not resource_pool_summaries:
            raise ValueError("Resource pool with name '{}' not found".
                            format(rp_filter_spec))
        
        resource_pool_id = resource_pool_summaries[0].resource_pool

        cli_logger.print('Resource pool ID: {}'.format(resource_pool_id))

        # Find and use the OVF library item defined in the manifest file.
        find_spec = Item.FindSpec(name=node_config["library_item"])
        item_ids = self.vsphere_client.content.library.Item.find(find_spec)

        lib_item_id = item_ids[0]

        deployment_target = LibraryItem.DeploymentTarget(
            resource_pool_id=resource_pool_id
        )
        ovf_summary = self.vsphere_client.vcenter.ovf.LibraryItem.filter(
            ovf_library_item_id=lib_item_id,
            target=deployment_target)
        cli_logger.print('Found an OVF template: {} to deploy.'.format(ovf_summary.name))

        # Build the deployment spec
        deployment_spec = LibraryItem.ResourcePoolDeploymentSpec(
            name=vm_name_target,
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
                .format(vm_name_target, result.resource_id.id))
            self.vm_id = result.resource_id.id
            error = result.error
            if error is not None:
                for warning in error.warnings:
                    cli_logger.print('OVF warning: {}'.format(warning.message))

        else:
            cli_logger.print('Deployment failed.')
            for error in result.error.errors:
                cli_logger.print('OVF error: {}'.format(error.message))
            
            raise ValueError("OVF deployment failed for VM {}".format(vm_name_target))
        
        # Get the created vm object
        vm = self.get_vm(result.resource_id.id)

        # Inject a new user with public key into the VM
        self.set_cloudinit_userdata(vm.vm)

        return vm

    # Example: If a tag called node-status:initializing is present on the VM.
    # If we would like to add a new value called finished with the node-status key.
    # We'll need to delete the older tag node-status:initializing first before creating
    # node-status:finished
    def remove_tag_from_vm(self, tag_key_to_remove, vm_id):
        yn_id = DynamicID(type=TYPE_OF_RESOURCE, id=vm_id)

        # List all the tags present on the VM.
        for tag_id in self.vsphere_client.tagging.TagAssociation.list_attached_tags(yn_id):
            vsphere_vm_tag = self.vsphere_client.tagging.Tag.get(tag_id=tag_id).name 
            if ':' in vsphere_vm_tag:
                tag_key_value = vsphere_vm_tag.split(':')
                tag_key = tag_key_value[0]
                if tag_key == tag_key_to_remove:
                    # Remove the tag matching the key passed.
                    cli_logger.info("Removing tag {} from the VM {}".format(tag_key, vm_id))
                    self.vsphere_client.tagging.TagAssociation.detach(tag_id, yn_id)
                    break

    def _create_node(self, node_config, tags, count):
        created_nodes_dict = {}

        for _ in range(count):

            # The nodes are named as follows:
            # ray-<cluster-name>-head-<uuid> for the head node
            # ray-<cluster-name>-worker-<uuid> for the worker nodes
            vm_name = tags[TAG_RAY_NODE_NAME]+"-"+str(uuid.uuid4())

            vm = None

            # Check if clone_from config is present and that it contains a valid VM
            try:
                vm = self.get_vm(node_config["clone_from"]) 
            except KeyError:
                cli_logger.info("clone_from config not present so creating VM from OVF.")

            # Clone from existing worker if we got a valid VM from clone_from config.
            if "clone" in node_config and node_config["clone"] is True and vm != None:

                cli_logger.info("Clone the worker from {}".format(vm))

                vm = self.create_instant_clone_node(vm, vm_name)
            else: 
                
                vm = self.create_ovf_node(node_config, vm_name)

                node_config["clone_from"] = vm.vm

            vm_id = vm.vm
            # Power on the created VM. If it was InstantClone then we would have
            # powered it off after creation.
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
                                       cardinality=CategoryModel.Cardinality.MULTIPLE,
                                       associable_types=set())
        category_id = None

        try:
            category_id = self.vsphere_client.tagging.Category.create(category_spec)
        except ErrorClients.Unauthorized as e:
            cli_logger.abort(f'Unathorised to create the category. Exception: {e.messages}')
        except Exception as e:
            cli_logger.abort(e)

        cli_logger.info(f'Category {category_id} created')
        
        return category_id

    def terminate_node(self, node_id):
        if node_id is None:
            return
        
        status = self.vsphere_client.vcenter.vm.Power.get(node_id)
        if status != HardPower.Info(state=HardPower.State.POWERED_OFF):
            self.vsphere_client.vcenter.vm.Power.stop(node_id)
            cli_logger.info('vm.Power.stop({})'.format(node_id))
            
        self.vsphere_client.vcenter.VM.delete(node_id)
        cli_logger.info("Deleted vm {}".format(node_id))
        self.cached_nodes.pop(node_id)
        self.tag_cache.pop(node_id)

    def terminate_nodes(self, node_ids):
        if not node_ids:
            return

        for node_id in node_ids:
            self.terminate_node(node_id)

    def _get_node(self, node_id):
        """Refresh and get info for this node, updating the cache."""
        self.non_terminated_nodes({})  # Side effect: updates cache

        vms  = self.vsphere_client.vcenter.VM.list(VM.FilterSpec(vms=set([node_id])))
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
