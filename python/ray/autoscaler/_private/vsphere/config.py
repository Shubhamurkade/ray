import copy
import logging
import os

from ray.autoscaler._private.event_system import CreateClusterEvent, global_event_system
from ray.autoscaler._private.util import check_legacy_fields
from cryptography.hazmat.primitives import serialization as crypto_serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.backends import default_backend as crypto_default_backend

PRIVATE_KEY_NAME = "ray-bootstrap-key"
PRIVATE_KEY_NAME_EXTN = "{}.pem".format(PRIVATE_KEY_NAME)

PUBLIC_KEY_NAME = "ray_bootstrap_public_key"
PUBLIC_KEY_NAME_EXTN = "{}.key".format(PUBLIC_KEY_NAME)

PRIVATE_KEY_PATH = os.path.expanduser("~/{}.pem".format(PRIVATE_KEY_NAME))
PUBLIC_KEY_PATH = os.path.expanduser("~/{}.key".format(PUBLIC_KEY_NAME))

USER_DATA_FILE_PATH = os.path.join(os.path.dirname(__file__), "./data/userdata.yaml")

logger = logging.getLogger(__name__)

def bootstrap_vsphere(config):
    # create a copy of the input config to modify
    config = copy.deepcopy(config)

    # Log warnings if user included deprecated `head_node` or `worker_nodes`
    # fields. Raise error if no `available_node_types`
    check_legacy_fields(config)

    # Create new key pair if doesn't exist already
    create_key_pair()

    # Configure SSH access, using an existing key pair if possible.
    config = configure_key_pair(config)

    global_event_system.execute_callback(
        CreateClusterEvent.ssh_keypair_downloaded,
        {"ssh_key_path": config["auth"]["ssh_private_key"]},
    )

    return config

def create_key_pair():
    
    # If the files already exists, we don't want to create new keys.
    # This if condition will currently pass even if there are invalid keys 
    # in those path. TODO: Only return if the keys are valid.

    if os.path.exists(PRIVATE_KEY_PATH) and os.path.exists(PUBLIC_KEY_PATH):
        logger.info("Key-pair already exist. Not creating new ones")
        return
    
    ########### Generate keys ##################
    key = rsa.generate_private_key(
        backend=crypto_default_backend(),
        public_exponent=65537,
        key_size=2048
    )

    private_key = key.private_bytes(
        crypto_serialization.Encoding.PEM,
        crypto_serialization.PrivateFormat.PKCS8,
        crypto_serialization.NoEncryption()
    )

    public_key = key.public_key().public_bytes(
        crypto_serialization.Encoding.OpenSSH,
        crypto_serialization.PublicFormat.OpenSSH
    )
    ##############################################

    with open(PRIVATE_KEY_PATH, 'wb') as content_file:
        content_file.write(private_key)
        os.chmod(PRIVATE_KEY_PATH, 0o600)

    with open(PUBLIC_KEY_PATH, 'wb') as content_file:
        content_file.write(public_key)

def configure_key_pair(config):
    
    logger.info("Configure key pairs for copying into the head node.")

    assert os.path.exists(PRIVATE_KEY_PATH), "Private key file at path {} was not found".format(PRIVATE_KEY_PATH)

    assert os.path.exists(PUBLIC_KEY_PATH), "Public key file at path {} was not found".format(PUBLIC_KEY_PATH)

    # updater.py file uses the following config to ssh onto the head node
    # Also, copies the file onto the head node
    config["auth"]["ssh_private_key"] = PRIVATE_KEY_PATH

    # The path where the public key should be copied onto the remote host
    public_key_remote_path = "~/{}".format(PUBLIC_KEY_NAME_EXTN)

    # Copy the public key to the remote host
    config["file_mounts"][public_key_remote_path] = PUBLIC_KEY_PATH

    return config

