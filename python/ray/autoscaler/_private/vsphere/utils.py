from collections import defaultdict
from functools import lru_cache

import boto3
from boto3.exceptions import ResourceNotExistsError
from boto3.resources.base import ServiceResource
from botocore.client import BaseClient
from botocore.config import Config

from ray.autoscaler._private.cli_logger import cf, cli_logger
from enum import Enum
import ssl
import atexit
from pyVim.connect import SmartConnect, Disconnect
from vmware.vapi.vsphere.client import create_vsphere_client
import requests

class VmwSdkClient():
    class ClientType(Enum):
        # Enum for SDK clients

        PYVMOMI_SDK = "pyvmomi"
        AUTOMATION_SDK = "automation_sdk"

    class SessionType(Enum):

        VERIFIED = "verified"
        UNVERIFIED = "unverified"

    def __init__(self, server, user, password, session_type: SessionType, client_type: ClientType):
        self.server = server
        self.user = user
        self.password = password
        self.session_type = session_type
        self.client_type = client_type
    
    def get_client(self):

        if self.client_type == self.ClientType.PYVMOMI_SDK:
            context_obj = None
            if self.session_type == self.SessionType.UNVERIFIED:
                context_obj = ssl._create_unverified_context()
            
            smart_connect_obj = SmartConnect(
                    host=self.server,
                    user=self.user,
                    pwd=self.password,
                    sslContext=context_obj)

            atexit.register(Disconnect, smart_connect_obj)

            return smart_connect_obj.content
        else:
            session = None
            if self.session_type == self.SessionType.UNVERIFIED:
                session = self.get_unverified_session()

            return create_vsphere_client(
                server=self.server,
                username=self.user,
                password=self.password,
                session=session
            )

    def get_unverified_session(self):
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