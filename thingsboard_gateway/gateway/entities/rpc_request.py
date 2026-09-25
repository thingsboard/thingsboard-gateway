#     Copyright 2026. ThingsBoard
#
#     Licensed under the Apache License, Version 2.0 (the "License");
#     you may not use this file except in compliance with the License.
#     You may obtain a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#     Unless required by applicable law or agreed to in writing, software
#     distributed under the License is distributed on an "AS IS" BASIS,
#     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#     See the License for the specific language governing permissions and
#     limitations under the License.

from enum import Enum

from tb_mqtt_client.tb_device_mqtt import DEFAULT_TIMEOUT
from thingsboard_gateway.gateway.constants import (
    DATA_PARAMETER,
    DEFAULT_CONNECTORS,
    RPC_ID_PARAMETER,
    RPC_PARAMS_PARAMETER,
    RPC_METHOD_PARAMETER,
    DEVICE_SECTION_PARAMETER
)


def create_rpc_request_from_dict(content, gateway_or_connector_req_id=None):
    if RPCRequestBase.is_old_format_rpc_content(content):
        content = RPCRequestBase._convert_old_format_rpc_content(content)

    rpc_type = RPCRequestBase.get_rpc_type(content)

    if rpc_type == RPCType.GATEWAY:
        return GatewayRPCRequest(gateway_or_connector_req_id, content)
    elif rpc_type == RPCType.CONNECTOR:
        return ConnectorRPCRequest(gateway_or_connector_req_id, content)
    elif rpc_type == RPCType.DEVICE:
        return DeviceRPCRequest(content)
    elif rpc_type == RPCType.RESERVED:
        return ReservedRPCRequest(content)
    else:
        raise ValueError(f"Unknown RPC type for content: {content}")


class RPCType(Enum):
    CONNECTOR = 'CONNECTOR'
    DEVICE = 'DEVICE'
    RESERVED = 'RESERVED'
    GATEWAY = 'GATEWAY'


class RPCRequestBase:
    def __init__(self, content):
        self.id = content[DATA_PARAMETER].get(RPC_ID_PARAMETER)
        self.timeout = content.get('timeout', DEFAULT_TIMEOUT)
        self.params = content[DATA_PARAMETER].get(RPC_PARAMS_PARAMETER)

    @staticmethod
    def is_old_format_rpc_content(content):
        return content.get(DATA_PARAMETER) is None

    @staticmethod
    def _convert_old_format_rpc_content(content):
        new_content = {}
        new_content[DATA_PARAMETER] = {
            RPC_PARAMS_PARAMETER: content[RPC_PARAMS_PARAMETER],
            RPC_METHOD_PARAMETER: content[RPC_METHOD_PARAMETER]
        }
        return new_content

    @staticmethod
    def get_rpc_type(content):
        if RPCRequestBase._is_gateway_rpc(content):
            return RPCType.GATEWAY
        elif RPCRequestBase._is_connector_rpc(content):
            return RPCType.CONNECTOR
        elif RPCRequestBase._is_reserved_rpc(content):
            return RPCType.RESERVED
        elif RPCRequestBase._is_device_rpc(content):
            return RPCType.DEVICE
        else:
            raise ValueError(f"Unknown RPC type for content: {content}")

    @staticmethod
    def _is_gateway_rpc(content):
        try:
            (module, _) = content[DATA_PARAMETER][RPC_METHOD_PARAMETER].split('_')
            if module == 'gateway':
                return True
        except (IndexError, ValueError):
            return False

    @staticmethod
    def _is_connector_rpc(content):
        try:
            (connector_type, _) = content[DATA_PARAMETER][RPC_METHOD_PARAMETER].split('_')
            if connector_type in DEFAULT_CONNECTORS.keys():
                return True
        except (IndexError, ValueError):
            return False

    @staticmethod
    def _is_reserved_rpc(content):
        rpc_method = content[DATA_PARAMETER][RPC_METHOD_PARAMETER].lower()
        if rpc_method == 'get' or rpc_method == 'set' and RPCRequestBase._is_device_rpc(content):
            return True

        return False

    @staticmethod
    def _is_device_rpc(content):
        return content.get(DEVICE_SECTION_PARAMETER) is not None


class GatewayRPCRequest(RPCRequestBase):
    def __init__(self, req_id, content):
        super().__init__(content)
        self.id = req_id
        self.rpc_type = RPCType.GATEWAY
        self.method_name = content[DATA_PARAMETER][RPC_METHOD_PARAMETER].replace("gateway_", "")

    def __str__(self):
        return f"RPC To Gateway: id={self.id}, method_name={self.method_name}, params={self.params}"


class ConnectorRPCRequest(RPCRequestBase):
    def __init__(self, req_id, content):
        super().__init__(content)
        self.id = req_id
        self.rpc_type = RPCType.CONNECTOR
        (connector_type, rpc_method_name) = content[DATA_PARAMETER][RPC_METHOD_PARAMETER].split('_')
        self.connector_type = connector_type
        self.method_name = rpc_method_name
        self.value = content[DATA_PARAMETER][RPC_PARAMS_PARAMETER].get('value')
        self.device_name = self.params.get('deviceName')

    def __str__(self):
        return f"RPC To Connector: id={self.id}, connector_type={self.connector_type}, method_name={self.method_name}, params={self.params}"  # noqa


class DeviceRPCRequest(RPCRequestBase):
    def __init__(self, content):
        super().__init__(content)
        self.rpc_type = RPCType.DEVICE
        self.method_name = content[DATA_PARAMETER][RPC_METHOD_PARAMETER]
        self.device_name = content.get(DEVICE_SECTION_PARAMETER)

    def __str__(self):
        return f"RPC To Device: id={self.id}, method_name={self.method_name}, device_name={self.device_name}, params={self.params}"  # noqa


class ReservedRPCRequest(RPCRequestBase):
    def __init__(self, content):
        super().__init__(content)
        self.rpc_type = RPCType.RESERVED
        self.method_name = content[DATA_PARAMETER][RPC_METHOD_PARAMETER]
        self.device_name = content.get(DEVICE_SECTION_PARAMETER)

    def __str__(self):
        return f"Reserved RPC: id={self.id}, method_name={self.method_name}, device_name={self.device_name}, params={self.params}"  # noqa
