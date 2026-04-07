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
from thingsboard_gateway.gateway.constants import (
    DATA_PARAMETER,
    DEVICE_SECTION_PARAMETER,
    RPC_ID_PARAMETER,
    RPC_PARAMS_PARAMETER,
    RPC_METHOD_PARAMETER,
)


class RequestRpcType(Enum):
    CONNECTOR = 'CONNECTOR'
    DEVICE = 'DEVICE'
    RESERVED = 'RESERVED'


class RequestRpcRequest:
    def __init__(self, content: dict):
        self.content = self.process_rpc_request_payload(content)
        self.rpc_method = self.content.get(DATA_PARAMETER, {}).get(RPC_METHOD_PARAMETER)
        self.rpc_type = self._get_rpc_type(self.content)
        self.timeout = self.content.get("timeout", 5)
        self.params = self.content.get(DATA_PARAMETER, {}).get(RPC_PARAMS_PARAMETER)
        self.id = self.content.get(DATA_PARAMETER, {}).get(RPC_ID_PARAMETER)
        self.device_name = self.content.get(DEVICE_SECTION_PARAMETER)

    @staticmethod
    def _is_connector_rpc(content: dict):
        try:
            (connector_type, rpc_method_name) = content['data'].get('method').split('_')
            if connector_type == "request" and content.get('device') is None:
                return True

        except (ValueError, AttributeError):
            return False

    @staticmethod
    def _is_reserved_rpc(content: dict):
        rpc_method_name = content["data"]["method"].lower()
        if rpc_method_name in ("get", "set") and content.get("device"):
            return True

        return False

    @staticmethod
    def process_rpc_request_payload(content: dict):
        normalized_content = dict(content)
        if normalized_content.get("data") is None:
            normalized_content["data"] = {
                "params": normalized_content["params"],
                "method": normalized_content["method"],
                "id": normalized_content["id"],
            }
        return normalized_content

    def _is_device_rpc(self, content: dict):
        return bool(content.get(DEVICE_SECTION_PARAMETER)) and not self._is_reserved_rpc(content)

    def _get_rpc_type(self, content: dict) -> RequestRpcType:
        if self._is_connector_rpc(content):
            return RequestRpcType.CONNECTOR

        elif self._is_reserved_rpc(content):
            return RequestRpcType.RESERVED

        elif self._is_device_rpc(content):
            return RequestRpcType.DEVICE

        raise ValueError('Unknown RPC type for content: %r' % content)
