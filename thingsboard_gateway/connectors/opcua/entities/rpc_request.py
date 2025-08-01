#     Copyright 2025. ThingsBoard
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
from re import findall, sub

from thingsboard_gateway.gateway.constants import (
    DATA_PARAMETER,
    DEVICE_SECTION_PARAMETER,
    RPC_ID_PARAMETER,
    RPC_PARAMS_PARAMETER,
    RPC_METHOD_PARAMETER,
    RPC_CONNECTOR_ARGUMENTS_PARAMETER
)

RPC_SET_SPLIT_PATTERNS = ["; ", ";=", "=", " "]


class OpcUaRpcType(Enum):
    CONNECTOR = 'CONNECTOR'
    DEVICE = 'DEVICE'
    RESERVED = 'RESERVED'


class OpcUaRpcRequest:
    def __init__(self, content: dict) -> None:
        if content.get('data') is None:
            content['data'] = {'params': content['params'], 'method': content['method'], 'id': content['id']}
        self.rpc_method = content[DATA_PARAMETER].get(RPC_METHOD_PARAMETER)
        self.rpc_type = self._get_rpc_type(content)
        self.timeout = content.get('timeout', 5)
        self.params = content[DATA_PARAMETER].get(RPC_PARAMS_PARAMETER)
        self._arguments = None
        self._received_identifier = None
        self.id = content[DATA_PARAMETER].get(RPC_ID_PARAMETER)
        self.device_name = content.get(DEVICE_SECTION_PARAMETER)
        self._fill_rpc_request(content)

    @property
    def arguments(self):
        return self._arguments

    @arguments.setter
    def arguments(self, value):
        self._arguments = value

    @property
    def received_identifier(self):
        return self._received_identifier

    @received_identifier.setter
    def received_identifier(self, value):
        self._received_identifier = value

    def _get_rpc_type(self, content: dict) -> OpcUaRpcType:
        if self._is_connector_rpc():
            return OpcUaRpcType.CONNECTOR

        elif self._is_reserved_rpc(content):
            return OpcUaRpcType.RESERVED

        elif content.get(DEVICE_SECTION_PARAMETER) and not self._is_reserved_rpc(content):
            return OpcUaRpcType.DEVICE

        raise ValueError('Unknown RPC type for content: %r' % content)

    def _is_connector_rpc(self) -> bool:
        try:
            (connector_type, rpc_method_name) = self.rpc_method.split('_')
            if connector_type == "opcua":
                self.rpc_method = rpc_method_name
                return True

        except(ValueError, AttributeError):
            return False

    @staticmethod
    def _is_reserved_rpc(content: dict) -> bool:
        rpc_method = content[DATA_PARAMETER][RPC_METHOD_PARAMETER].lower()
        if rpc_method == 'get' or rpc_method == 'set' and content.get(DEVICE_SECTION_PARAMETER):
            return True

        return False

    def _fill_rpc_request(self, content: dict):
        if self.rpc_type == OpcUaRpcType.CONNECTOR:
            self._fill_connector_rpc_request(content)
        elif self.rpc_type == OpcUaRpcType.DEVICE:
            self._fill_device_rpc_request(content)
        elif self.rpc_type == OpcUaRpcType.RESERVED:
            self._fill_reserved_rpc_request(content)

    def _fill_connector_rpc_request(self, content: dict):
        values_sections = content[DATA_PARAMETER][RPC_PARAMS_PARAMETER][RPC_CONNECTOR_ARGUMENTS_PARAMETER]
        values = [value["value"] for value in values_sections]
        self._arguments = values

    def _fill_reserved_rpc_request(self, content: dict):
        params = content.get(DATA_PARAMETER, {}).get(RPC_PARAMS_PARAMETER)
        if self.rpc_method == "get":
            self.params = params
            return
        if self.rpc_method != "set" or not isinstance(params, str):
            return

        ident = self.__find_identifier_request(params)
        if ident:
            value = params[len(ident):]
            delimiter = next((p for p in RPC_SET_SPLIT_PATTERNS if p in params), None)
            self.params = ident
            self.arguments = sub(r"[;\s]+$", "", value.split(delimiter)[-1]).strip(' ;')
            return

        delimiter = next((p for p in RPC_SET_SPLIT_PATTERNS if p in params), None)
        parts = (
            [p.strip() for p in params.split(delimiter) if p.strip()]
            if delimiter
            else [params.strip()]
        )

        if not delimiter or len(parts) != 2:
            return

        full_path, value = parts
        self.params = full_path
        self.arguments = sub(r"[;\s]+$", "", value)

    def _fill_device_rpc_request(self, content: dict):
        rpc_section = content[DATA_PARAMETER].get(RPC_METHOD_PARAMETER)
        self.rpc_method = rpc_section
        self.arguments = self.params

    @staticmethod
    def __find_identifier_request(path: str) -> list | None:
        identifier = findall(r"(ns=\d+;[isgb]=[^}\s]+)(?=\s|$)", path)
        if identifier:
            return identifier[0]
