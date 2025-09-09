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
from re import compile, match

from thingsboard_gateway.gateway.constants import (
    DATA_PARAMETER,
    DEVICE_SECTION_PARAMETER,
    RPC_ID_PARAMETER,
    RPC_PARAMS_PARAMETER,
    RPC_METHOD_PARAMETER
)

AVAILABLE_DATA_TYPES = "string|bytes|bits|16int|16uint|16float|32int|32uint|32float|64int|64uint|64float"

class RPCType(Enum):
    CONNECTOR = 'CONNECTOR'
    DEVICE = 'DEVICE'
    RESERVED = 'RESERVED'


class RPCRequest:
    def __init__(self, content):
        if self.is_old_format_rpc_content(content):
            content = self._convert_old_format_rpc_content(content)

        self.rpc_type = self._get_rpc_type(content)
        self.method = None
        self.params = None
        self._value = None
        self.timeout = content.get('timeout', 5.0)
        self.id = content[DATA_PARAMETER].get(RPC_ID_PARAMETER)
        self.device_name = content.get(DEVICE_SECTION_PARAMETER)

        self._fill_rpc_request(content)

    @property
    def value(self):
        return {DATA_PARAMETER: {RPC_PARAMS_PARAMETER: self._value}}

    def is_old_format_rpc_content(self, content):
        return content.get(DATA_PARAMETER) is None

    @staticmethod
    def _convert_old_format_rpc_content(content):
        new_content = {}
        new_content[DATA_PARAMETER] = {RPC_PARAMS_PARAMETER: content[RPC_PARAMS_PARAMETER],
                                       RPC_METHOD_PARAMETER: content[RPC_METHOD_PARAMETER]}
        return new_content

    def _get_rpc_type(self, content):
        if self._is_connector_rpc(content):
            return RPCType.CONNECTOR
        elif self._is_reserved_rpc(content):
            return RPCType.RESERVED
        elif self._is_device_rpc(content):
            return RPCType.DEVICE
        else:
            raise ValueError('Unknown RPC type for content: %r' % content)

    @staticmethod
    def _is_connector_rpc(content) -> bool:
        try:
            (connector_type, _) = content[DATA_PARAMETER][RPC_METHOD_PARAMETER].split('_')
            if connector_type == 'modbus':
                return True
        except (IndexError, ValueError):
            return False

    @staticmethod
    def _is_reserved_rpc(content) -> bool:
        rpc_method = content[DATA_PARAMETER][RPC_METHOD_PARAMETER].lower()
        if rpc_method == 'get' or rpc_method == 'set' and RPCRequest._is_device_rpc(content):
            return True

        return False

    @staticmethod
    def _is_device_rpc(content) -> bool:
        return content.get(DEVICE_SECTION_PARAMETER) is not None

    def _fill_rpc_request(self, content):
        if self.rpc_type == RPCType.CONNECTOR:
            self._fill_connector_rpc_request(content)
        elif self.rpc_type == RPCType.DEVICE:
            self._fill_device_rpc_request(content)
        elif self.rpc_type == RPCType.RESERVED:
            self._fill_reserved_rpc_request(content)

    def _fill_connector_rpc_request(self, content):
        (connector_type, rpc_method_name) = content[DATA_PARAMETER][RPC_METHOD_PARAMETER].split('_')
        if connector_type == 'modbus':
            self.method = rpc_method_name
            self.params = content[DATA_PARAMETER][RPC_PARAMS_PARAMETER]
            self._value = content[DATA_PARAMETER][RPC_PARAMS_PARAMETER].get('value')

    def _fill_device_rpc_request(self, content):
        self.method = content[DATA_PARAMETER][RPC_METHOD_PARAMETER]
        self._value = content[DATA_PARAMETER][RPC_PARAMS_PARAMETER]

    def _fill_reserved_rpc_request(self, content):

        self.method = content[DATA_PARAMETER][RPC_METHOD_PARAMETER]
        input_params = content[DATA_PARAMETER][RPC_PARAMS_PARAMETER]

        get_pattern = compile(
            rf'^type=(?:{AVAILABLE_DATA_TYPES});functionCode=[1-4];objectsCount=\d+;address=\d+;'
        )

        set_pattern = compile(
            rf'^type=(?:{AVAILABLE_DATA_TYPES});functionCode=(?:5|6|15|16);'rf'objectsCount=\d+;address=\d+;value=.+;$'
        )
        pattern = get_pattern if self.method == 'get' else set_pattern
        expected_schema = (
            "get type=<type>;functionCode=<functionCode>;objectsCount=<objectsCount>;address=<address>;" if self.method == "get"
            else "set type=<type>;functionCode=<functionCode>;objectsCount=<objectsCount>;address=<address>;value=<value>;"

        )
        if not pattern.match(input_params):
            raise ValueError(
                f'The requested RPC either does not match with the schema {expected_schema} or incorrect value/values provided')

        if self.method == 'set':
            input_params_value_list =  input_params.split("value=")
            (input_params, input_value) = input_params_value_list
            self._value = input_value[:-1]

        else:
            input_params = content.get(DATA_PARAMETER, {}).get(RPC_PARAMS_PARAMETER, {})

        self.params = self.__form_reserved_rpc_params(input_params=input_params)

    @staticmethod
    def __form_reserved_rpc_params(input_params: str):
        params = {}
        for param in input_params.split(';'):
            try:
                (key, value) = param.split('=')
            except ValueError:
                continue
            if key and value:
                params[key] = value if key not in ('functionCode', 'objectsCount', 'address') else int(
                    value)
        return params

    def can_return_response(self):
        return self.id is not None or self.rpc_type == RPCType.CONNECTOR

    def for_existing_device(self):
        return not self.params.get('host') and not self.params.get('port') and not self.params.get('unitId')