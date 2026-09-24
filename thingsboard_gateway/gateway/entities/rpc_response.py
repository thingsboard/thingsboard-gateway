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


class RPCResponse:
    def __init__(self, rpc_id, to_connector_rpc=False, device=None):
        self.id = rpc_id
        self._message = ''
        self.device_name = device
        self.to_connector_rpc = to_connector_rpc

    def set_message(self, msg):
        self._message = {'result': msg}

    def set_error_msg(self, err_msg):
        self._message = {'error': err_msg}

    @property
    def message(self):
        return self._message
