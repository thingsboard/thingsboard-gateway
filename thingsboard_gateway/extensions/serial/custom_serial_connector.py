#     Copyright 2019. ThingsBoard
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

from threading import Thread
from thingsboard_gateway.connectors.custom_connector import CustomConnector, log


class CustomSerialConnector(CustomConnector):
    def __init__(self, gateway,  config, connector_type):
        super().__init__(gateway, config, connector_type)
        log.debug('Custom converter initialization success')

    def open(self):
        pass

    def close(self):
        pass

    def get_name(self):
        pass

    def is_connected(self):
        pass

    def on_attributes_update(self, content):
        pass

    def server_side_rpc_handler(self, content):
        pass
