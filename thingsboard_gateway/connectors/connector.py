#     Copyright 2022. ThingsBoard
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

import logging
from abc import ABC, abstractmethod
from thingsboard_gateway.gateway.constants import DEFAULT_SEND_ON_CHANGE_INFINITE_TTL_VALUE, DEFAULT_SEND_ON_CHANGE_VALUE

log = logging.getLogger("connector")


class Connector(ABC):

    @abstractmethod
    def open(self):
        pass

    @abstractmethod
    def close(self):
        pass

    @abstractmethod
    def get_name(self):
        pass

    @abstractmethod
    def get_config(self):
        pass

    @abstractmethod
    def is_connected(self):
        pass

    @abstractmethod
    def on_attributes_update(self, content):
        pass

    @abstractmethod
    def server_side_rpc_handler(self, content):
        pass

    def is_filtering_enable(self, device_name):
        return DEFAULT_SEND_ON_CHANGE_VALUE

    def get_ttl_for_duplicates(self, device_name):
        return DEFAULT_SEND_ON_CHANGE_INFINITE_TTL_VALUE
