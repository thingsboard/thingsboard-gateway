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

from bacpypes3.ipv4.app import ForeignApplication as App
from bacpypes3.pdu import IPv4Address

from thingsboard_gateway.connectors.bacnet.application import Application
from thingsboard_gateway.connectors.bacnet.entities.device_object_config import DeviceObjectConfig


class ForeignApplication(Application, App):
    def __init__(self, device_object_config: DeviceObjectConfig, indication_callback, logger):
        super().__init__(device_object_config, indication_callback, logger)

    def register_foreign_device(self, address: IPv4Address, ttl: int) -> None:
        self.__log.debug(f"(register_foreign_device) Registering foreign device")
        self.register(address, ttl)
