#     Copyright 2024. ThingsBoard
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

from bacpypes3.ipv4.app import NormalApplication as App
from bacpypes3.local.device import DeviceObject
from bacpypes3.pdu import Address

from thingsboard_gateway.connectors.bacnet.entities.device_object_config import DeviceObjectConfig


class Application(App):
    def __init__(self, device_object_config: DeviceObjectConfig, indication_callback, logger):
        self.__device_object_config = device_object_config
        self.__device_object = DeviceObject(**self.__device_object_config.device_object_config)
        super().__init__(self.__device_object, Address(self.__device_object_config.address))

        self.__log = logger
        self.__indication_callback = indication_callback

    async def indication(self, apdu) -> None:
        self.__log.debug(f"(indication) Received APDU: {apdu}")
        await super().indication(apdu)
        self.__indication_callback(apdu)

    async def do_who_is(self, device_address):
        devices = await self.who_is(address=Address(device_address),
                                    timeout=self.__device_object_config.device_discovery_timeout)
        if len(devices):
            return devices[0]
