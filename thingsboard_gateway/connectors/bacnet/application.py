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

from typing import List
from bacpypes3.ipv4.app import NormalApplication as App
from bacpypes3.local.device import DeviceObject
from bacpypes3.pdu import Address
from bacpypes3.primitivedata import ObjectIdentifier
from bacpypes3.apdu import AbortReason, AbortPDU, ErrorRejectAbortNack
from bacpypes3.basetypes import PropertyIdentifier
from bacpypes3.vendor import VendorInfo

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
        await self.__indication_callback(apdu)

    async def do_who_is(self, device_address):
        devices = await self.who_is(address=Address(device_address),
                                    timeout=self.__device_object_config.device_discovery_timeout)
        if len(devices):
            return devices[0]

    async def get_object_identifiers(
        self, device_address: Address, device_identifier: ObjectIdentifier
    ) -> List[ObjectIdentifier]:
        try:
            object_list = await self.read_property(
                device_address, device_identifier, "objectList"
            )
            return object_list
        except AbortPDU as err:
            if err.apduAbortRejectReason != AbortReason.segmentationNotSupported:
                self.__log.warning(f"{device_identifier} objectList abort: {err}\n")
                return []
        except ErrorRejectAbortNack as err:
            self.__log.warning(f"{device_identifier} objectList error/reject: {err}\n")
            return []

        object_list = []
        try:
            object_list_length = await self.read_property(
                device_address,
                device_identifier,
                "objectList",
                array_index=0,
            )

            for i in range(object_list_length):
                object_identifier = await self.read_property(
                    device_address,
                    device_identifier,
                    "objectList",
                    array_index=i + 1,
                )
                object_list.append(object_identifier)
        except ErrorRejectAbortNack as err:
            self.__log.info(f"{device_identifier} object-list length error/reject: {err}\n")

        return object_list

    async def get_device_objects(self,
                                 device_address: Address,
                                 device_identifier: ObjectIdentifier,
                                 vendor_info: VendorInfo):
        result = []

        object_list = await self.get_object_identifiers(device_address, device_identifier)

        for object_id in object_list:
            config = {}

            object_class = vendor_info.get_object_class(object_id[0])

            if object_class is None:
                self.__log.warning(f"unknown object type: {object_id}\n")
                continue

            try:
                property_identifier = PropertyIdentifier('object-name')

                property_class = object_class.get_property_type(property_identifier)
                if property_class is None:
                    self.__log.warning(f"{object_id} unknown property: {property_identifier}\n")
                    continue

                property_value = await self.read_property(
                    device_address, object_id, property_identifier
                )

                config = {
                    'objectType': object_id[0].__str__(),
                    'objectId': object_id[-1],
                    'propertyId': 'presentValue',
                    'key': property_value,
                }
            except ErrorRejectAbortNack as err:
                self.__log.warning(f"{object_id} object-name error: {err}\n")
            else:
                result.append(config)

        return result
