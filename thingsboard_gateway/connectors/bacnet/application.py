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

from asyncio import sleep
from queue import Queue, Empty
from typing import List
from bacpypes3.ipv4.app import NormalApplication as App
from bacpypes3.local.device import DeviceObject
from bacpypes3.pdu import Address
from bacpypes3.primitivedata import ObjectIdentifier
from bacpypes3.object import DeviceObject as DeviceObjectClass
from bacpypes3.basetypes import PropertyIdentifier
from bacpypes3.vendor import get_vendor_info
from bacpypes3.basetypes import Segmentation
from bacpypes3.apdu import (
    APDU,
    AbortPDU,
    ComplexAckPDU,
    ErrorPDU,
    RejectPDU,
    SimpleAckPDU,
    ErrorRejectAbortNack
)
from bacpypes3.comm import bind

from thingsboard_gateway.connectors.bacnet.application_service_access_point import ApplicationServiceAccessPoint
from thingsboard_gateway.connectors.bacnet.device import Device
from thingsboard_gateway.connectors.bacnet.entities.device_object_config import DeviceObjectConfig


class Application(App):
    def __init__(self, device_object_config: DeviceObjectConfig, indication_callback, logger):
        self.__stopped = False

        self.__device_object_config = device_object_config
        self.__device_object = DeviceObject(**self.__device_object_config.device_object_config)
        super().__init__(self.__device_object, Address(self.__device_object_config.address))

        self.asap = ApplicationServiceAccessPoint(self.device_object, self.device_info_cache)
        bind(self, self.asap, self.nsap)

        self.__log = logger
        self.__indication_callback = indication_callback
        self.__confirmation_queue = Queue(1_000_000)

    def close(self):
        self.__stopped = True
        super().close()

    async def indication(self, apdu) -> None:
        self.__log.debug(f"(indication) Received APDU: {apdu}")
        await super().indication(apdu)
        self.__indication_callback(apdu)

    async def confirmation(self, apdu: APDU) -> None:
        self.__confirmation_queue.put_nowait(apdu)

    async def confirmation_handler(self):
        while not self.__stopped:
            try:
                apdu = self.__confirmation_queue.get_nowait()

                if apdu.pduSource is None:
                    self.__log.warning("Received APDU without source address: %s", apdu)
                    return

                pdu_source = apdu.pduSource
                if pdu_source not in self._requests:
                    return

                requests = self._requests[pdu_source]
                for indx, (request, future) in enumerate(requests):
                    if request.apduInvokeID == apdu.apduInvokeID:
                        break
                else:
                    return

                if isinstance(apdu, (SimpleAckPDU, ComplexAckPDU)):
                    future.set_result(apdu)
                elif isinstance(apdu, (ErrorPDU, RejectPDU, AbortPDU)):
                    future.set_exception(apdu)
                else:
                    raise TypeError("apdu")
            except Empty:
                await sleep(0.1)
            except Exception as e:
                self.__log.error("APDU confirmation error: %s", e)

    async def do_who_is(self, device_address):
        devices = await self.who_is(address=Address(device_address),
                                    timeout=self.__device_object_config.device_discovery_timeout)
        if len(devices):
            return devices[0]

    async def get_object_identifiers_with_segmentation(
        self, device_address: Address, device_identifier: ObjectIdentifier
    ) -> List[ObjectIdentifier]:
        try:
            object_list = await self.read_property(
                device_address, device_identifier, "objectList"
            )
            return object_list
        except AbortPDU as err:
            self.__log.warning(f"{device_identifier} objectList abort: {err}")
            return []
        except ErrorRejectAbortNack as err:
            self.__log.warning(f"{device_identifier} objectList error/reject: {err}")
            return []

    async def get_object_identifiers_without_segmentation(
        self, device_address: Address, device_identifier: ObjectIdentifier
    ) -> List[ObjectIdentifier]:
        object_list = []

        try:
            object_list_length = await self.read_property(
                device_address,
                device_identifier,
                "objectList",
                array_index=0,
            )
        except Exception as e:
            self.__log.error('%s error reading object-list length: %s', device_identifier, e)
            return []

        for i in range(object_list_length):
            try:
                object_identifier = await self.read_property(
                    device_address,
                    device_identifier,
                    "objectList",
                    array_index=i + 1,
                )
                object_list.append(object_identifier)
            except Exception as e:
                self.__log.error('%s error reading object-list[%d]: %s', device_identifier, i + 1, e)
                continue

        return object_list

    async def get_device_objects(self, apdu):
        result = []

        device_address = apdu.pduSource
        device_identifier = apdu.iAmDeviceIdentifier
        vendor_info = get_vendor_info(apdu.vendorID)
        segmentation_supported = apdu.segmentationSupported

        if segmentation_supported in (Segmentation.segmentedBoth, Segmentation.segmentedTransmit):
            object_list = await self.get_object_identifiers_with_segmentation(device_address, device_identifier)
        else:
            object_list = await self.get_object_identifiers_without_segmentation(device_address, device_identifier)

        for object_id in object_list:
            config = {}

            object_class = vendor_info.get_object_class(object_id[0])

            if object_class is None or object_class is DeviceObjectClass:
                self.__log.warning(f"unknown object type: {object_id}, {object_class}")
                continue

            try:
                property_identifier = PropertyIdentifier('object-name')

                property_class = object_class.get_property_type(property_identifier)
                if property_class is None:
                    self.__log.warning(f"{object_id} unknown property: {property_identifier}")
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
                self.__log.warning(f"{object_id} object-name error: {err}")
            else:
                result.append(config)

        return result

    async def get_device_name(self, apdu):
        try:
            device_name = await self.read_property(
                Address(apdu.pduSource.__str__()),
                Device.get_object_id({'objectType': 'device', 'objectId': apdu.iAmDeviceIdentifier[1]}), 'objectName')

            return device_name
        except Exception as e:
            self.__log.warning(f"Failed to get device name: {e}")
            return None
