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

from asyncio import Queue, QueueEmpty, sleep
from typing import List
from bacpypes3.ipv4.app import NormalApplication as App
from bacpypes3.local.device import DeviceObject
from bacpypes3.pdu import Address
from bacpypes3.primitivedata import ObjectIdentifier, Unsigned
from bacpypes3.basetypes import PropertyIdentifier, ReadAccessSpecification
from bacpypes3.vendor import get_vendor_info
from bacpypes3.constructeddata import SequenceOf, Array
from bacpypes3.apdu import (
    APDU,
    AbortPDU,
    ComplexAckPDU,
    ErrorPDU,
    RejectPDU,
    SimpleAckPDU,
    ErrorRejectAbortNack,
    ReadPropertyMultipleRequest,
    ReadPropertyMultipleACK
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
            except QueueEmpty:
                await sleep(0.1)
            except Exception as e:
                self.__log.error("APDU confirmation error: %s", e)

    async def do_who_is(self, device_address):
        devices = await self.who_is(address=Address(device_address),
                                    timeout=self.__device_object_config.device_discovery_timeout)
        if len(devices):
            return devices[0]

        return None

    async def get_object_identifiers_with_segmentation(self, device) -> List[ObjectIdentifier]:
        try:
            object_list = await self.read_property(
                device.details.address, device.details.identifier, "objectList"
            )
            return object_list
        except AbortPDU as e:
            self.__log.warning(f"{device.details.identifier} objectList abort: {e}")
        except ErrorRejectAbortNack as e:
            self.__log.warning(f"{device.details.identifier} objectList error/reject: {e}")
        except ErrorPDU:
            self.__log.error('ErrorPDU reading object-list')
        except Exception as e:
            self.__log.error(f"{device.details.identifier} objectList error: {e}")

        return []

    async def get_device_object_list_len(self, device):
        try:
            object_list_length = await self.read_property(
                device.details.address,
                device.details.identifier,
                "objectList",
                array_index=0,
            )
        except ErrorPDU:
            self.__log.error('ErrorPDU reading object-list length')
            return 0
        except Exception as e:
            self.__log.error('%s error reading object-list length: %s', device.details.identifier, e)
            return 0

        return object_list_length

    async def get_object_identifiers_without_segmentation(self, device, index_to_read=None) -> List[ObjectIdentifier]:
        object_list = []

        if index_to_read is None:
            object_list_length = await self.get_device_object_list_len(device)
            if object_list == 0:
                return []

            device.details.objects_len = object_list_length
            index_to_read = range(object_list_length)

        for i in index_to_read:
            try:
                object_identifier = await self.read_property(
                    device.details.address,
                    device.details.identifier,
                    "objectList",
                    array_index=i + 1,
                )

                if object_identifier[0].__str__() == 'device':
                    continue

                object_list.append(object_identifier)

                if index_to_read is not None:
                    device.details.sucess_read_for(i)
            except ErrorPDU:
                self.__log.error('ErrorPDU reading object-list[%d]', i + 1)
                device.details.failed_to_read_indexes = i
                continue
            except Exception as e:
                self.__log.error('%s error reading object-list[%d]: %s', device.details.identifier, i + 1, e)
                device.details.failed_to_read_indexes = i
                continue

        return object_list

    async def read_multiple_objects(self, device, object_list):
        if len(object_list) == 0:
            self.__log.warning("%s no objects to read", device.details.object_id)
            return []

        read_access_specifications = self.__get_read_access_specifications(object_list, device.details.vendor_id)
        if len(read_access_specifications) == 0:
            self.__log.warning("no read access specifications")
            return []

        request = ReadPropertyMultipleRequest(
            listOfReadAccessSpecs=SequenceOf(ReadAccessSpecification)(
                read_access_specifications
            ),
            destination=Address(device.details.address),
        )

        result = None
        try:
            result = await self.request(request)
        except AbortPDU as e:
            self.__log.warning("%s objectList abort: %s", device.details.object_id, e)
        except ErrorRejectAbortNack as e:
            self.__log.warning("%s objectList error/reject: %s", device.details.object_id, e)
        except ErrorPDU:
            self.__log.error('ErrorPDU reading object-list')
        except Exception as e:
            self.__log.error("%s objectList error: %s", device.details.object_id, e)

        if not isinstance(result, ReadPropertyMultipleACK):
            self.__log.error("Invalid response type: %s", type(result))
            return []

        decoded_result = self.decode_tag_list(result, device.details.vendor_id)

        return decoded_result

    async def get_device_objects(self, device, index_to_read=None):
        if device.details.is_segmentation_supported():
            object_list = await self.get_object_identifiers_with_segmentation(device)
        else:
            object_list = await self.get_object_identifiers_without_segmentation(device,
                                                                                 index_to_read=index_to_read)

        if len(object_list) == 0:
            self.__log.warning("%s no objects to read", device.details.object_id)
            return None

        object_list = [{'objectId': obj, 'propertyId': 'object-name'} for obj in object_list]

        return ObjectIterator(self, device, object_list, self.read_multiple_objects)

    async def get_device_values(self, device):
        return ObjectIterator(self,
                              device,
                              device.uplink_converter_config.objects_to_read,
                              self.read_multiple_objects)

    def __get_read_access_specifications(self, object_list, vendor_id):
        read_access_specifications = []
        vendor_info = get_vendor_info(vendor_id)

        for obj in object_list:
            try:
                object_id = obj.get('objectId')
                if not isinstance(object_id, ObjectIdentifier):
                    obj_str = f"{obj['objectType']},{object_id}"
                    object_id = ObjectIdentifier(obj_str)

                object_class = vendor_info.get_object_class(object_id[0])
                if object_class is None:
                    self.__log.warning(f"unknown object type: {object_id}, {object_class}")
                    continue

                property_identifier = PropertyIdentifier(obj['propertyId'])

                property_class = object_class.get_property_type(property_identifier)
                if property_class is None:
                    self.__log.warning(f"{object_id} unknown property: {property_identifier}")
                    continue

                ras = ReadAccessSpecification(
                    objectIdentifier=object_id,
                    listOfPropertyReferences=[property_identifier],
                )

                read_access_specifications.append(ras)
            except Exception as e:
                self.__log.error("failed to create read access specification for %s: %s", object_id, e)
                continue

        return read_access_specifications

    async def get_device_name(self, apdu):
        device_name = None

        try:
            address = apdu.pduSource.__str__()
            device_name = await self.read_property(
                Address(address),
                Device.get_object_id({'objectType': 'device', 'objectId': apdu.iAmDeviceIdentifier[1]}), 'objectName')
        except AbortPDU as e:
            self.__log.warning("Reading %s device name abort: %s", device_name, e)
        except ErrorRejectAbortNack as e:
            self.__log.warning("Reading %s device name reject: %s", device_name, e)
        except ErrorPDU:
            self.__log.error('ErrorPDU reading %s device name', device_name)
        except Exception as e:
            self.__log.warning("Failed to get %s device name: %s", device_name, e)

        return device_name

    def decode_tag_list(self, tag_list, vendor_id):
        vendor_info = get_vendor_info(vendor_id)
        result_list = []

        for read_access_result in tag_list.listOfReadAccessResults:
            object_identifier = read_access_result.objectIdentifier
            object_class = vendor_info.get_object_class(object_identifier[0])

            for read_access_result_element in read_access_result.listOfResults:
                try:
                    property_identifier = read_access_result_element.propertyIdentifier
                    property_array_index = read_access_result_element.propertyArrayIndex
                    read_result = read_access_result_element.readResult

                    if read_result.propertyAccessError:
                        result_list.append(
                            (
                                object_identifier,
                                property_identifier,
                                property_array_index,
                                read_result.propertyAccessError,
                            )
                        )
                        continue

                    property_type = object_class.get_property_type(property_identifier)
                    if property_type is None:
                        self.__log.warning("%r not supported", property_identifier)

                        result_list.append(
                            (
                                object_identifier,
                                property_identifier,
                                property_array_index,
                                None,
                            )
                        )
                        continue

                    if issubclass(property_type, Array):
                        if property_array_index is None:
                            pass
                        elif property_array_index == 0:
                            property_type = Unsigned
                        else:
                            property_type = property_type._subtype

                    property_value = read_result.propertyValue.cast_out(property_type)

                    result_list.append(
                        (
                            object_identifier,
                            property_identifier,
                            property_array_index,
                            property_value,
                        )
                    )
                except Exception as e:
                    self.__log.error('failed to decode read access result: %s', e)
                    continue

        return result_list


class ObjectIterator:
    def __init__(self, app, device, object_list, func):
        self.app = app
        self.items = object_list
        self.device = device
        self.limit = device.details.get_max_apdu_count()
        self.func = func
        self.index = 0

    async def get_next(self):
        if self.index >= len(self.items):
            return [], True

        end_index = self.index + self.limit
        result = self.items[self.index:end_index]
        self.index = end_index
        finished = self.index >= len(self.items)

        r = await self.func(self.device, result)

        return r, result, finished
