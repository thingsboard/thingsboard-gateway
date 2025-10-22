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
from bacpypes3.ipv4.app import NormalApplication, ForeignApplication
from bacpypes3.local.device import DeviceObject
from bacpypes3.pdu import Address, IPv4Address
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
from thingsboard_gateway.connectors.bacnet.entities.device_object_config import DeviceObjectConfig


class Application(NormalApplication, ForeignApplication):
    def __init__(self, device_object_config: DeviceObjectConfig, indication_callback, logger, is_foreign_application: bool = False):
        self.__stopped = False

        self.__device_object_config = device_object_config
        self.__device_object = DeviceObject(**self.__device_object_config.device_object_config)

        if not is_foreign_application:
            NormalApplication.__init__(
                self, self.__device_object, Address(self.__device_object_config.address)
            )
        else:
            ForeignApplication.__init__(
                self, self.__device_object, Address(self.__device_object_config.address)
            )

        self.asap = ApplicationServiceAccessPoint(self.device_object, self.device_info_cache)
        bind(self, self.asap, self.nsap)

        self.__log = logger
        self.__indication_callback = indication_callback
        self.__confirmation_queue = Queue(1_000_000)
        self.__is_foreign_application = is_foreign_application

    def register_foreign_device(self, address: IPv4Address, ttl: int) -> None:
        if self.__is_foreign_application:
            self.__log.debug(f"(register_foreign_device) Registering foreign device")
            self.register(address, ttl)
        else:
            self.__log.error(f"(register_foreign_device) Registering foreign device not possible because application is no foreign application")

    def unregister_foreign_device(self) -> None:
        if self.__is_foreign_application:
            self.__log.debug(f"(unregister_foreign_device) Unregistering foreign device")
            self.unregister()
        else:
            self.__log.debug(f"(unregister_foreign_device) Unregistering foreign device not needed because application is no foreign application")

    def close(self):
        self.__stopped = True
        self.unregister_foreign_device()
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
        try:
            devices = await self.who_is(address=Address(device_address),
                                        timeout=self.__device_object_config.device_discovery_timeout)
            if len(devices):
                return devices[0]

        except ValueError as e:
            self.__log.error("Incorrect data format is used %s", str(e))
            self.__log.debug("Error %s", e, exc_info=e)

        except Exception as e:
            self.__log.error("An unexpected error occurred while device look up process %s", str(e))
            self.__log.debug("Error %s", e, exc_info=e)

    async def get_object_identifiers_with_segmentation(self, device) -> List[ObjectIdentifier]:
        object_list = await self.__send_request_wrapper(self.read_property,
                                                        err_msg=f"Failed to read {device.details.identifier} object-list",  # noqa
                                                        address=device.details.address,
                                                        objid=device.details.identifier,
                                                        prop='objectList')

        if object_list is None:
            return []

        return list(filter(lambda obj_id: str(obj_id[0]) != 'device', object_list))

    async def get_object_identifiers_without_segmentation(self, device, index_to_read=None) -> List[ObjectIdentifier]:
        object_list = []
        success_indexes = []

        if index_to_read is None:
            object_list_length = await self.__send_request_wrapper(self.read_property,
                                                                   err_msg=f"Failed to read {device.details.identifier} object-list length",  # noqa
                                                                   address=device.details.address,
                                                                   objid=device.details.identifier,
                                                                   prop='objectList',
                                                                   array_index=0)
            if object_list_length is None:
                return []

            device.details.objects_len = object_list_length
            index_to_read = range(object_list_length)

        for i in index_to_read:
            object_identifier = await self.__send_request_wrapper(self.read_property,
                                                                  err_msg=f"Failed to read {device.details.identifier} object-list[{i + 1}]",  # noqa
                                                                  address=device.details.address,
                                                                  objid=device.details.identifier,
                                                                  prop='objectList',
                                                                  array_index=i + 1)
            if object_identifier is None:
                device.details.failed_to_read_indexes = i
                continue

            if object_identifier[0].__str__() == 'device':
                continue

            object_list.append(object_identifier)
            success_indexes.append(i)

        for i in success_indexes:
            device.details.sucess_read_for(i)

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

        result = await self.__send_request_wrapper(self.request,
                                                   err_msg=f"Failed to read {device.details.object_id} objects",
                                                   apdu=request)

        if not isinstance(result, ReadPropertyMultipleACK):
            self.__log.error("Invalid response type: %s", type(result))
            return []

        decoded_result = self.decode_tag_list(result, device.details.vendor_id)

        return decoded_result

    async def get_device_objects(self, device, with_all_properties=False, index_to_read=None):
        if device.details.is_segmentation_supported():
            object_list = await self.get_object_identifiers_with_segmentation(device)
        else:
            object_list = await self.get_object_identifiers_without_segmentation(device,
                                                                                 index_to_read=index_to_read)

        if len(object_list) == 0:
            self.__log.warning("%s no objects to read", device.details.object_id)
            return

        object_list = [{'objectId': obj, 'propertyId': 'object-name' if not with_all_properties else 'all'}
                       for obj in object_list]

        return ObjectIterator(self, device, object_list, self.read_multiple_objects)

    async def get_device_values(self, device):
        return ObjectIterator(self,
                              device,
                              device.uplink_converter_config.objects_to_read,
                              self.read_multiple_objects)

    def __get_read_access_specifications(self, object_list, vendor_id):
        read_access_specifications = []
        vendor_info = get_vendor_info(vendor_id)

        for object in object_list:
            try:
                object_id = object['objectId']
                if not isinstance(object_id, ObjectIdentifier):
                    obj_str = f"{object['objectType']},{object_id}"
                    object_id = ObjectIdentifier(obj_str)

                object_class = vendor_info.get_object_class(object_id[0])
                if object_class is None:
                    self.__log.warning(f"unknown object type: {object_id}, {object_class}")
                    continue

                properties = []
                if not isinstance(object['propertyId'], set) and not isinstance(object['propertyId'], list):
                    object['propertyId'] = {object['propertyId']}

                for prop in object['propertyId']:
                    property_identifier = PropertyIdentifier(prop)

                    properties.append(property_identifier)

                ras = ReadAccessSpecification(
                    objectIdentifier=object_id,
                    listOfPropertyReferences=properties,
                )

                read_access_specifications.append(ras)
            except Exception as e:
                self.__log.error("failed to create read access specification for %s: %s", object_id, e)
                continue

        return read_access_specifications

    async def get_device_name(self, address: Address, object_id: ObjectIdentifier):
        device_name = await self.__send_request_wrapper(self.read_property,
                                                        err_msg=f"Failed to read {address} device name",
                                                        address=address,
                                                        objid=object_id,
                                                        prop='objectName')

        return device_name

    async def get_router_info(self, device_address: Address):
        router_info = {}

        # who-is method direct call doesn't trigger the indication callback
        router = await self.__send_request_wrapper(self.who_is,
                                                   err_msg=f"Failed to discover router info for {device_address}",
                                                   address=device_address)
        if router is not None:
            router = router[0] if isinstance(router, list) else router
        else:
            return None

        router_name = await self.get_device_name(router.pduSource, router.iAmDeviceIdentifier)
        if router_name is None:
            return None

        try:
            router_info['routerVendorId'] = router.vendorID
            router_info['routerId'] = router.iAmDeviceIdentifier[1]
            router_info['routerAddress'] = str(router.pduSource)
            router_info['routerName'] = router_name
        except Exception as e:
            self.__log.error("Failed to parse router %s info: %s", device_address, e)
            return None

        return router_info

    async def __send_request_wrapper(self, func, err_msg=None, *args, **kwargs):
        """
        Helper method to send a request and handle exceptions.
        """

        try:
            return await func(*args, **kwargs)
        except AbortPDU as e:
            self.__log.warning("(Request aborted) %s: %s", err_msg, e)
        except ErrorRejectAbortNack as e:
            self.__log.warning("(Request rejected) %s: %s", err_msg, e)
        except ErrorPDU as e:
            self.__log.error("(Error in request) %s: %s", err_msg, e)
        except Exception as e:
            self.__log.error("(Unexpected error in request) %s: %s", err_msg, e)

        return None

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
        self.limit = self.get_limit()
        self.func = func
        self.index = 0

    async def get_next(self):
        if self.index >= len(self.items):
            return [], {}, True

        end_index = self.index + self.limit
        result = self.items[self.index:end_index]
        self.index = end_index
        finished = self.index >= len(self.items)

        r = await self.func(self.device, result)

        return r, result, finished

    def get_limit(self):
        max_prop_count = 0

        for item in self.items:
            cur_prop_count = len(item['propertyId']) if isinstance(item['propertyId'], set) else 1
            if item['propertyId'] == 'all':
                max_prop_count = 6
                break

            if cur_prop_count > max_prop_count:
                max_prop_count = cur_prop_count

        return int(self.device.details.get_max_apdu_count() / max_prop_count) if max_prop_count > 0 else 1
