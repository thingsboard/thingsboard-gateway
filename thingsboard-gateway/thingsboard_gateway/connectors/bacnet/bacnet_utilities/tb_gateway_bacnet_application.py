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

from bacpypes.apdu import APDU, IAmRequest, ReadPropertyRequest, SimpleAckPDU, WhoIsRequest, WritePropertyRequest
from bacpypes.app import BIPSimpleApplication
from bacpypes.constructeddata import Any
from bacpypes.core import deferred
from bacpypes.iocb import IOCB
from bacpypes.object import get_datatype
from bacpypes.pdu import Address, GlobalBroadcast
from bacpypes.primitivedata import Null, ObjectIdentifier, Atomic, Integer, Real, Unsigned

from thingsboard_gateway.connectors.bacnet.bacnet_utilities.tb_gateway_bacnet_device import TBBACnetDevice
from thingsboard_gateway.connectors.connector import log


class TBBACnetApplication(BIPSimpleApplication):
    def __init__(self, connector, configuration):
        try:
            self.__config = configuration
            self.__connector = connector
            assert self.__config is not None
            assert self.__config.get("general") is not None
            self.requests_in_progress = {}
            self.discovered_devices = {}
            self.__device = TBBACnetDevice(self.__config["general"])
            super().__init__(self.__device, self.__config["general"]["address"])
        except Exception as e:
            log.exception(e)

    def do_whois(self, device=None):
        try:
            device = {} if device is None else device
            request = WhoIsRequest()
            address = device.get("address")
            low_limit = device.get("idLowLimit")
            high_limit = device.get("idHighLimit")
            if address is not None:
                request.pduDestination = Address(address)
            else:
                request.pduDestination = GlobalBroadcast()
            if low_limit is not None and high_limit is not None:
                request.deviceInstanceRangeLowLimit = int(low_limit)
                request.deviceInstanceRangeHighLimit = int(high_limit)
            iocb = IOCB(request)
            deferred(self.request_io, iocb)

        except Exception as e:
            log.exception(e)

    def indication(self, apdu: APDU):
        if isinstance(apdu, IAmRequest):
            log.debug("Received IAmRequest from device with ID: %i and address %s:%i",
                      apdu.iAmDeviceIdentifier[1],
                      apdu.pduSource.addrTuple[0],
                      apdu.pduSource.addrTuple[1]
                      )
            log.debug(apdu.pduSource)
            request = ReadPropertyRequest(
                destination=apdu.pduSource,
                objectIdentifier=apdu.iAmDeviceIdentifier,
                propertyIdentifier='objectName',
                )
            iocb = IOCB(request)
            deferred(self.request_io, iocb)
            iocb.add_callback(self.__iam_cb, vendor_id=apdu.vendorID)
            self.requests_in_progress.update({iocb: {"callback": self.__iam_cb}})

    def do_read_property(self, device, mapping_type=None, config=None, callback=None):
        try:
            iocb = device if isinstance(device, IOCB) else self.form_iocb(device, config, "readProperty")
            deferred(self.request_io, iocb)
            self.requests_in_progress.update({iocb: {"callback": callback,
                                                     "device": device,
                                                     "mapping_type": mapping_type,
                                                     "config": config}})
            iocb.add_callback(self.__general_cb)
        except Exception as e:
            log.exception(e)

    def do_write_property(self, device, callback=None):
        try:
            iocb = device if isinstance(device, IOCB) else self.form_iocb(device, request_type="writeProperty")
            deferred(self.request_io, iocb)
            self.requests_in_progress.update({iocb: {"callback": callback}})
            iocb.add_callback(self.__general_cb)
        except Exception as error:
            log.exception("exception: %r", error)

    def do_binary_rising_edge(self, device, callback=None):
        device["propertyValue"] = 1
        self.do_write_property(device)
        device["propertyValue"] = 0
        self.do_write_property(device)

    def check_or_add(self, device):
        device_address = device["address"] if isinstance(device["address"], Address) else Address(device["address"])
        if self.discovered_devices.get(device_address) is None:
            self.do_whois(device)
            return False
        return True

    def __on_mapping_response_cb(self, iocb: IOCB):
        try:
            if self.requests_in_progress.get(iocb) is not None:
                log.debug(iocb)
                log.debug(self.requests_in_progress[iocb])
                if iocb.ioResponse:
                    apdu = iocb.ioResponse
                    value = self.__property_value_from_apdu(apdu)
                    callback_params = self.requests_in_progress[iocb]
                    if callback_params.get("callback") is not None:
                        self.__general_cb(iocb, callback_params, value)
                elif iocb.ioError:
                    log.exception(iocb.ioError)
        except Exception as e:
            log.exception(e)
        if self.requests_in_progress.get(iocb) is not None:
            del self.requests_in_progress[iocb]

    def __general_cb(self, iocb, callback_params=None, value=None):
        try:
            if callback_params is None:
                callback_params = self.requests_in_progress[iocb]
            if iocb.ioResponse:
                apdu = iocb.ioResponse
                if isinstance(apdu, SimpleAckPDU):
                    log.debug("Write to %s - successfully.", str(apdu.pduSource))
                else:
                    log.debug("Received response: %r", apdu)
            elif iocb.ioError:
                log.exception(iocb.ioError)
            else:
                log.error("There are no data in response and no errors.")
            if isinstance(callback_params, dict) and callback_params.get("callback"):
                try:
                    callback_params["callback"](iocb, callback_params)
                except TypeError:
                    callback_params["callback"](iocb)
        except Exception as e:
            log.exception("During processing callback, exception has been raised:")
            log.exception(e)
        if self.requests_in_progress.get(iocb) is not None:
            del self.requests_in_progress[iocb]

    def __iam_cb(self, iocb: IOCB, vendor_id=None):
        if iocb.ioResponse:
            apdu = iocb.ioResponse
            log.debug("Received IAm Response: %s", str(apdu))
            if self.discovered_devices.get(apdu.pduSource) is None:
                self.discovered_devices[apdu.pduSource] = {}
            value = self.__connector.default_converters["uplink_converter"]("{}").convert(None, apdu)
            log.debug("Property: %s is %s", apdu.propertyIdentifier, value)
            self.discovered_devices[apdu.pduSource].update({apdu.propertyIdentifier: value})
            data_to_connector = {
                "address": apdu.pduSource,
                "objectId": apdu.objectIdentifier,
                "name": value,
                "vendor": vendor_id if vendor_id is not None else 0
                }
            self.__connector.add_device(data_to_connector)
        elif iocb.ioError:
            log.exception(iocb.ioError)

    @staticmethod
    def form_iocb(device, config=None, request_type="readProperty"):
        config = config if config is not None else device
        address = device["address"] if isinstance(device["address"], Address) else Address(device["address"])
        object_id = ObjectIdentifier(config["objectId"])
        property_id = config.get("propertyId")
        value = config.get("propertyValue")
        property_index = config.get("propertyIndex")
        priority = config.get("priority")
        vendor = device.get("vendor", config.get("vendorId", 0))
        request = None
        iocb = None
        if request_type == "readProperty":
            try:
                request = ReadPropertyRequest(
                    objectIdentifier=object_id,
                    propertyIdentifier=property_id
                    )
                request.pduDestination = address
                if property_index is not None:
                    request.propertyArrayIndex = int(property_index)
                iocb = IOCB(request)
            except Exception as e:
                log.exception(e)
        elif request_type == "writeProperty":
            datatype = get_datatype(object_id.value[0], property_id, vendor)
            if (isinstance(value, str) and value.lower() == 'null') or value is None:
                value = Null()
            elif issubclass(datatype, Atomic):
                if datatype is Integer:
                    value = int(value)
                elif datatype is Real:
                    value = float(value)
                elif datatype is Unsigned:
                    value = int(value)
                value = datatype(value)
            elif not isinstance(value, datatype):
                log.error("invalid result datatype, expecting %s" % (datatype.__name__,))
            request = WritePropertyRequest(
                objectIdentifier=object_id,
                propertyIdentifier=property_id
                )
            request.pduDestination = address
            request.propertyValue = Any()
            try:
                request.propertyValue.cast_in(value)
            except AttributeError as e:
                log.debug(e)
            except Exception as error:
                log.exception("WriteProperty cast error: %r", error)
            if property_index is not None:
                request.propertyArrayIndex = property_index
            if priority is not None:
                request.priority = priority
            iocb = IOCB(request)
        else:
            log.error("Request type is not found or not implemented")
        return iocb
