#  Copyright 2020. ThingsBoard
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

from bacpypes.pdu import Address, GlobalBroadcast
from bacpypes.object import get_datatype
from bacpypes.apdu import APDU

from bacpypes.app import BIPSimpleApplication

from bacpypes.core import run, deferred, enable_sleeping
from bacpypes.iocb import IOCB

from bacpypes.apdu import ReadPropertyRequest, WhoIsRequest, IAmRequest, WritePropertyRequest, SimpleAckPDU
from bacpypes.primitivedata import Tag, Null, Atomic, Boolean, Unsigned, Integer, \
    Real, Double, OctetString, CharacterString, BitString, Date, Time, ObjectIdentifier
from bacpypes.constructeddata import ArrayOf, Array, Any, AnyAtomic

from thingsboard_gateway.connectors.connector import log
from thingsboard_gateway.tb_utility.tb_utility import TBUtility
from thingsboard_gateway.connectors.bacnet.bacnet_utilities.tb_gateway_bacnet_device import TBBACnetDevice


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
            iocb.add_callback(self.__iam_cb)
            self.requests_in_progress.update({iocb: {"callback": self.__iam_cb}})

    def do_read_property(self, device, mapping_type, mapping_object, callback=None):
        try:
            iocb = self.form_iocb(device, mapping_object, "readProperty")
            deferred(self.request_io, iocb)
            iocb.add_callback(self.__on_mapping_response_cb)
            self.requests_in_progress.update({iocb: {"callback": callback,
                                                     "device": device,
                                                     "mapping_type": mapping_type,
                                                     "mapping_object": mapping_object}})
        except Exception as e:
            log.exception(e)
    
    def do_write_property(self, device):
        try:
            iocb = self.form_iocb(device, request_type="writeProperty")
            deferred(self.request_io, iocb)
            callback_params = {}
            iocb.add_callback(self.__general_cb, (iocb, callback_params, None))
        except Exception as error:
            log.exception("exception: %r", error)

    def check_or_add(self, device):
        device_address = device["address"] if isinstance(device["address"], Address) else Address(device["address"])
        if self.discovered_devices.get(device_address) is None:
            self.do_whois(device)
            return False
        return True

    def __on_mapping_response_cb(self, iocb: IOCB):
        try:
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
            del self.requests_in_progress[iocb]
        except Exception as e:
            log.exception(e)
            del self.requests_in_progress[iocb]

    def __general_cb(self, iocb, callback_params, value):
        log.debug(callback_params)
        if callback_params.get("mapping_type") is not None:
            callback_params["callback"](callback_params["mapping_object"]["uplink_converter"], callback_params["mapping_type"], callback_params["mapping_object"], value)
        else:
            if iocb.ioResponse:
                apdu = iocb.ioResponse
                if isinstance(apdu, SimpleAckPDU):
                    log.debug("Write to %s - successfully.", str(apdu.pduSource))
            elif iocb.ioError:
                log.exception(iocb.ioError)
            else:
                log.error("There are no data in response and no errors.")

    def __iam_cb(self, iocb: IOCB, *args):
        if iocb.ioResponse:
            apdu = iocb.ioResponse
            log.debug(args)
            log.debug("Received IAm Response: %s", str(apdu))
            if self.discovered_devices.get(apdu.pduSource) is None:
                self.discovered_devices[apdu.pduSource] = {}
            value = self.__property_value_from_apdu(apdu)
            log.debug("Property: %s is %s", apdu.propertyIdentifier, value)
            self.discovered_devices[apdu.pduSource].update({apdu.propertyIdentifier: value})
            data_to_connector = {
                "address": apdu.pduSource,
                "objectId": apdu.objectIdentifier,
                "name": self.__property_value_from_apdu(apdu),

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
            datatype = get_datatype(object_id.value[0], property_id)
            if isinstance(value, str) and value.lower() == 'null':
                value = Null()
            datatype(value)
            request = WritePropertyRequest(
                objectIdentifier=object_id,
                propertyIdentifier=property_id
            )
            request.pduDestination = Address(address)
            request.propertyValue = Any()
            try:
                request.propertyValue.cast_in(value)
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

    @staticmethod
    def __property_value_from_apdu(apdu: APDU):
        tag_list = apdu.propertyValue.tagList
        non_app_tags = [tag for tag in tag_list if tag.tagClass != Tag.applicationTagClass]
        if non_app_tags:
            raise RuntimeError("Value has some non-application tags")
        first_tag = tag_list[0]
        other_type_tags = [tag for tag in tag_list[1:] if tag.tagNumber != first_tag.tagNumber]
        if other_type_tags:
            raise RuntimeError("All tags must be the same type")
        datatype = Tag._app_tag_class[first_tag.tagNumber]
        if not datatype:
            raise RuntimeError("unknown datatype")
        if len(tag_list) > 1:
            datatype = ArrayOf(datatype)
        value = apdu.propertyValue.cast_out(datatype)
        return value
