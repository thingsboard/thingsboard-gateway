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
    def __init__(self, configuration):
        try:
            self.__config = configuration
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
                      *apdu.pduSource.addrTuple
                      )
            log.debug(apdu.pduSource)
            request = ReadPropertyRequest(
                destination=apdu.pduSource,
                objectIdentifier=apdu.iAmDeviceIdentifier,
                propertyIdentifier='objectName',
            )
            iocb = IOCB(request)
            deferred(self.request_io, iocb)
            iocb.add_callback(self.__on_mapping_response_cb)
            self.requests_in_progress.update({iocb: {"callback": self.__general_cb}})

    def do_read(self, device, mapping_type, mapping_object, callback=None):
        try:
            object_id = mapping_object.get("objectId")
            property_id = mapping_object.get("propertyId")
            property_index = mapping_object.get("propertyIndex")
            object_id = ObjectIdentifier(object_id)
            request = ReadPropertyRequest(
                objectIdentifier=object_id,
                propertyIdentifier=property_id
            )
            request.pduDestination = Address(device["address"])
            if self.discovered_devices.get(request.pduDestination) is not None:
                # TODO Add ability to select any property for getting name and move this into converter
                device_name_tag = TBUtility.get_value(device["deviceName"], get_tag=True)
                if self.discovered_devices[request.pduDestination].get(device_name_tag) is not None:
                    device_name = device["deviceName"].replace("${%s}" % (device_name_tag,), self.discovered_devices[request.pduDestination][device_name_tag])
                    mapping_object.update({"deviceName": device_name})
                if property_index is not None:
                    request.propertyArrayIndex = int(property_index)
                iocb = IOCB(request)
                deferred(self.request_io, iocb)
                iocb.add_callback(self.__on_mapping_response_cb)
                self.requests_in_progress.update({iocb: {"callback": callback,
                                                         "device": device,
                                                         "mapping_type": mapping_type,
                                                         "mapping_object": mapping_object}})
        except Exception as e:
            log.exception(e)
    
    def do_write(self, device, content):
        address = content.get("address")
        objectId = content.get("objectId")
        propertyId = content.get("propertyId")
        value = content.get("value")
        index = content.get("index")
        priority = content.get("priority")
        log.debug("do_write %r", args)

        try:
            addr, obj_id, prop_id = args[:3]
            obj_id = ObjectIdentifier(obj_id).value
            value = args[3]
            indx = None
            if len(args) >= 5:
                if args[4] != "-":
                    indx = int(args[4])
            log.debug("    - indx: %r", indx)
            priority = None
            if len(args) >= 6:
                priority = int(args[5])
            log.debug("    - priority: %r", priority)
            datatype = get_datatype(obj_id[0], prop_id)
            log.debug("    - datatype: %r", datatype)

            # change atomic values into something encodeable, null is a special case
            if (value == 'null'):
                value = Null()
            elif issubclass(datatype, AnyAtomic):
                dtype, dvalue = value.split(':', 1)
                log.debug("    - dtype, dvalue: %r, %r", dtype, dvalue)

                datatype = {
                    'b': Boolean,
                    'u': lambda x: Unsigned(int(x)),
                    'i': lambda x: Integer(int(x)),
                    'r': lambda x: Real(float(x)),
                    'd': lambda x: Double(float(x)),
                    'o': OctetString,
                    'c': CharacterString,
                    'bs': BitString,
                    'date': Date,
                    'time': Time,
                    'id': ObjectIdentifier,
                }[dtype]
                log.debug("    - datatype: %r", datatype)

                value = datatype(dvalue)
                log.debug("    - value: %r", value)

            elif issubclass(datatype, Atomic):
                if datatype is Integer:
                    value = int(value)
                elif datatype is Real:
                    value = float(value)
                elif datatype is Unsigned:
                    value = int(value)
                value = datatype(value)
            elif issubclass(datatype, Array) and (indx is not None):
                if indx == 0:
                    value = Integer(value)
                elif issubclass(datatype.subtype, Atomic):
                    value = datatype.subtype(value)
                elif not isinstance(value, datatype.subtype):
                    raise TypeError("invalid result datatype, expecting %s" % (datatype.subtype.__name__,))
            elif not isinstance(value, datatype):
                raise TypeError("invalid result datatype, expecting %s" % (datatype.__name__,))
            log.debug("    - encodeable value: %r %s", value, type(value))

            # build a request
            request = WritePropertyRequest(
                objectIdentifier=obj_id,
                propertyIdentifier=prop_id
            )
            request.pduDestination = Address(addr)

            # save the value
            request.propertyValue = Any()
            try:
                request.propertyValue.cast_in(value)
            except Exception as error:
                log.exception("WriteProperty cast error: %r", error)

            # optional array index
            if indx is not None:
                request.propertyArrayIndex = indx

            # optional priority
            if priority is not None:
                request.priority = priority

            log.debug("    - request: %r", request)

            # make an IOCB
            iocb = IOCB(request)
            log.debug("    - iocb: %r", iocb)

            # give it to the application
            deferred(self.request_io, iocb)

            # wait for it to complete
            iocb.wait()

            # do something for success
            if iocb.ioResponse:
                # should be an ack
                if not isinstance(iocb.ioResponse, SimpleAckPDU):
                    log.debug("    - not an ack")
                    return

            # do something for error/reject/abort
            if iocb.ioError:
                log.error(str(iocb.ioError) + '\n')

        except Exception as error:
            log.exception("exception: %r", error)

    def check_or_add(self, device):
        device_address = Address(device["address"])
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
                log.debug("Received callback with data: %s", str(value))
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
            callback_params["callback"](callback_params["device"]["converter"], callback_params["mapping_type"], callback_params["mapping_object"], value)
        else:
            if iocb.ioResponse:
                apdu = iocb.ioResponse
                if self.discovered_devices.get(apdu.pduSource) is None:
                    self.discovered_devices[apdu.pduSource] = {}
                self.discovered_devices[apdu.pduSource].update({apdu.propertyIdentifier: value})
            elif iocb.ioError:
                log.exception(iocb.ioError)
            else:
                log.error("There are no data in response and no errors.")

