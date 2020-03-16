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

from thingsboard_gateway.connectors.connector import log
from thingsboard_gateway.connectors.bacnet.bacnet_utilities.tb_gateway_bacnet_device import TBBACnetDevice

from bacpypes.pdu import Address

from bacpypes.app import BIPSimpleApplication

from bacpypes.core import run, deferred, enable_sleeping
from bacpypes.iocb import IOCB

from bacpypes.apdu import ReadPropertyRequest
from bacpypes.primitivedata import Tag, ObjectIdentifier
from bacpypes.constructeddata import ArrayOf



class TBBACnetApplication(BIPSimpleApplication):
    def __init__(self, configuration):
        try:
            self.__config = configuration
            assert self.__config is not None
            assert self.__config.get("general") is not None
            self.requests_in_progress = {}
            self.__device = TBBACnetDevice(self.__config["general"])
            super().__init__(self.__device, self.__config["general"]["address"])
        except Exception as e:
            log.exception(e)

    def do_read(self, device, mapping_type, mapping_object, callback=None):
        try:
            object_id = mapping_object.get("objectId")
            property_id = mapping_object.get("propertyId")
            property_index = mapping_object.get("propertyIndex")
            object_id = ObjectIdentifier(object_id).value
            request = ReadPropertyRequest(
                objectIdentifier=object_id,
                propertyIdentifier=property_id
            )
            request.pduDestination = Address(device["address"])
            if property_index is not None:
                request.propertyArrayIndex = int(property_index)
            iocb = IOCB(request)
            deferred(self.request_io, iocb)
            iocb.add_callback(self.__on_response)
            self.requests_in_progress.update({iocb: {"callback": callback,
                                                     "device": device,
                                                     "mapping_type": mapping_type,
                                                     "mapping_object": mapping_object}})
        except Exception as e:
            log.exception(e)

    def __on_response(self, iocb: IOCB):
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
                # datatype = ArrayOf(datatype)
                value = apdu.propertyValue.cast_out(datatype)
                log.debug("Received callback with data: %s", str(value))
                callback_params = self.requests_in_progress[iocb]
                if callback_params["callback"] is not None:
                    callback_params["callback"](callback_params["device"]["converter"], callback_params["mapping_type"], callback_params["mapping_object"], value)
            elif iocb.ioError:
                log.exception(iocb.ioError)
            del self.requests_in_progress[iocb]
        except Exception as e:
            log.exception(e)
            del self.requests_in_progress[iocb]


