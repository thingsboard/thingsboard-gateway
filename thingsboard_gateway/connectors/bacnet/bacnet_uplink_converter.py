#     Copyright 2021. ThingsBoard
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

from bacpypes.apdu import APDU, ReadPropertyACK
from bacpypes.primitivedata import Tag
from bacpypes.constructeddata import ArrayOf
from thingsboard_gateway.connectors.bacnet.bacnet_converter import BACnetConverter, log
from thingsboard_gateway.tb_utility.tb_utility import TBUtility


class BACnetUplinkConverter(BACnetConverter):
    def __init__(self, config):
        self.__config = config

    def convert(self, config, data):
        value = None
        if isinstance(data, ReadPropertyACK):
            value = self.__property_value_from_apdu(data)
        if config is not None:
            datatypes = {"attributes": "attributes",
                         "timeseries": "telemetry",
                         "telemetry": "telemetry"}
            dict_result = {"deviceName": None, "deviceType": None, "attributes": [], "telemetry": []}
            dict_result["deviceName"] = self.__config.get("deviceName", config[1].get("name", "BACnet device"))
            dict_result["deviceType"] = self.__config.get("deviceType", "default")
            dict_result[datatypes[config[0]]].append({config[1]["key"]: value})
        else:
            dict_result = value
        log.debug("%r %r", self, dict_result)
        return dict_result

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


