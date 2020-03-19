#      Copyright 2020. ThingsBoard
#  #
#      Licensed under the Apache License, Version 2.0 (the "License");
#      you may not use this file except in compliance with the License.
#      You may obtain a copy of the License at
#  #
#          http://www.apache.org/licenses/LICENSE-2.0
#  #
#      Unless required by applicable law or agreed to in writing, software
#      distributed under the License is distributed on an "AS IS" BASIS,
#      WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#      See the License for the specific language governing permissions and
#      limitations under the License.

from thingsboard_gateway.connectors.bacnet.bacnet_converter import BACnetConverter, log
from thingsboard_gateway.tb_utility.tb_utility import TBUtility


class BACnetUplinkConverter:
    def __init__(self, config):
        self.__config = config

    def convert(self, config, data):
        datatypes = {"attributes": "attributes",
                     "timeseries": "telemetry"}
        dict_result = {"deviceName": None, "deviceType": None, "attributes": [], "telemetry": []}
        dict_result["deviceName"] = config[1].get("deviceName", self.__config.get("name", "BACnet device"))
        dict_result["deviceType"] = config[1].get("deviceType", self.__config.get("deviceType", "default"))
        dict_result[datatypes[config[0]]].append({config[1]["key"]: data})
        return dict_result


