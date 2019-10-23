#     Copyright 2019. ThingsBoard
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

from thingsboard_gateway.connectors.opcua.opcua_converter import OpcUaConverter, log
from thingsboard_gateway.tb_utility.tb_utility import TBUtility


class OpcUaUplinkConverter(OpcUaConverter):
    def __init__(self, config):
        self.__config = config

    def convert(self, path, data):
        device_name = self.__config["deviceName"]
        result = {"deviceName": device_name,
                  "deviceType": self.__config.get("deviceType", "OPC-UA Device"),
                  "attributes": [],
                  "telemetry": [], }
        current_variable = path.split('.')[-1]
        try:
            for attr in self.__config["attributes"]:
                if TBUtility.get_value(attr["path"], get_tag=True) == current_variable:
                    result["attributes"].append({attr["key"]: attr["path"].replace("${"+current_variable+"}", str(data))})
            for ts in self.__config["timeseries"]:
                if TBUtility.get_value(ts["path"], get_tag=True) == current_variable:
                    result["telemetry"].append({ts["key"]: ts["path"].replace("${"+current_variable+"}", str(data))})
            return result
        except Exception as e:
            log.exception(e)
