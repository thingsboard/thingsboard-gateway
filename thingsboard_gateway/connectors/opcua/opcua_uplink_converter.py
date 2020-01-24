#     Copyright 2020. ThingsBoard
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

    def convert(self, config, data):
        device_name = self.__config["deviceName"]
        result = {"deviceName": device_name,
                  "deviceType": self.__config.get("deviceType", "OPC-UA Device"),
                  "attributes": [],
                  "telemetry": [], }
        current_variable = config.split('.')
        try:
            for attr in self.__config["attributes"]:
                path = TBUtility.get_value(attr["path"], get_tag=True)
                if path == '.'.join(current_variable[-len(path.split('.')):]):
                    result["attributes"].append({attr["key"]: attr["path"].replace("${"+path+"}", str(data))})
            for ts in self.__config["timeseries"]:
                path = TBUtility.get_value(ts["path"], get_tag=True)
                if path == '.'.join(current_variable[-len(path.split('.')):]):
                    result["telemetry"].append({ts["key"]: ts["path"].replace("${"+path+"}", str(data))})
            return result
        except Exception as e:
            log.exception(e)
