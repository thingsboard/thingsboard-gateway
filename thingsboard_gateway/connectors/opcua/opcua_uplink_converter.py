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

from re import fullmatch

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
        try:
            information_types = {"attributes": "attributes", "timeseries": "telemetry"}
            for information_type in information_types:
                for information in self.__config[information_type]:
                    path = TBUtility.get_value(information["path"], get_tag=True)
                    if isinstance(config, tuple):
                        config_information = config[0].replace('\\\\', '\\') if path == config[0].replace('\\\\', '\\') or fullmatch(path, config[0].replace('\\\\', '\\')) else config[1].replace('\\\\', '\\')
                    else:
                        config_information = config.replace('\\\\', '\\')
                    if path == config_information or fullmatch(path, config_information) or path.replace('\\\\', '\\') == config_information:
                        result[information_types[information_type]].append({information["key"]: information["path"].replace("${"+path+"}", str(data))})
            return result
        except Exception as e:
            log.exception(e)
