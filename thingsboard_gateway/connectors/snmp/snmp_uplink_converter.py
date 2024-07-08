#     Copyright 2024. ThingsBoard
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

from thingsboard_gateway.connectors.converter import Converter
from thingsboard_gateway.gateway.statistics_service import StatisticsService


class SNMPUplinkConverter(Converter):
    def __init__(self, config, logger):
        self._log = logger
        self.__config = config

    @StatisticsService.CollectStatistics(start_stat_type='receivedBytesFromDevices',
                                         end_stat_type='convertedBytesFromDevice')
    def convert(self, config, data):
        converted_data = {
            "deviceName": self.__config["deviceName"],
            "deviceType": self.__config["deviceType"],
            'telemetry': [],
            'attributes': []
        }

        try:
            for datatype in ('attributes', 'telemetry'):
                for datatype_config in config[datatype]:
                    data_key = datatype_config["key"]
                    item_data = data.get(data_key)
                    if isinstance(item_data, dict):
                        converted_data[datatype].append({data_key: {str(k): str(v) for k, v in item_data.items()}})
                    elif isinstance(item_data, list):
                        if isinstance(item_data[0], str):
                            converted_data[datatype].append({data_key: ','.join(item_data)})
                        elif isinstance(item_data[0], dict):
                            res = {}
                            for item in item_data:
                                res.update(**item)
                            converted_data[datatype].append({data_key: {str(k): str(v) for k, v in res.items()}})
                    elif isinstance(item_data, str):
                        converted_data[datatype].append({data_key: item_data})
                    elif isinstance(item_data, bytes):
                        converted_data[datatype].append({data_key: item_data.decode("UTF-8")})
                    else:
                        converted_data[datatype].append({data_key: item_data})
        except Exception as e:
            self._log.exception(e)

        self._log.debug(converted_data)
        return converted_data
