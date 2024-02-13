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

from thingsboard_gateway.connectors.socket.socket_uplink_converter import SocketUplinkConverter
from thingsboard_gateway.gateway.statistics_service import StatisticsService


class BytesSocketUplinkConverter(SocketUplinkConverter):
    def __init__(self, config, logger):
        self._log = logger
        self.__config = config

    @StatisticsService.CollectStatistics(start_stat_type='receivedBytesFromDevices',
                                         end_stat_type='convertedBytesFromDevice')
    def convert(self, config, data):
        if data is None:
            return {}

        dict_result = {
            "deviceName": self.__config['deviceName'],
            "deviceType": self.__config['deviceType']
        }

        try:
            dict_result["telemetry"] = []
            dict_result["attributes"] = []

            for section in ('telemetry', 'attributes'):
                for item in config[section]:
                    try:
                        byte_from = item.get('byteFrom')
                        byte_to = item.get('byteTo')

                        byte_to = byte_to if byte_to != -1 else len(data)
                        converted_data = data[byte_from:byte_to]
                        if config['encoding'] == 'hex':
                            converted_data = converted_data.hex()
                        else:
                            try:
                                converted_data = converted_data.replace(b"\x00", b'').decode(config['encoding'])
                            except UnicodeDecodeError:
                                converted_data = str(converted_data)

                        if item.get('key') is not None:
                            dict_result[section].append(
                                {item['key']: converted_data})
                        else:
                            self._log.error('Key for %s not found in config: %s', config['type'], config['section_config'])
                    except Exception as e:
                        self._log.exception(e)
        except Exception as e:
            self._log.exception(e)

        self._log.debug(dict_result)
        return dict_result
