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


class CustomSerialUplinkConverter(Converter):
    def __init__(self, config, logger):
        self._log = logger
        self.__config = config

    def convert(self, config, data: bytes):
        dict_result = {
            'deviceName': self.__config.get('name', 'CustomSerialDevice'),
            'deviceType': self.__config.get('deviceType', 'default'),
            'attributes': [],
            'telemetry': []
        }
        keys = ['attributes', 'telemetry']
        for key in keys:
            dict_result[key] = []
            if self.__config.get(key) is not None:
                for config_object in self.__config.get(key):
                    data_to_convert = data
                    if config_object.get('untilDelimiter') is not None:
                        data_to_convert = data.split(config_object.get('untilDelimiter').encode('UTF-8'))[0]
                    if config_object.get('fromDelimiter') is not None:
                        data_to_convert = data.split(config_object.get('fromDelimiter').encode('UTF-8'))[1]
                    if config_object.get('toByte') is not None:
                        to_byte = config_object.get('toByte')
                        if to_byte == -1:
                            to_byte = len(data) - 1
                        data_to_convert = data_to_convert[:to_byte]
                    if config_object.get('fromByte') is not None:
                        from_byte = config_object.get('fromByte')
                        data_to_convert = data_to_convert[from_byte:]
                    converted_data = {config_object['key']: data_to_convert.decode('UTF-8')}
                    dict_result[key].append(converted_data)
        self._log.debug("Converted data: %s", dict_result)
        return dict_result
