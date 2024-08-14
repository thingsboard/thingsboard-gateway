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

from time import time
from datetime import timezone

from thingsboard_gateway.connectors.opcua.opcua_converter import OpcUaConverter
from asyncua.ua.uatypes import LocalizedText, VariantType

DATA_TYPES = {
    'attributes': 'attributes',
    'timeseries': 'telemetry'
}


class OpcUaUplinkConverter(OpcUaConverter):
    def __init__(self, config, logger):
        self._log = logger
        self.__config = config
        self.data = {
            'deviceName': self.__config['device_name'],
            'deviceType': self.__config['device_type'],
            'attributes': [],
            'telemetry': [],
        }
        self._last_node_timestamp = 0

    def clear_data(self):
        self.data = {
            'deviceName': self.__config['device_name'],
            'deviceType': self.__config['device_type'],
            'attributes': [],
            'telemetry': [],
        }

    def get_data(self):
        if len(self.data['attributes']) or len(self.data['telemetry']):
            data_list = []
            device_names = self.__config.get('device_names')
            if device_names:
                for device in device_names:
                    self.data['deviceName'] = device
                    data_list.append(self.data)

                return data_list

            return [self.data]

        return None

    def convert(self, configs, values):
        for (val, config) in zip(values, configs):
            if not val:
                continue

            if val is not None:
                self.data[DATA_TYPES[config['section']]].append({config['key']: val})
