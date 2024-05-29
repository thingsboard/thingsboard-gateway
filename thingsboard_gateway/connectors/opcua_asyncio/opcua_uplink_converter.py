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

from thingsboard_gateway.connectors.opcua_asyncio.opcua_converter import OpcUaConverter
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

    def convert(self, config, val):
        if not val:
            return

        data = val.Value.Value

        if data is not None:
            if isinstance(data, LocalizedText):
                data = data.Text
            elif val.Value.VariantType == VariantType.ExtensionObject:
                data = str(data)
            elif val.Value.VariantType == VariantType.DateTime:
                if data.tzinfo is None:
                    data = data.replace(tzinfo=timezone.utc)
                data = data.isoformat()

            if config['section'] == 'timeseries':
                if val.SourceTimestamp:
                    timestamp = int(val.SourceTimestamp.replace(tzinfo=timezone.utc).timestamp() * 1000)
                elif val.ServerTimestamp:
                    timestamp = int(val.ServerTimestamp.replace(tzinfo=timezone.utc).timestamp() * 1000)
                else:
                    timestamp = int(time() * 1000)

                self.data[DATA_TYPES[config['section']]].append({'ts': timestamp, 'values': {config['key']: data}})
            else:
                self.data[DATA_TYPES[config['section']]].append({config['key']: data})

            self._log.debug('Converted data: %s', self.data)
