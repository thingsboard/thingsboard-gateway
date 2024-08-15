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
            if not val or val is None:
                continue

            data = val

            if not isinstance(data, (int, float, str, bool, dict, list, type(None), LocalizedText)):
                self._log.info(f"Non primitive data type: {type(data)}")
                if isinstance(data, LocalizedText):
                    data = data.Text
                elif val.VariantType == VariantType.ExtensionObject:
                    data = str(data)
                elif val.VariantType == VariantType.DateTime:
                    if data.tzinfo is None:
                        data = data.replace(tzinfo=timezone.utc)
                    data = data.isoformat()
                elif val.VariantType == VariantType.StatusCode:
                    data = data.name
                elif (val.VariantType == VariantType.QualifiedName
                      or val.VariantType == VariantType.NodeId
                      or val.VariantType == VariantType.ExpandedNodeId):
                    data = data.to_string()
                elif val.VariantType == VariantType.ByteString:
                    data = data.hex()
                elif val.VariantType == VariantType.XmlElement:
                    data = data.decode('utf-8')
                elif val.VariantType == VariantType.Guid:
                    data = str(data)
                elif val.VariantType == VariantType.DiagnosticInfo:
                    data = data.to_string()
                elif val.VariantType == VariantType.Null:
                    data = None
                else:
                    self._log.info(f"Unsupported data type: {val.VariantType}, will be processed as a string.")
                    data = str(data)

            self.data[DATA_TYPES[config['section']]].append({config['key']: data})
