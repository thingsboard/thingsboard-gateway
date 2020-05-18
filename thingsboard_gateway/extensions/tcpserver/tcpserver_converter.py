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


import binascii
import struct
from thingsboard_gateway.connectors.converter import Converter, log


class BytesTcpUplinkConverter(Converter):
    def __init__(self, config):
        self.__config = config
        self.result_dict = {
            'deviceName': config.get('deviceName', 'CustomSerialDevice'),
            'deviceType': config.get('deviceType', 'default'),
            'attributes': [],
            'timeseries': []
        }

    def convert(self, data):
        keys = ['attributes', 'timeseries']
        # print("data:", binascii.hexlify(data))
        for key in keys:
            self.result_dict[key] = []
            if self.__config.get(key) is not None:
                for config_object in self.__config.get(key):
                    data_to_convert = data
                    if config_object.get('toByte') is not None:
                        to_byte = config_object.get('toByte')
                        if to_byte == -1:
                            to_byte = len(data) - 1
                        data_to_convert = data_to_convert[:to_byte + 1]
                    if config_object.get('fromByte') is not None:
                        from_byte = config_object.get('fromByte')
                        data_to_convert = data_to_convert[from_byte:]
                    # data_to_convert = binascii.b2a_hex(data_to_convert)
                    if len(data_to_convert) < 4:
                        convert_data = int(binascii.b2a_hex(data_to_convert), 16)
                    else:
                        if config_object.get('byteOrder').lower() == 'big':
                            convert_data, = struct.unpack('>f', data_to_convert)
                        if config_object.get('byteOrder').lower() == 'little':
                            convert_data = struct.unpack('<f', data_to_convert)
                    converted_data = {config_object['tag']: convert_data}
                    self.result_dict[key].append(converted_data)
        # print("result_dict:", self.result_dict)
        return self.result_dict