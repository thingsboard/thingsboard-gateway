#     Copyright 2025. ThingsBoard
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

from pymodbus.constants import Endian


class BytesUplinkConverterConfig:
    def __init__(self, **kwargs):
        self.report_strategy = kwargs.get('reportStrategy')
        self.device_name = kwargs['deviceName']
        self.device_type = kwargs.get('deviceType', 'default')
        self.byte_order = Endian.BIG if kwargs.get('byteOrder', 'LITTLE').upper() == "BIG" else Endian.LITTLE
        self.word_order = Endian.BIG if kwargs.get('wordOrder', 'LITTLE').upper() == "BIG" else Endian.LITTLE
        self.telemetry = kwargs.get('timeseries', [])
        self.attributes = kwargs.get('attributes', [])
        self.unit_id = kwargs['unitId']

    def is_readable(self):
        return len(self.telemetry) > 0 or len(self.attributes) > 0
