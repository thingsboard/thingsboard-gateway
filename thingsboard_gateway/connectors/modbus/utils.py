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

from pymodbus import ExceptionResponse
from pymodbus.exceptions import ModbusIOException


class Utils:
    @staticmethod
    def is_wide_range_request(address):
        if not isinstance(address, str):
            return False

        return '-' in address

    @staticmethod
    def parse_wide_range_request(address, objects_count=1):
        try:
            address_range = address.split('-')
            registers_to_read = (int(address_range[1]) - int(address_range[0]) + 1) * objects_count
            return int(address_range[0]), registers_to_read
        except Exception as e:
            raise ValueError('Invalid address range: {}'.format(e))

    @staticmethod
    def get_start_address(address_pattern):
        if isinstance(address_pattern, int):
            return address_pattern

        if Utils.is_wide_range_request(address_pattern):
            return Utils.parse_wide_range_request(address_pattern)[0]

        raise ValueError('Invalid address pattern')

    @staticmethod
    def is_encoded_data_valid(encoded_data):
        if encoded_data is None:
            return False

        return not isinstance(encoded_data, ModbusIOException) and not isinstance(encoded_data, ExceptionResponse)

    @staticmethod
    def get_registers_from_encoded_data(encoded_data, function_code):
        if function_code in (1, 2):
            encoded_data = encoded_data.bits
        elif function_code in (3, 4):
            encoded_data = encoded_data.registers
        else:
            raise ValueError(f"Unsupported function code: {function_code}")

        return encoded_data
