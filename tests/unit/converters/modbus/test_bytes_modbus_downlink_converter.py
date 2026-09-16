#     Copyright 2026. ThingsBoard
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

from tests.unit.BaseUnitTest import BaseUnitTest
from thingsboard_gateway.connectors.modbus.bytes_modbus_downlink_converter import BytesModbusDownlinkConverter
from thingsboard_gateway.connectors.modbus.entities.bytes_downlink_converter_config import \
    BytesDownlinkConverterConfig


class BytesModbusDownlinkConverterTests(BaseUnitTest):

    def setUp(self):
        self.converter = BytesModbusDownlinkConverter(None, self.log)

    def _config(self, function_code, objects_count):
        return BytesDownlinkConverterConfig(device_name="Modbus Test",
                                            byte_order="BIG",
                                            word_order="BIG",
                                            repack=False,
                                            objects_count=objects_count,
                                            function_code=function_code,
                                            lower_type="coils",
                                            address=0)

    def test_write_multiple_coils_preserves_bit_order(self):
        """Regression test: BinaryPayloadBuilder.to_coils() extracts bits MSB-first from the
        packed register, reversing each 8-bit group relative to the order they were added in.
        Writing coils/discrete inputs (functionCode 15) must undo that so the device receives
        the bits in the order the caller specified."""
        values = [False, True, False, False, False, False, True, True,
                 True, True, False, True, False, False, True, True]

        result = self.converter.convert(self._config(15, len(values)), {"data": {"params": values}})

        self.assertEqual(values, result)
