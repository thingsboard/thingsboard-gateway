import unittest

try:
    from pymodbus.constants import Endian
except (ImportError, ModuleNotFoundError):
    from thingsboard_gateway.tb_utility.tb_utility import TBUtility
    TBUtility.install_package("pymodbus", version="3.0.0", force_install=True)
    from pymodbus.constants import Endian

from pymodbus.payload import BinaryPayloadBuilder

from tests.unit.BaseUnitTest import BaseUnitTest
from thingsboard_gateway.connectors.modbus.bytes_modbus_uplink_converter import BytesModbusUplinkConverter


class ModbusConverterTests(BaseUnitTest):
    @unittest.skip("Skip tests, because builder contains wrong numbers, should be fixed in future.")
    def test_modbus_getting_values(self):
        self.maxDiff = None
        test_modbus_config = {
            "attributes": [
                {"string": {
                    "byteOrder": "BIG",
                    "wordOrder": "BIG",
                    "tag": "string",
                    "type": "string",
                    "functionCode": 4,
                    "objectsCount": 4
                }},
                {"bits": {
                    "byteOrder": "BIG",
                    "wordOrder": "BIG",
                    "tag": "bits",
                    "type": "bits",
                    "functionCode": 4,
                    "objectsCount": 8
                }},
                {"bits2": {
                    "byteOrder": "BIG",
                    "wordOrder": "BIG",
                    "tag": "bits",
                    "type": "bits",
                    "functionCode": 4,
                    "objectsCount": 2
                }},
                {"8int": {
                    "byteOrder": "BIG",
                    "wordOrder": "BIG",
                    "tag": "8int",
                    "type": "8int",
                    "functionCode": 4,
                    "objectsCount": 1
                }},
                {"16int": {
                    "byteOrder": "BIG",
                    "wordOrder": "BIG",
                    "tag": "16int",
                    "type": "16int",
                    "functionCode": 4,
                    "objectsCount": 1
                }},
                {"long": {
                    "byteOrder": "BIG",
                    "wordOrder": "BIG",
                    "tag": "long",
                    "type": "long",
                    "functionCode": 4,
                    "objectsCount": 1
                }},
                {"long_with_divider": {
                    "byteOrder": "BIG",
                    "wordOrder": "BIG",
                    "tag": "long",
                    "type": "long",
                    "functionCode": 4,
                    "objectsCount": 1,
                    "divider": 10
                }},
                {"32int": {
                    "byteOrder": "BIG",
                    "wordOrder": "BIG",
                    "tag": "32int",
                    "type": "32int",
                    "functionCode": 4,
                    "objectsCount": 2
                }},
                {"64int": {
                    "byteOrder": "BIG",
                    "wordOrder": "BIG",
                    "tag": "64int",
                    "type": "64int",
                    "functionCode": 4,
                    "objectsCount": 4
                }},
            ],
            "timeseries": [
                {"8uint": {
                    "byteOrder": "BIG",
                    "wordOrder": "BIG",
                    "tag": "8uint",
                    "type": "8uint",
                    "functionCode": 4,
                    "objectsCount": 1}},
                {"16uint": {
                    "byteOrder": "BIG",
                    "wordOrder": "BIG",
                    "tag": "16uint",
                    "type": "16uint",
                    "functionCode": 4,
                    "objectsCount": 2}},
                {"32uint": {
                    "byteOrder": "BIG",
                    "wordOrder": "BIG",
                    "tag": "32uint",
                    "type": "32uint",
                    "functionCode": 4,
                    "objectsCount": 4}},
                {"64uint": {
                    "byteOrder": "BIG",
                    "wordOrder": "BIG",
                    "tag": "64uint",
                    "type": "64uint",
                    "functionCode": 4,
                    "objectsCount": 1}},
                {"double": {
                    "byteOrder": "BIG",
                    "wordOrder": "BIG",
                    "tag": "double",
                    "type": "double",
                    "functionCode": 4,
                    "objectsCount": 2}},
                {"16float": {
                    "byteOrder": "BIG",
                    "wordOrder": "BIG",
                    "tag": "16float",
                    "type": "16float",
                    "functionCode": 4,
                    "objectsCount": 1}},
                {"32float": {
                    "byteOrder": "BIG",
                    "wordOrder": "BIG",
                    "tag": "32float",
                    "type": "32float",
                    "functionCode": 4,
                    "objectsCount": 2}},
                {"64float": {
                    "byteOrder": "BIG",
                    "wordOrder": "BIG",
                    "tag": "64float",
                    "type": "64float",
                    "functionCode": 4,
                    "objectsCount": 4}},
            ]
        }
        test_modbus_body_to_convert = {}
        test_modbus_convert_config = {}
        test_modbus_result = {'deviceName': 'Modbus Test',
                              'deviceType': 'default',
                              'telemetry': [
                                  {'8uint': 18},
                                  {'16uint': 4660},
                                  {'32uint': 305419896},
                                  {'64uint': 1311768468603649775},
                                  {'double': 22.5},
                                  {'16float': 1.240234375},
                                  {'32float': 22.34000015258789},
                                  {'64float': -123.45}],
                              'attributes': [
                                  {'string': 'abcdefgh'},
                                  {'bits': [False, True, False, True, False, False, True, True]},
                                  {'bits2': [True, True]},
                                  {'8int': -18},
                                  {'16int': -22136},
                                  {'long': -22136},
                                  {'long_with_divider': -2213.6},
                                  {'32int': -4660},
                                  {'64int': -3735928559}]
                              }

        builder = BinaryPayloadBuilder(byteorder=Endian.BIG)
        builder_registers = {"string": (builder.add_string, 'abcdefgh'),
                             "bits": (builder.add_bits, [0, 1, 0, 1, 0, 0, 1, 1]),
                             "bits2": (builder.add_bits, [1, 1]),
                             "8int": (builder.add_8bit_int, -0x12),
                             "16int": (builder.add_16bit_int, -0x5678),
                             "long": (builder.add_16bit_int, -0x5678),
                             "long_with_divider": (builder.add_16bit_int, -0x5678),
                             "32int": (builder.add_32bit_int, -0x1234),
                             "64int": (builder.add_64bit_int, -0xDEADBEEF),
                             "8uint": (builder.add_8bit_uint, 0x12),
                             "16uint": (builder.add_16bit_uint, 0x1234),
                             "32uint": (builder.add_32bit_uint, 0x12345678),
                             "64uint": (builder.add_64bit_uint, 0x12345678DEADBEEF),
                             "double": (builder.add_32bit_float, 22.5),
                             "16float": (builder.add_16bit_float, 1.24),
                             "32float": (builder.add_32bit_float, 22.34),
                             "64float": (builder.add_64bit_float, -123.45),
                             }

        class DummyResponse:
            def __init__(self, registers):
                self.registers = registers[:]

        for datatype in test_modbus_config:
            test_modbus_body_to_convert[datatype] = {}
            for tag_dict in test_modbus_config[datatype]:
                for tag in tag_dict:
                    builder_registers[tag][0](builder_registers[tag][1])
                    test_modbus_body_to_convert[datatype].update(
                        {tag: {"input_data": DummyResponse(builder.to_registers()), "data_sent": tag_dict[tag]}})
                    builder.reset()

        converter = BytesModbusUplinkConverter({"deviceName": "Modbus Test", "deviceType": "default", "unitId": 1}, logger=self.log)
        result = converter.convert(test_modbus_convert_config, test_modbus_body_to_convert)
        self.assertDictEqual(result, test_modbus_result)

    def test_decode_bits_preserves_original_bit_order(self):
        """Regression test for https://github.com/thingsboard/thingsboard-gateway/issues/2146:
        decoding 'bit'/'bits' values must return the bits in their original order instead of
        reading them backwards."""
        converter = BytesModbusUplinkConverter({"deviceName": "Modbus Test", "deviceType": "default", "unitId": 1},
                                               logger=self.log)

        # Coils/discrete inputs (functionCode 1/2) are decoded via BinaryPayloadDecoder.fromCoils(),
        # which internally reverses bit order within each byte.
        coils = [True, False, False, False, False, False, False, False]
        self.assertEqual(
            converter.decode_data(coils, {'functionCode': 2, 'type': 'bit', 'objectsCount': 2},
                                  Endian.LITTLE, Endian.BIG),
            [True, False])
        self.assertEqual(
            converter.decode_data(coils, {'functionCode': 1, 'type': 'bit', 'objectsCount': 1},
                                  Endian.LITTLE, Endian.BIG),
            True)

        coils_full_byte = [True, True, False, True, False, False, True, False]
        self.assertEqual(
            converter.decode_data(coils_full_byte, {'functionCode': 1, 'type': 'bits', 'objectsCount': 8},
                                  Endian.LITTLE, Endian.BIG),
            coils_full_byte)

        coils_two_bytes = [True, True, False, False, False, False, False, False,
                           True, False, False, False, False, False, False, False]
        self.assertEqual(
            converter.decode_data(coils_two_bytes, {'functionCode': 1, 'type': 'bits', 'objectsCount': 16},
                                  Endian.LITTLE, Endian.BIG),
            coils_two_bytes)

        # Holding/input registers (functionCode 3/4) are decoded via BinaryPayloadDecoder.fromRegisters(),
        # which does not reverse bit order, so no un-reversal should be applied there.
        builder = BinaryPayloadBuilder(byteorder=Endian.BIG)
        builder.add_bits([0, 1, 0, 1, 0, 0, 1, 1])
        registers = builder.to_registers()
        self.assertEqual(
            converter.decode_data(registers, {'functionCode': 4, 'type': 'bits', 'objectsCount': 8},
                                  Endian.BIG, Endian.BIG),
            [False, True, False, True, False, False, True, True])

        builder.reset()
        builder.add_bits([1, 1])
        registers = builder.to_registers()
        self.assertEqual(
            converter.decode_data(registers, {'functionCode': 4, 'type': 'bits', 'objectsCount': 2},
                                  Endian.BIG, Endian.BIG),
            [True, True])

    def test_decode_single_bit_from_holding_register_with_objects_count_one(self):
        """Regression test for https://github.com/thingsboard/thingsboard-gateway/issues/2101:
        selecting an individual bit (0-15) from a single holding/input register (objectsCount: 1)
        must address the correct bit instead of always collapsing to bit 0."""
        converter = BytesModbusUplinkConverter({"deviceName": "Modbus Test", "deviceType": "default", "unitId": 1},
                                               logger=self.log)

        # 25729 (0x6481): high byte 0x64 bits LSB-first, then low byte 0x81 bits LSB-first,
        # as produced by BinaryPayloadDecoder.decode_bits() for register data.
        expected_bits = [0, 0, 1, 0, 0, 1, 1, 0, 1, 0, 0, 0, 0, 0, 0, 1]
        registers = [25729]

        for bit_index, expected_value in enumerate(expected_bits):
            self.assertEqual(
                converter.decode_data(registers,
                                      {'functionCode': 3, 'type': 'bits', 'objectsCount': 1,
                                       'bit': bit_index, 'bitTargetType': 'int'},
                                      Endian.BIG, Endian.BIG),
                expected_value,
                f"Mismatch for bit {bit_index}")

        # A different register value should flip the bits accordingly, proving each bit index
        # is independently addressable rather than every index collapsing to the same value.
        # 9345 (0x2481): high byte 0x24, low byte 0x81.
        other_expected_bits = [0, 0, 1, 0, 0, 1, 0, 0, 1, 0, 0, 0, 0, 0, 0, 1]
        other_registers = [9345]

        for bit_index, expected_value in enumerate(other_expected_bits):
            self.assertEqual(
                converter.decode_data(other_registers,
                                      {'functionCode': 3, 'type': 'bits', 'objectsCount': 1,
                                       'bit': bit_index, 'bitTargetType': 'int'},
                                      Endian.BIG, Endian.BIG),
                expected_value,
                f"Mismatch for bit {bit_index}")

    def test_decode_single_bit_from_multiple_holding_registers(self):
        """Selecting a bit index beyond the first register (e.g. bit 23 with objectsCount: 2)
        must reach into the second register instead of clamping to the last bit of the first
        one (the pre-fix behavior)."""
        converter = BytesModbusUplinkConverter({"deviceName": "Modbus Test", "deviceType": "default", "unitId": 1},
                                               logger=self.log)

        # register 0 = 0x0001, register 1 = 0x8000
        registers = [1, 0x8000]

        # bit 8 = LSB of register 0's low byte (0x01) -> 1
        self.assertEqual(
            converter.decode_data(registers,
                                  {'functionCode': 3, 'type': 'bits', 'objectsCount': 2,
                                   'bit': 8, 'bitTargetType': 'int'},
                                  Endian.BIG, Endian.BIG),
            1)
        # bit 9, still within register 0 -> 0
        self.assertEqual(
            converter.decode_data(registers,
                                  {'functionCode': 3, 'type': 'bits', 'objectsCount': 2,
                                   'bit': 9, 'bitTargetType': 'int'},
                                  Endian.BIG, Endian.BIG),
            0)
        # bit 16, start of register 1's high byte (0x80) -> 0
        self.assertEqual(
            converter.decode_data(registers,
                                  {'functionCode': 3, 'type': 'bits', 'objectsCount': 2,
                                   'bit': 16, 'bitTargetType': 'int'},
                                  Endian.BIG, Endian.BIG),
            0)
        # bit 23 = MSB of register 1's high byte (0x80) -> 1, only reachable when all of
        # register 1's bits are decoded instead of clamping to register 0's last bit
        self.assertEqual(
            converter.decode_data(registers,
                                  {'functionCode': 3, 'type': 'bits', 'objectsCount': 2,
                                   'bit': 23, 'bitTargetType': 'int'},
                                  Endian.BIG, Endian.BIG),
            1)


if __name__ == '__main__':
    unittest.main()
