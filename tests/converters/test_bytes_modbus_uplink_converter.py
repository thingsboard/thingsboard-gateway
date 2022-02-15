import unittest

from pymodbus.constants import Endian
from pymodbus.payload import BinaryPayloadBuilder

from thingsboard_gateway.connectors.modbus.bytes_modbus_uplink_converter import BytesModbusUplinkConverter


class ModbusConverterTests(unittest.TestCase):
    def test_modbus_getting_values(self):
        test_modbus_config = {
            "attributes": [
                {"string": {
                    "byteOrder": "BIG",
                    "tag": "string",
                    "type": "string",
                    "functionCode": 4,
                    "registerCount": 4
                }},
                {"bits": {
                    "byteOrder": "BIG",
                    "tag": "bits",
                    "type": "bits",
                    "functionCode": 4,
                    "registerCount": 1
                }},
                {"8int": {
                    "byteOrder": "BIG",
                    "tag": "8int",
                    "type": "8int",
                    "functionCode": 4,
                    "registerCount": 1
                }},
                {"16int": {
                    "byteOrder": "BIG",
                    "tag": "16int",
                    "type": "16int",
                    "functionCode": 4,
                    "registerCount": 1
                }},
                {"long": {
                    "byteOrder": "BIG",
                    "tag": "long",
                    "type": "long",
                    "functionCode": 4,
                    "registerCount": 1
                }},
                {"long_with_divider": {
                    "byteOrder": "BIG",
                    "tag": "long",
                    "type": "long",
                    "functionCode": 4,
                    "registerCount": 1,
                    "divider": 10
                }},
                {"32int": {
                    "byteOrder": "BIG",
                    "tag": "32int",
                    "type": "32int",
                    "functionCode": 4,
                    "registerCount": 2
                }},
                {"64int": {
                    "byteOrder": "BIG",
                    "tag": "64int",
                    "type": "64int",
                    "functionCode": 4,
                    "registerCount": 4
                }},
            ],
            "timeseries": [
                {"8uint": {
                    "byteOrder": "BIG",
                    "tag": "8uint",
                    "type": "8uint",
                    "functionCode": 4,
                    "registerCount": 1}},
                {"16uint": {
                    "byteOrder": "BIG",
                    "tag": "16uint",
                    "type": "16uint",
                    "functionCode": 4,
                    "registerCount": 2}},
                {"32uint": {
                    "byteOrder": "BIG",
                    "tag": "32uint",
                    "type": "32uint",
                    "functionCode": 4,
                    "registerCount": 4}},
                {"64uint": {
                    "byteOrder": "BIG",
                    "tag": "64uint",
                    "type": "64uint",
                    "functionCode": 4,
                    "registerCount": 1}},
                {"double": {
                    "byteOrder": "BIG",
                    "tag": "double",
                    "type": "double",
                    "functionCode": 4,
                    "registerCount": 2}},
                {"16float": {
                    "byteOrder": "BIG",
                    "tag": "16float",
                    "type": "16float",
                    "functionCode": 4,
                    "registerCount": 1}},
                {"32float": {
                    "byteOrder": "BIG",
                    "tag": "32float",
                    "type": "32float",
                    "functionCode": 4,
                    "registerCount": 2}},
                {"64float": {
                    "byteOrder": "BIG",
                    "tag": "64float",
                    "type": "64float",
                    "functionCode": 4,
                    "registerCount": 4}},
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
                                  {'bits': [False, True, False, True, True, False, True, False]},
                                  {'8int': -18},
                                  {'16int': -22136},
                                  {'long': -22136},
                                  {'long_with_divider': -2213.6},
                                  {'32int': -4660},
                                  {'64int': -3735928559}]
                              }

        builder = BinaryPayloadBuilder(byteorder=Endian.Big)
        builder_registers = {"string": (builder.add_string, 'abcdefgh'),
                             "bits": (builder.add_bits, [0, 1, 0, 1, 1, 0, 1, 0]),
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

        converter = BytesModbusUplinkConverter({"deviceName": "Modbus Test", "deviceType": "default", "unitId": 1})
        result = converter.convert(test_modbus_convert_config, test_modbus_body_to_convert)
        self.assertDictEqual(result, test_modbus_result)


if __name__ == '__main__':
    unittest.main()