#     Copyright 2020. ThingsBoard
#
#     Licensed under the Apache License, Version 2.0 (the "License"];
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

import struct
import unittest
from random import randint, uniform, choice
from string import ascii_lowercase

from thingsboard_gateway.connectors.can.bytes_can_downlink_converter import BytesCanDownlinkConverter


class BytesCanDownlinkConverterTests(unittest.TestCase):

    def setUp(self):
        self.converter = BytesCanDownlinkConverter()

    def test_data_in_hex_in_conf(self):
        expected_can_data = [0, 1, 2, 3]
        config = {"dataInHex": "00 01 02 03"}
        data = {}

        actual_can_data = self.converter.convert(config, data)
        self.assertListEqual(actual_can_data, expected_can_data)

    def test_data_in_hex_in_data(self):
        expected_can_data = [0, 1, 2, 3]
        config = {}
        data = {"dataInHex": "00 01 02 03"}

        actual_can_data = self.converter.convert(config, data)
        self.assertListEqual(actual_can_data, expected_can_data)

    def test_no_data(self):
        self.assertIsNone(self.converter.convert({}, {}))

    def test_wrong_data_format(self):
        self.assertIsNone(self.converter.convert({}, [1, 2, 3]))

    def test_bool_data(self):
        value = True
        expected_can_data = [int(value)]
        data = {"value": value}
        actual_can_data = self.converter.convert({}, data)
        self.assertListEqual(actual_can_data, expected_can_data)

    def test_unsigned_integer_data(self):
        for data_length in [1, 2, 3, 4]:
            # Empty byteorder value means default encoding (big)
            for byteorder in ["", "little"]:
                config = { "dataLength": data_length }
                if byteorder:
                    config["dataByteorder"] = byteorder
                else:
                    byteorder = "big"

                data = {"value": randint(0, pow(2, 8 * data_length))}

                actual_can_data = self.converter.convert(config, data)
                self.assertListEqual(actual_can_data,
                                     list(data["value"].to_bytes(data_length, byteorder, signed=False)))

    def test_signed_integer_data(self):
        for data_length in [1, 2, 3, 4]:
            # Empty byteorder value means default encoding (big)
            for byteorder in ["", "little"]:
                config = {
                    "dataLength": data_length,
                    "dataSigned": True
                }

                if byteorder:
                    config["dataByteorder"] = byteorder
                else:
                    byteorder = "big"

                data = {"value": randint(-int(pow(2, 8 * data_length) / 2),
                                         int(pow(2, 8 * data_length) / 2) - 1)}

                actual_can_data = self.converter.convert(config, data)
                self.assertListEqual(actual_can_data,
                                     list(data["value"].to_bytes(data_length, byteorder, signed=True)))

    def test_float_data(self):
        # Empty byteorder value means default encoding (big)
        for byteorder in ["", "little"]:
            data = {"value": uniform(-3.1415926535, 3.1415926535)}
            config = {}
            if byteorder:
                config["dataByteorder"] = byteorder
            else:
                byteorder = "big"
            actual_can_data = self.converter.convert(config, data)
            self.assertListEqual(actual_can_data,
                                 list(struct.pack(">f" if byteorder[0] == "b" else "<f", data["value"])))

    def test_string_data(self):
        # Empty encoding value means default encoding (ascii)
        for encoding in ["", "utf-8"]:
            value = "".join(choice(ascii_lowercase) for _ in range(8))
            data = {"value": value}
            config = {}
            if encoding:
                config["dataEncoding"] = encoding
            else:
                encoding = "ascii"
            actual_can_data = self.converter.convert(config, data)
            self.assertListEqual(actual_can_data, list(value.encode(encoding)))

    def test_expression_data(self):
        default_data_length = 1
        default_byteorder = "big"
        data = {
            "one": 1,
            "two": 2,
            "three": 3
        }
        config = {"dataExpression": "one + two + three"}
        value = 0
        for i in data.values():
            value += i
        actual_can_data = self.converter.convert(config, data)
        self.assertListEqual(actual_can_data,
                             list(value.to_bytes(default_data_length, default_byteorder)))

    def test_strict_eval_violation(self):
        data = {"value": randint(0, 256)}
        config = {
            "dataExpression": "pow(value, 2)",
            "strictEval": True
        }
        self.assertIsNone(self.converter.convert(config, data))

    def test_data_before(self):
        value = True
        expected_can_data = [0, 1, 2, 3, int(value)]
        data = {"value": value}
        config = {"dataBefore": "00 01 02 03"}
        actual_can_data = self.converter.convert(config, data)
        self.assertListEqual(actual_can_data, expected_can_data)

    def test_data_after(self):
        value = True
        expected_can_data = [int(value), 3, 2, 1, 0]
        data = {"value": value}
        config = {"dataAfter": "03 02 01 00"}
        actual_can_data = self.converter.convert(config, data)
        self.assertListEqual(actual_can_data, expected_can_data)


if __name__ == '__main__':
    unittest.main()
