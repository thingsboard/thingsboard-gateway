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

import _struct
import unittest
from math import isclose
from random import randint, uniform, choice
from string import ascii_lowercase

from thingsboard_gateway.connectors.can.bytes_can_uplink_converter import BytesCanUplinkConverter


class BytesCanUplinkConverterTests(unittest.TestCase):

    def setUp(self):
        self.converter = BytesCanUplinkConverter()

    def _has_no_data(self, data):
        return bool(data is None or not data.get("attributes", []) and not data.get("telemetry", []))

    def test_wrong_type(self):
        can_data = [0, 1, 0, 0, 0]
        configs = [{
            "key": "var",
            "is_ts": True,
            "type": "wrong_type"
        }]
        tb_data = self.converter.convert(configs, can_data)
        self.assertTrue(self._has_no_data(tb_data))

    def test_bool_true(self):
        can_data = [0, 1, 0, 0, 0]
        configs = [{
            "key": "boolVar",
            "is_ts": True,
            "type": "bool",
            "start": 1
        }]
        tb_data = self.converter.convert(configs, can_data)
        self.assertTrue(tb_data["telemetry"]["boolVar"])

    def test_bool_false(self):
        can_data = [1, 0, 1, 1, 1]
        configs = [{
            "key": "boolVar",
            "is_ts": False,
            "type": "bool",
            "start": 1
        }]
        tb_data = self.converter.convert(configs, can_data)
        self.assertFalse(tb_data["attributes"]["boolVar"])

    def _test_int(self, type, byteorder):
        int_value = randint(-32768, 32767)
        int_size = 2

        can_data = [0, 0]
        configs = [{
            "key": type + "Var",
            "is_ts": True,
            "type": type,
            "start": len(can_data),
            "length": int_size,
            "byteorder": byteorder,
            "signed": int_value < 0
        }]

        can_data.extend(int_value.to_bytes(int_size, byteorder, signed=(int_value < 0)))
        tb_data = self.converter.convert(configs, can_data)
        self.assertEqual(tb_data["telemetry"][type + "Var"], int_value)

    def test_int_big_byteorder(self):
        self._test_int("int", "big")

    def test_int_little_byteorder(self):
        self._test_int("int", "little")

    def test_long_big_byteorder(self):
        self._test_int("long", "big")

    def test_long_little_byteorder(self):
        self._test_int("long", "little")

    def _test_float_point_number(self, type, byteorder):
        float_value = uniform(-3.1415926535, 3.1415926535)

        can_data = [0, 0]
        configs = [{
            "key": type + "Var",
            "is_ts": True,
            "type": type,
            "start": len(can_data),
            "length": 4 if type[0] == "f" else 8,
            "byteorder": byteorder
        }]

        can_data.extend(_struct.pack((">" if byteorder[0] == "b" else "<") + type[0],
                                     float_value))
        tb_data = self.converter.convert(configs, can_data)
        self.assertTrue(isclose(tb_data["telemetry"][type + "Var"], float_value, rel_tol=1e-05))

    def test_float_big_byteorder(self):
        self._test_float_point_number("float", "big")

    def test_float_little_byteorder(self):
        self._test_float_point_number("float", "little")

    def test_double_big_byteorder(self):
        self._test_float_point_number("double", "big")

    def test_double_little_byteorder(self):
        self._test_float_point_number("double", "little")

    def _test_string(self, encoding="ascii"):
        str_length = randint(1, 8)
        str_value = ''.join(choice(ascii_lowercase) for _ in range(str_length))

        configs = [{
            "key": "stringVar",
            "is_ts": True,
            "type": "string",
            "start": 0,
            "length": str_length,
            "encoding": encoding
        }]

        can_data = str_value.encode(encoding)
        tb_data = self.converter.convert(configs, can_data)
        self.assertEqual(tb_data["telemetry"]["stringVar"], str_value)

    def test_string_default_ascii_encoding(self):
        self._test_string()

    def test_string_utf_8_string(self):
        self._test_string("utf-8")

    def _test_eval_int(self, number, strict_eval, expression):
        can_data = number.to_bytes(1, "big", signed=(number < 0))

        # By default the strictEval flag is True
        configs = [{
            "key": "var",
            "is_ts": True,
            "type": "int",
            "start": 0,
            "length": 1,
            "byteorder": "big",
            "signed": number < 0,
            "expression": expression,
            "strictEval": strict_eval
        }]

        return self.converter.convert(configs, can_data)

    def test_strict_eval_violation(self):
        number = randint(-128, 256)
        tb_data = self._test_eval_int(number, True, "pow(value, 2)")
        self.assertTrue(self._has_no_data(tb_data))

    def test_strict_eval(self):
        number = randint(-128, 256)
        tb_data = self._test_eval_int(number, True, "value * value")
        self.assertEqual(tb_data["telemetry"]["var"], number * number)

    def test_no_strict_eval(self):
        number = randint(-128, 256)
        tb_data = self._test_eval_int(number, False, "pow(value, 2)")
        self.assertEqual(tb_data["telemetry"]["var"], number * number)

    def test_multiple_valid_configs(self):
        bool_value = True
        int_value = randint(0, 256)
        can_data = [0, int(bool_value), int_value, 0, 0, 0]
        configs = [
            {
                "key": "boolVar",
                "type": "boolean",
                "is_ts": True,
                "start": 1
            },
            {
                "key": "intVar",
                "type": "int",
                "is_ts": False,
                "start": 2,
                "length": 4,
                "byteorder": "little",
                "signed": False
            }
        ]

        tb_data = self.converter.convert(configs, can_data)
        self.assertEqual(tb_data["telemetry"]["boolVar"], bool_value)
        self.assertEqual(tb_data["attributes"]["intVar"], int_value)

    def test_multiple_configs_one_invalid(self):
        bool_value = True
        invalid_length = 3  # Float requires 4 bytes

        can_data = [0, int(bool_value), randint(0, 256), 0, 0, 0]
        configs = [
            {
                "key": "validVar",
                "type": "boolean",
                "is_ts": True,
                "start": 1
            },
            {
                "key": "invalidVar",
                "type": "float",
                "is_ts": False,
                "start": 2,
                "length": invalid_length
            }
        ]

        tb_data = self.converter.convert(configs, can_data)
        self.assertEqual(tb_data["telemetry"]["validVar"], bool_value)
        self.assertIsNone(tb_data["attributes"].get("invalidVar"))


if __name__ == '__main__':
    unittest.main()
