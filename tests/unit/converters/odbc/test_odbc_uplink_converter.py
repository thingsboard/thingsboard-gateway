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

from sys import path
path.append('..')

import unittest
from random import randint, uniform, choice
from string import ascii_lowercase

from tests.unit.BaseUnitTest import BaseUnitTest
from thingsboard_gateway.connectors.odbc.odbc_uplink_converter import OdbcUplinkConverter


class OdbcUplinkConverterTests(BaseUnitTest):

    def setUp(self):
        self.converter = OdbcUplinkConverter(logger=self.log)
        self.db_data = {"boolValue": True,
                        "intValue": randint(0, 256),
                        "floatValue": uniform(-3.1415926535, 3.1415926535),
                        "stringValue": "".join(choice(ascii_lowercase) for _ in range(8))}

    def test_glob_matching(self):
        converted_data = self.converter.convert("*", self.db_data)
        self.assertDictEqual(converted_data, self.db_data)

    def test_data_subset(self):
        config = ["floatValue", "boolValue"]
        converted_data = self.converter.convert(config, self.db_data)
        expected_data = {}
        for key in config:
            expected_data[key] = self.db_data[key]
        self.assertDictEqual(converted_data, expected_data)

    def test_alias(self):
        config = [{"column": "stringValue", "name": "valueOfString"}]
        converted_data = self.converter.convert(config, self.db_data)
        self.assertDictEqual(converted_data, {config[0]["name"]: self.db_data[config[0]["column"]]})

    def test_name_expression(self):
        attr_name = "someAttribute"
        config = [{"nameExpression": "key", "value": "intValue"}]
        self.db_data["key"] = attr_name
        converted_data = self.converter.convert(config, self.db_data)
        self.assertDictEqual(converted_data, {attr_name: self.db_data[config[0]["value"]]})

    def test_value_config(self):
        config = [{"name": "someValue", "value": "stringValue + str(intValue)"}]
        converted_data = self.converter.convert(config, self.db_data)
        self.assertDictEqual(converted_data, {config[0]["name"]: self.db_data["stringValue"] + str(self.db_data["intValue"])})

    def test_one_valid_one_invalid_configs(self):
        config = ["unkownColumnValue", "stringValue"]
        with self.assertLogs(level="ERROR") as log:
            converted_data = self.converter.convert(config, self.db_data)
            self.assertTrue(log.output, "Failed to convert SQL data to TB format: 'unkownColumnValue'")
        self.assertDictEqual(converted_data, {config[1]: self.db_data[config[1]]})


if __name__ == '__main__':
    unittest.main()
