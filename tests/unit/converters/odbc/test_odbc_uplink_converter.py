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
        data = {"deviceName": "testDevice",
                "deviceType": "testType",
                "attributes": {"key": "value"},
                "telemetry": [{"value": 42, "ts": 1234567890}]}
        converted_data = self.converter.convert("*", data)

        self.assertEqual(converted_data.device_name, data["deviceName"])
        self.assertEqual(converted_data.device_type, data["deviceType"])
        self.assertDictEqual(converted_data.to_dict().get('attributes'), data["attributes"])
        self.assertDictEqual(converted_data.telemetry[0].values, data["telemetry"][0])

    def test_data_subset(self):
        config = {
            "attributes": "*",
            "timeseries": [
              {
                "name": "value1",
                "value": "floatValue"
              },
              {
                "name": "value2",
                "value": "boolValue"
              }
            ]
          }
        converted_data = self.converter.convert(config, self.db_data)
        expected_telemetry_keys = ["value1", "value2"]
        expected_attributes_keys = ["boolValue", "intValue", "floatValue", "stringValue"]
        for attribute in converted_data.attributes:
            self.assertIn(attribute.key, expected_attributes_keys)
        for telemetry_entry in converted_data.telemetry:
            for telemetry in telemetry_entry.values.keys():
                self.assertIn(telemetry.key, expected_telemetry_keys)

    def test_alias(self):
        config = {
            "attributes": "*",
            "timeseries": [
                {
                    "name": "valueOfString",
                    "column": "stringValue"
                }
            ]
        }
        converted_data = self.converter.convert(config, self.db_data)
        self.assertEqual(list(converted_data.telemetry[0].values.keys())[0].key, config["timeseries"][0]["name"])

    def test_name_expression(self):
        config = {
            "attributes": [{
                "nameExpression": "key",
                "value": "intValue"
            }],
            "timeseries": []
        }
        attr_name = "someAttribute"
        self.db_data["key"] = attr_name
        converted_data = self.converter.convert(config, self.db_data)
        self.assertEqual(list(converted_data.attributes.values.keys())[0].key, attr_name)

    def test_value_config(self):
        config = {
            "attributes": [],
            "timeseries": [
                {
                    "name": "someValue",
                    "value": "stringValue + str(intValue)"
                }
            ]
        }
        converted_data = self.converter.convert(config, self.db_data)
        self.assertEqual(list(converted_data.telemetry[0].values.values())[0], self.db_data["stringValue"] + str(self.db_data["intValue"]))

    def test_one_valid_one_invalid_configs(self):
        config = {
            "attributes": [{
                "name": "key",
                "column": "unkownColumnValue"
            },
            {
                "name": "str",
                "value": "stringValue"
            }],
            "timeseries": []
        }
        with self.assertLogs(level="ERROR") as log:
            converted_data = self.converter.convert(config, self.db_data)
            self.assertTrue(log.output, "Failed to convert SQL data to TB format: 'unkownColumnValue'")
        self.assertEqual(len(converted_data.attributes.values), 1)
        self.assertEqual(list(converted_data.attributes.values.keys())[0].key, config["attributes"][1]["name"])


if __name__ == '__main__':
    unittest.main()
