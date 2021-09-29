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

import unittest

from thingsboard_gateway.connectors.mqtt.custom_object_converter import CustomObjectConverter


class CustomObjectConverterTests(unittest.TestCase):
    def test_object_convertation(self):
        config = {
            "type": "custom",
            "deviceNameJsonExpression": "Device Name",
            "deviceTypeJsonExpression": "Device Type",
            "timeout": 60000,
            "attributes": [],
            "timeseries": [
                {
                    "type": "json",
                    "key": "object",
                    "value": {"str": "here is ${str}", "int": "${int}", "double": "${double}"}
                }
            ]
        }
        data = {
            "str": "string",
            "int": 1,
            "double": 2.0
        }
        converter = CustomObjectConverter({"converter": config})
        result = converter.convert(config, data)
        expected = {
            'deviceName': 'Device Name',
            'deviceType': 'Device Type',
            'attributes': [],
            'telemetry': [{'object': {'str': 'here is string', 'int': 1, 'double': 2.0}}]
        }
        self.assertEqual(expected, result)

    def test_complex_object_convertation(self):
        config = {
            "type": "custom",
            "deviceNameJsonExpression": "Device Name",
            "deviceTypeJsonExpression": "Device Type",
            "timeout": 60000,
            "attributes": [],
            "timeseries": [
                {
                    "type": "json",
                    "key": "object",
                    "value": {
                        "complex": {"str": "${str}", "int": "${int}"},
                        "double": "${double}"
                    }
                }
            ]
        }
        data = {
            "str": "string",
            "int": 1,
            "double": 2.0
        }
        converter = CustomObjectConverter({"converter": config})
        result = converter.convert(config, data)
        expected = {
            'deviceName': 'Device Name',
            'deviceType': 'Device Type',
            'attributes': [],
            'telemetry': [{'object': {'complex': {'str': 'string', 'int': 1}, 'double': 2.0}}]
        }
        self.assertEqual(expected, result)

    def test_simple_convertation(self):
        config = {
            "type": "custom",
            "deviceNameJsonExpression": "Device Name",
            "deviceTypeJsonExpression": "Device Type",
            "timeout": 60000,
            "attributes": [],
            "timeseries": [
                {
                    "type": "string",
                    "key": "str",
                    "value": "${str}"
                }
            ]
        }
        data = {
            "str": "string",
            "int": 1,
            "double": 2.0
        }
        converter = CustomObjectConverter({"converter": config})
        result = converter.convert(config, data)
        expected = {
            'deviceName': 'Device Name',
            'deviceType': 'Device Type',
            'attributes': [],
            'telemetry': [{'str': 'string'}]
        }
        self.assertEqual(expected, result)
