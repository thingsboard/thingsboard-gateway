#     Copyright 2022. ThingsBoard
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
from random import randint

from tests.unit.BaseUnitTest import BaseUnitTest
from thingsboard_gateway.gateway.constants import *
from thingsboard_gateway.connectors.mqtt.json_mqtt_uplink_converter import JsonMqttUplinkConverter


class JsonMqttUplinkConverterTests(BaseUnitTest):
    DEVICE_NAME = "TestDevice"
    DEVICE_TYPE = "TestDeviceType"

    def test_topic_name_and_type(self):
        topic, config, data = self._get_device_1_test_data()
        converter = JsonMqttUplinkConverter(config, logger=self.log)
        converted_data = converter.convert(topic, data)

        self.assertEqual(self.DEVICE_NAME, converted_data["deviceName"])
        self.assertEqual(self.DEVICE_TYPE, converted_data["deviceType"])

    def test_json_name_and_type(self):
        topic, config, data = self._get_device_2_test_data()
        converter = JsonMqttUplinkConverter(config, logger=self.log)
        converted_data = converter.convert(topic, data)

        self.assertEqual(self.DEVICE_NAME, converted_data["deviceName"])
        self.assertEqual(self.DEVICE_TYPE, converted_data["deviceType"])

    def test_glob_matching(self):
        topic, config, data = self._get_device_1_test_data()
        converter = JsonMqttUplinkConverter(config, logger=self.log)
        converted_data = converter.convert(topic, data)
        self.assertDictEqual(data, self._convert_to_dict(converted_data.get('telemetry')))
        self.assertDictEqual(data, self._convert_to_dict(converted_data.get('attributes')))

    def test_array_result(self):
        topic, config, single_data = self._get_device_1_test_data()
        array_data = [single_data, single_data]
        converter = JsonMqttUplinkConverter(config, logger=self.log)
        converted_array_data = converter.convert(topic, array_data)

        self.assertTrue(isinstance(converted_array_data, list))
        for item in converted_array_data:
            self.assertDictEqual(single_data, self._convert_to_dict(item.get('telemetry')))
            self.assertDictEqual(single_data, self._convert_to_dict(item.get('attributes')))

    def test_without_send_on_change_option(self):
        topic, config, data = self._get_device_1_test_data()
        converter = JsonMqttUplinkConverter(config, logger=self.log)
        converted_array_data = converter.convert(topic, data)
        self.assertIsNone(converted_array_data.get(SEND_ON_CHANGE_PARAMETER))

    def test_with_send_on_change_option_disabled(self):
        topic, config, data = self._get_device_1_test_data()
        config["converter"][SEND_ON_CHANGE_PARAMETER] = False
        converter = JsonMqttUplinkConverter(config, logger=self.log)
        converted_array_data = converter.convert(topic, data)
        self.assertFalse(converted_array_data.get(SEND_ON_CHANGE_PARAMETER))

    def test_with_send_on_change_option_enabled(self):
        topic, config, data = self._get_device_1_test_data()
        config["converter"][SEND_ON_CHANGE_PARAMETER] = True
        converter = JsonMqttUplinkConverter(config, logger=self.log)
        converted_array_data = converter.convert(topic, data)
        self.assertTrue(converted_array_data.get(SEND_ON_CHANGE_PARAMETER))

    def test_parse_device_name_from_spaced_key_name(self):
        device_key_name = "device name"

        topic, config, data = self._get_device_test_data_with_spaced_key_and_different_out_type(device_key_name)
        converter = JsonMqttUplinkConverter(config, logger=self.log)
        converted_data = converter.convert(topic, data)

        self.assertEqual(data[device_key_name], converted_data["deviceName"])

    def test_convert_data_from_string_to_int_without_eval(self):
        use_eval = False
        device_key_name = "device name"
        attr_key_name = "test_key"

        topic, config, data = self._get_device_test_data_with_spaced_key_and_different_out_type(
                                                                device_key_name, attr_key_name, use_eval)
        converter = JsonMqttUplinkConverter(config, logger=self.log)
        converted_data = converter.convert(topic, data)

        self.assertEqual(converted_data[ATTRIBUTES_PARAMETER][0][attr_key_name], int(float(data[attr_key_name])))

    def test_convert_data_from_string_to_int_with_eval(self):
        use_eval = True
        device_key_name = "device name"
        attr_key_name = "test_key"

        topic, config, data = self._get_device_test_data_with_spaced_key_and_different_out_type(
                                                                device_key_name, attr_key_name, use_eval)
        converter = JsonMqttUplinkConverter(config, logger=self.log)
        converted_data = converter.convert(topic, data)

        self.assertEqual(converted_data[ATTRIBUTES_PARAMETER][0][attr_key_name], 2 * int(float(data[attr_key_name])))

    @staticmethod
    def _convert_to_dict(data_array):
        data_dict = {}
        for item_container in data_array:
            item_data = item_container
            if item_data.get("ts") is not None:
                data_dict["ts"] = item_data.get("ts")
                item_data = item_container.get("values")
            for key in item_data:
                data_dict[key] = item_data[key]
        return data_dict

    def _get_device_1_test_data(self):
        topic = f"{self.DEVICE_NAME}/{self.DEVICE_TYPE}"
        config = {
          "topicFilter": topic,
          "converter": {
            "type": "json",
            "deviceInfo": {
                "deviceNameExpressionSource": "topic",
                "deviceNameExpression": "(.*?)(?=/.*)",
                "deviceProfileExpressionSource": "topic",
                "deviceProfileExpression": f"(?<={self.DEVICE_NAME}/)(.*)"
            },
            "timeout": 60000,
            "attributes": "*",
            "timeseries": "*"
          }
        }
        data = {
            "Temperature": randint(0, 256),
            "Pressure": randint(0, 256)
        }
        return topic, config, data

    def _get_device_2_test_data(self):
        topic = f"{self.DEVICE_NAME}/{self.DEVICE_TYPE}"
        config = {
          "topicFilter": topic,
          "converter": {
            "type": "json",
            "deviceInfo": {
                "deviceNameExpressionSource": "message",
                "deviceNameExpression": "${DeviceName}",
                "deviceProfileExpressionSource": "message",
                "deviceProfileExpression": "${DeviceType}"
            },
            "timeout": 60000,
            "attributes": "*",
            "timeseries": "*"
          }
        }
        data = {
            "DeviceName": self.DEVICE_NAME,
            "DeviceType": self.DEVICE_TYPE
        }
        return topic, config, data

    def _get_device_test_data_with_spaced_key_and_different_out_type(self, device_name_key, attr_key_name="test_key", use_eval=False):
        topic = "topic"
        value_expression = "${" + attr_key_name + "}"
        config = {
            "topicFilter": topic,
            "converter": {
                "type": "json",
                "useEval": use_eval,
                "deviceInfo": {
                    "deviceNameExpressionSource": "message",
                    "deviceNameExpression": "${" + device_name_key + "}",
                    "deviceProfileExpressionSource": "message",
                    "deviceProfileExpression": self.DEVICE_TYPE
                },
                "attributes": [
                    {
                        "type": "int",
                        "key": attr_key_name,
                        "value": f"{value_expression} + {value_expression}" if use_eval else value_expression
                    }
                ],
                "timeseries": []
            }
        }
        data = {
          device_name_key: self.DEVICE_NAME,
          attr_key_name: "21.420000"
        }
        return topic, config, data


if __name__ == '__main__':
    unittest.main()
