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

from thingsboard_gateway.connectors.mqtt.json_mqtt_uplink_converter import JsonMqttUplinkConverter


class JsonMqttUplinkConverterTests(unittest.TestCase):

    def test_glob_matching(self):
        topic, config, data = self._get_device_1_test_data()
        converter = JsonMqttUplinkConverter(config)
        converted_data = converter.convert(topic, data)
        self.assertDictEqual(data, self._convert_to_dict(converted_data.get('telemetry')))
        self.assertDictEqual(data, self._convert_to_dict(converted_data.get('attributes')))

    def _convert_to_dict(self, data_array):
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
        topic = "Device/data"
        config = {
          "topicFilter": topic,
          "converter": {
            "type": "json",
            "deviceNameTopicExpression": "(.*?)(?=\/data)",
            "deviceTypeTopicExpression": "Boiler",
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


if __name__ == '__main__':
    unittest.main()
