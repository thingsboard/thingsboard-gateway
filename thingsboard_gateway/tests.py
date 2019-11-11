#     Copyright 2019. ThingsBoard
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

import unittest
from thingsboard_gateway.connectors.mqtt.json_mqtt_uplink_converter import JsonMqttUplinkConverter
from thingsboard_gateway.storage.memory_event_storage import MemoryEventStorage
from thingsboard_gateway.storage.file_event_storage import FileEventStorage
from random import randint
import logging
import os
import base64
import threading

logging.basicConfig(level=logging.ERROR,
                    format='%(asctime)s - %(levelname)s - %(module)s - %(lineno)d - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')


class MqttConverterTest(unittest.TestCase):
    def test_getting_attributes(self):
        test_config = {
                        "converter": {
                            "type": "json",
                            "deviceNameJsonExpression": "${$.sensorName}",
                            "deviceTypeJsonExpression": "${$.sensorType}",
                            "filterExpression": "",
                            "timeout": 60000,
                            "attributes": [
                                {
                                    "key": "model",
                                    "type": "string",
                                    "value": "${$.model}"
                                }
                            ],
                            "timeseries": [
                                {
                                    "key": "temperature",
                                    "type": "double",
                                    "value": "${$.t}"
                                }
                            ]
                            }
                        }
        test_body_to_convert = {
               "sensorName": "SensorA",
               "sensorType": "temperature-sensor",
               "model": "T1000",
               "t": 42.0
               }
        test_topic = "sensor/temperature/SensorA"
        test_result = {
               "deviceName": "SensorA",
               "deviceType": "temperature-sensor",
               "attributes": [{"model":"T1000"}],
               "telemetry": [{"temperature": 42.0}]
        }
        result = JsonMqttUplinkConverter(test_config).convert(test_topic, test_body_to_convert)
        self.assertDictEqual(result,test_result)


class TestStorage(unittest.TestCase):
    def test_memory_storage(self):

        test_size = randint(0, 100)

        storage_test_config = {
            "type": "memory",
            "read_records_count": 10,
            "max_records_count": test_size*10
        }
        storage = MemoryEventStorage(storage_test_config)

        for test_value in range(test_size*10):
            storage.put(test_value)

        result = []
        for _ in range(test_size):
            result.append(storage.get_event_pack())
            storage.event_pack_processing_done()

        correct_result = [[x for x in range(y*10,(y+1)*10)] for y in range(test_size)]

        self.assertListEqual(result, correct_result)

    def test_file_storage(self):

        test_size = randint(0, 100)

        storage_test_config = {
             "data_folder_path": "storage/data/",
             "max_files_count": 100,
             "max_records_per_file": 10,
             "max_read_records_count": 10,
             "no_records_sleep_interval": 5000
        }
        storage = FileEventStorage('storage', storage_test_config)

        for test_value in range(test_size * 10):
            storage.put(str(test_value))

        result = []
        for _ in range(test_size):
            batch = storage.get_event_pack()
            result.append(batch)
            storage.event_pack_processing_done()

        correct_result = [[str(x) for x in range(y*10, (y+1)*10)] for y in range(test_size)]

        print(result)
        print(correct_result)
        self.assertListEqual(result, correct_result)


if __name__ == '__main__':
    unittest.main()
