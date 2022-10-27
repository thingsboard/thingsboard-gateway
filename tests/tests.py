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

import logging
import unittest
from os import remove, listdir, removedirs
from time import sleep
from random import randint

from thingsboard_gateway.connectors.mqtt.json_mqtt_uplink_converter import JsonMqttUplinkConverter
from thingsboard_gateway.connectors.opcua.opcua_uplink_converter import OpcUaUplinkConverter
from thingsboard_gateway.connectors.ble.bytes_ble_uplink_converter import BytesBLEUplinkConverter
from thingsboard_gateway.connectors.request.json_request_uplink_converter import JsonRequestUplinkConverter
from thingsboard_gateway.storage.memory.memory_event_storage import MemoryEventStorage
from thingsboard_gateway.storage.file.file_event_storage import FileEventStorage

logging.basicConfig(level=logging.ERROR,
                    format='%(asctime)s - %(levelname)s - %(module)s - %(lineno)d - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')


class ConvertersTests(unittest.TestCase):
    def test_mqtt_getting_values(self):
        test_mqtt_config = {
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
        test_mqtt_body_to_convert = {
            "sensorName": "SensorA",
            "sensorType": "temperature-sensor",
            "model": "T1000",
            "t": 42.0
        }
        test_mqtt_convert_config = "sensor/temperature/SensorA"
        test_mqtt_result = {
            "deviceName": "SensorA",
            "deviceType": "temperature-sensor",
            "attributes": [{"model": "T1000"}],
            "telemetry": [{"temperature": '42.0'}]
        }

        converter = JsonMqttUplinkConverter(test_mqtt_config)
        result = converter.convert(test_mqtt_convert_config, test_mqtt_body_to_convert)
        self.assertDictEqual(result, test_mqtt_result)

    def test_opcua_getting_values(self):
        test_opcua_config = {'deviceNodePattern': 'Root\\.Objects\\.Device1',
                             'deviceNamePattern': 'Device ${Root\\.Objects\\.Device1\\.serialNumber}',
                             'attributes': [{'key': 'temperature °C', 'path': '${ns=2;i=5}'}],
                             'timeseries': [{'key': 'humidity',
                                             'path': '${Root\\.Objects\\.Device1\\.TemperatureAndHumiditySensor\\.Humidity}'},
                                            {'key': 'batteryLevel', 'path': '${Battery\\.batteryLevel}'}],
                             'deviceName': 'Device Number One',
                             'deviceType': 'default'}
        test_data_list = [24.1, 25.8, 59.8]
        test_configs = [('ns=2;i=5', 'ns=2;i=5'),
                        ('Root\\.Objects\\.Device1\\.TemperatureAndHumiditySensor\\.Humidity',
                         'Root\\.Objects\\.Device1\\.TemperatureAndHumiditySensor\\.Humidity'),
                        ('Battery\\.batteryLevel', 'Root\\\\.Objects\\\\.Device1\\\\.Battery\\\\.batteryLevel')]
        test_opcua_result = [
            {'deviceName': 'Device Number One', 'deviceType': 'default', 'attributes': [{'temperature °C': '24.1'}],
             'telemetry': []},
            {'deviceName': 'Device Number One', 'deviceType': 'default', 'attributes': [],
             'telemetry': [{'humidity': '25.8'}]},
            {'deviceName': 'Device Number One', 'deviceType': 'default', 'attributes': [],
             'telemetry': [{'batteryLevel': '59.8'}]}]

        converter = OpcUaUplinkConverter(test_opcua_config)
        result = []
        for index, config in enumerate(test_configs):
            result.append(converter.convert(config, test_data_list[index]))
        self.assertListEqual(result, test_opcua_result)

    def test_ble_getting_values(self):
        test_ble_config = {
            "deviceName": "Temperature and humidity sensor",
            "deviceType": "BLEDevice",
            "MACAddress": "4C:65:A8:DF:85:C0",
            "telemetry": [
                {
                    "key": "temperature",
                    "method": "notify",
                    "characteristicUUID": "226CAA55-6476-4566-7562-66734470666D",
                    "byteFrom": 2,
                    "byteTo": 6
                },
                {
                    "key": "humidity",
                    "method": "notify",
                    "characteristicUUID": "226CAA55-6476-4566-7562-66734470666D",
                    "byteFrom": 9,
                    "byteTo": 13
                }
            ],
            "attributes": [
                {
                    "key": "name",
                    "characteristicUUID": "00002A00-0000-1000-8000-00805F9B34FB",
                    "method": "read",
                    "byteFrom": 0,
                    "byteTo": -1
                }
            ]
        }
        test_data_list = [b'T=54.7 H=37.0', b'T=54.7 H=37.0', b'Some string']
        test_configs = [
                        {"section_config": {"key": "temperature",
                                            "byteFrom": 2,
                                            "byteTo": 6},
                         "type": "telemetry",
                         "clean": False},
                        {"section_config": {"key": "humidity",
                                            "byteFrom": 9,
                                            "byteTo": 13},
                         "type": "telemetry",
                         "clean": False},
                        {"section_config": {"key": "name",
                                            "byteFrom": 0,
                                            "byteTo": -1},
                         "type": "attributes",
                         "clean": False}
                        ]
        test_result = {'deviceName': 'Temperature and humidity sensor',
                       'deviceType': 'BLEDevice',
                       'telemetry': [
                           {'temperature': '54.7'},
                           {'humidity': '37.0'}
                       ],
                       'attributes': [
                           {'name': 'Some string'}
                       ]}

        result = {}

        converter = BytesBLEUplinkConverter(test_ble_config)
        for index, config in enumerate(test_configs):
            result = converter.convert(config, test_data_list[index])
        self.assertDictEqual(result, test_result)

    def test_request_getting_values(self):
        test_request_config = {
          "url": "/last",
          "httpMethod": "GET",
          "deviceNameJsonExpression": "${$.sensor}",
          "deviceTypeJsonExpression": "default",
          "httpHeaders": {
            "ACCEPT": "application/json"
          },
          "allowRedirects": True,
          "timeout": 0.5,
          "scanPeriod": 5,
          "converter": {
            "type": "json",
            "attributes": [
            ],
            "telemetry": [
              {

                "key": "${$.name}",
                "type": "int",
                "value": "${$.value}"
              }
            ]
          }
        }
        test_request_body_to_convert = {"name": "Humidity",
                                        "sensor": "aranet:358151000412:100886",
                                        "time": "2020-03-17T16:16:03Z",
                                        "unit": "%RH",
                                        "value": "66"}

        test_request_convert_config = "127.0.0.1:5000/last"
        test_request_result = {
            "deviceName": "aranet:358151000412:100886",
            "deviceType": "default",
            "attributes": [],
            "telemetry": [{"Humidity": '66'}]
        }

        converter = JsonRequestUplinkConverter(test_request_config)
        result = converter.convert(test_request_convert_config, test_request_body_to_convert)
        self.assertDictEqual(result, test_request_result)


class TestStorage(unittest.TestCase):
    def test_memory_storage(self):

        test_size = randint(0, 100)

        storage_test_config = {
            "type": "memory",
            "read_records_count": 10,
            "max_records_count": test_size * 10
        }
        storage = MemoryEventStorage(storage_test_config)

        for test_value in range(test_size * 10):
            storage.put(test_value)

        result = []
        for _ in range(test_size):
            result.append(storage.get_event_pack())
            storage.event_pack_processing_done()
        correct_result = [[item for item in range(pack * 10, (pack + 1) * 10)] for pack in range(test_size)]

        self.assertListEqual(result, correct_result)

    def test_file_storage(self):

        storage_test_config = {"data_folder_path": "storage/data/",
                               "max_file_count": 1000,
                               "max_records_per_file": 10,
                               "max_read_records_count": 10,
                               "no_records_sleep_interval": 5000
                               }

        test_size = randint(0, storage_test_config["max_file_count"]-1)

        storage = FileEventStorage(storage_test_config)

        for test_value in range(test_size * 10):
            storage.put(str(test_value))
            sleep(.01)

        result = []
        for _ in range(test_size):
            batch = storage.get_event_pack()
            result.append(batch)
            storage.event_pack_processing_done()

        correct_result = [[str(x) for x in range(y * 10, (y + 1) * 10)] for y in range(test_size)]

        print(result)
        print(correct_result)
        for file in listdir(storage_test_config["data_folder_path"]):
            remove(storage_test_config["data_folder_path"]+"/"+file)
        removedirs(storage_test_config["data_folder_path"])
        self.assertListEqual(result, correct_result)


if __name__ == '__main__':
    unittest.main()
