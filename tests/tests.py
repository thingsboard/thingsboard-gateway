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

LOG = logging.getLogger("TEST")


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
        expected_result = {
            "deviceName": "SensorA",
            "deviceType": "temperature-sensor",
            "attributes": [{"model": "T1000"}],
            "telemetry": [{"temperature": 42.0}]
        }

        converter = JsonMqttUplinkConverter(test_mqtt_config, LOG)
        result = converter.convert(test_mqtt_convert_config, test_mqtt_body_to_convert)
        self.assertDictEqual(expected_result, result)

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
        expected_result = [
            {'deviceName': 'Device Number One', 'deviceType': 'default', 'attributes': [{'temperature °C': '24.1'}],
             'telemetry': []},
            {'deviceName': 'Device Number One', 'deviceType': 'default', 'attributes': [],
             'telemetry': [{'humidity': '25.8'}]},
            {'deviceName': 'Device Number One', 'deviceType': 'default', 'attributes': [],
             'telemetry': [{'batteryLevel': '59.8'}]}]

        converter = OpcUaUplinkConverter(test_opcua_config, LOG)
        result = []
        for index, config in enumerate(test_configs):
            result.append(converter.convert(config, test_data_list[index]))
        self.assertListEqual(expected_result, result)


if __name__ == '__main__':
    unittest.main()
