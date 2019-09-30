import unittest
from connectors.mqtt.json_mqtt_uplink_converter import JsonMqttUplinkConverter
from storage.memory_event_storage import MemoryEventStorage
from storage.file_event_storage import FileEventStorage
from random import randint
import logging

logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s - %(levelname)s - %(module)s - %(lineno)d - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')


class MqttConnectorTest(unittest.TestCase):
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
        test_result = {
               "deviceName": "SensorA",
               "deviceType": "temperature-sensor",
               "attributes": [{"model":"T1000"}],
               "telemetry": [{"temperature": 42.0}]
        }
        result = JsonMqttUplinkConverter(test_config).convert(test_body_to_convert)
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
             "max_records_count": test_size*10,
             "read_records_count": 10,
             "records_per_file": 40
        }
        storage = FileEventStorage(storage_test_config)

        for test_value in range(test_size*10):
            storage.put(str(test_value))

        result = []
        for _ in range(test_size):
            result.append(storage.get_event_pack())
            storage.event_pack_processing_done()

        correct_result = [[str(x) for x in range(y*10,(y+1)*10)] for y in range(test_size)]

        self.assertListEqual(result, correct_result)


if __name__ == '__main__':
    unittest.main()
