import unittest
from time import time
from sys import path
import os
from simplejson import loads
path.insert(0, '../../')
from thingsboard_gateway.new_storage.storage_handler import StorageHandler

class TestStorageHandler(unittest.TestCase):
    
    def setUp(self):

        config = {
            "data_file_path": "./testing.db",
            "max_read_records_count": 100,
            "max_days_to_store_data": 1
        }

        self.sh = StorageHandler(config)

    def test_get_connected_devices(self):

        devices_add = {
                "testDevice": "Mqtt",
                "testDevice2": "REST"
            }

        for d in devices_add:
            print(d)
            self.sh.add_device(d, devices_add[d])

        devices = self.sh.connected_device_names

        print(devices)


if __name__ == "__main__":
    unittest.main()
