from os import listdir, remove, removedirs
from random import randint
from time import sleep
from unittest import TestCase

from thingsboard_gateway.storage.file.file_event_storage import FileEventStorage
from thingsboard_gateway.storage.memory.memory_event_storage import MemoryEventStorage


class TestStorage(TestCase):
    def test_memory_storage(self):

        test_size = 20

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
                               "max_file_count": 20,
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
