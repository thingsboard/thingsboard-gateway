from logging import getLogger
from os import listdir, remove, removedirs
from random import randint
from threading import Event
from time import sleep
from unittest import TestCase

from thingsboard_gateway.storage.file.file_event_storage import FileEventStorage
from thingsboard_gateway.storage.memory.memory_event_storage import MemoryEventStorage
from thingsboard_gateway.storage.sqlite.sqlite_event_storage import SQLiteEventStorage

LOG = getLogger("TEST")


class TestStorage(TestCase):
    def test_memory_storage(self):

        test_size = 20

        storage_test_config = {
            "type": "memory",
            "read_records_count": 10,
            "max_records_count": test_size * 10
        }

        stop_event = Event()

        storage = MemoryEventStorage(storage_test_config, LOG, stop_event)

        for test_value in range(test_size * 10):
            storage.put(test_value)

        result = []
        for _ in range(test_size):
            result.append(storage.get_event_pack())
            storage.event_pack_processing_done()
        correct_result = [[item for item in range(pack * 10, (pack + 1) * 10)] for pack in range(test_size)]

        self.assertListEqual(result, correct_result)

        stop_event.set()

    def test_file_storage(self):

        storage_test_config = {"data_folder_path": "storage/data/",
                               "max_file_count": 20,
                               "max_records_per_file": 10,
                               "max_read_records_count": 10,
                               "no_records_sleep_interval": 5000
                               }

        test_size = randint(0, storage_test_config["max_file_count"]-1)

        stop_event = Event()

        storage = FileEventStorage(storage_test_config, LOG, stop_event)

        for test_value in range(test_size * 10):
            storage.put(str(test_value))
            sleep(.001)

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

        stop_event.set()

    def test_sqlite_storage(self):
        storage_test_config = {
            "data_file_path": "storage/data/data.db",
            "messages_ttl_check_in_hours": 1,
            "messages_ttl_in_days": 7,
            "max_read_records_count": 70
        }

        stop_event = Event()

        storage = SQLiteEventStorage(storage_test_config, LOG, stop_event)
        test_size = 20
        expected_result = []
        save_results = []

        for test_value_int in range(test_size * 10):
            test_value = str(test_value_int)
            expected_result.append(test_value)
            save_result = storage.put(test_value)
            save_results.append(save_result)
            sleep(.01)
        sleep(1)

        self.assertTrue(all(save_results))

        result = []
        for _ in range(test_size):
            batch = storage.get_event_pack()
            result.append(batch)
            storage.event_pack_processing_done()

        unpacked_result = []
        for batch in result:
            for item in batch:
                unpacked_result.append(item)

        remove(storage_test_config["data_file_path"])
        self.assertListEqual(unpacked_result, expected_result)

        stop_event.set()
