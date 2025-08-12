#     Copyright 2025. ThingsBoard
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

from logging import getLogger
from os import listdir, remove, removedirs, path
from random import randint
from shutil import rmtree
from threading import Event
from time import sleep
from unittest import TestCase

from thingsboard_gateway.storage.file.file_event_storage import FileEventStorage
from thingsboard_gateway.storage.memory.memory_event_storage import MemoryEventStorage
from thingsboard_gateway.storage.sqlite.sqlite_event_storage import SQLiteEventStorage
from thingsboard_gateway.storage.sqlite.storage_settings import StorageSettings

LOG = getLogger("TEST")
LOG.trace = LOG.debug


class TestStorage(TestCase):
    def test_memory_storage(self):

        test_size = 20

        storage_test_config = {
            "type": "memory",
            "read_records_count": 10,
            "max_records_count": test_size * 10,
        }

        stop_event = Event()

        storage = MemoryEventStorage(storage_test_config, LOG, stop_event)

        for test_value in range(test_size * 10):
            storage.put(test_value)

        result = []
        for _ in range(test_size):
            result.append(storage.get_event_pack())
            storage.event_pack_processing_done()
        correct_result = [
            [item for item in range(pack * 10, (pack + 1) * 10)]
            for pack in range(test_size)
        ]

        self.assertListEqual(result, correct_result)

        stop_event.set()

    def test_file_storage(self):

        storage_test_config = {
            "data_folder_path": "storage/data/",
            "max_file_count": 20,
            "max_records_per_file": 10,
            "max_read_records_count": 10,
            "no_records_sleep_interval": 5000,
        }

        test_size = randint(0, storage_test_config["max_file_count"] - 1)

        stop_event = Event()

        storage = FileEventStorage(storage_test_config, LOG, stop_event)

        for test_value in range(test_size * 10):
            storage.put(str(test_value))
            sleep(0.001)

        result = []
        for _ in range(test_size):
            batch = storage.get_event_pack()
            result.append(batch)
            storage.event_pack_processing_done()

        correct_result = [
            [str(x) for x in range(y * 10, (y + 1) * 10)] for y in range(test_size)
        ]

        print(result)
        print(correct_result)
        for file in listdir(storage_test_config["data_folder_path"]):
            remove(storage_test_config["data_folder_path"] + "/" + file)
        removedirs(storage_test_config["data_folder_path"])
        self.assertListEqual(result, correct_result)

        stop_event.set()

    def test_sqlite_storage(self):
        storage_test_config = {
            "data_file_path": "storage/data/data.db",
            "messages_ttl_check_in_hours": 1,
            "messages_ttl_in_days": 7,
            "max_read_records_count": 70,
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
            sleep(0.01)
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


class TestSQLiteEventStorageRotation(TestCase):

    def setUp(self):
        self.directory = path.join("storage", "data")
        self.db_path = path.join(self.directory, "data.db")
        self.config = {
            "data_file_path": self.db_path,
            "messages_ttl_check_in_hours": 1,
            "messages_ttl_in_days": 7,
            "max_read_records_count": 1000,
            "size_limit": 0.05,
            "max_db_amount": 3,
            "oversize_check_period": 1 / 20,
            "writing_batch_size": 1000,
        }
        self.settings = StorageSettings(self.config, enable_validation=False)
        self.stop_event = Event()
        self.sqlite_storage = SQLiteEventStorage(self.settings, LOG, self.stop_event)

    def tearDown(self):
        self.stop_event.set()
        self.sqlite_storage.stop()
        rmtree(self.directory)
        sleep(2)

    def _drain_storage(self, storage: SQLiteEventStorage):
        out = []
        while not self.stop_event.is_set():
            batch = storage.get_event_pack()
            if not batch:
                break
            out.extend(batch)
            storage.event_pack_processing_done()
        sleep(2)
        return out

    def _fill_storage(self, storage, count, delay=0.0):

        for i in range(count):
            fat_msg = "X" * 32768
            result = storage.put(f"{i}:{fat_msg}")
            self.assertTrue(result, f"put() failed at index {i}")
            if delay:
                sleep(delay)

    def _db_files(self):
        return sorted(f for f in listdir(self.directory) if f.endswith(".db"))

    def test_write_read_without_rotation(self):
        self._fill_storage(self.sqlite_storage, 20)
        sleep(1)
        self.assertListEqual(self._db_files(), ["data.db"])

        self.sqlite_storage.stop()

    def test_rotation_creates_new_db(self):
        DATA_RANGE = 150
        self._fill_storage(self.sqlite_storage, DATA_RANGE, delay=0.1)
        sleep(2.0)
        dbs = self._db_files()
        self.assertEqual(len(dbs), 2)
        self.assertLessEqual(len(dbs), self.config["max_db_amount"])

        self.sqlite_storage.stop()

    def test_rotation_persists_across_restart(self):
        DATA_RANGE = 150
        self._fill_storage(self.sqlite_storage, DATA_RANGE, delay=0.1)
        sleep(2.0)
        self.sqlite_storage.stop()
        storage2 = SQLiteEventStorage(self.settings, LOG, self.stop_event)
        storage2.stop()

    def test_no_new_database_appear_after_max_db_amount_reached(self):
        messages_before_db_amount_reached = []
        put_results = []
        for i in range(1800):
            fat_msg = "X" * 32768
            result = self.sqlite_storage.put(f"{i}:{fat_msg}")

            put_results.append(result)
            if not result:
                break
            messages_before_db_amount_reached.append(f"{i}:{fat_msg}")
            sleep(0.07)
        sleep(2.0)
        dbs = sorted(f for f in listdir(self.directory) if f.endswith(".db"))
        self.assertEqual(
            len(dbs),
            self.config["max_db_amount"],
            f"Expected exactly {self.config['max_db_amount']} files, got {len(dbs)}",
        )
        self.assertIn(
            False,
            put_results,
            "Expected self.sqlite_storage.put(...) to eventually return False once max_db_amount was reached",
        )
        self.sqlite_storage.stop()

    def test_sqlite_storage_is_operational_after_max_db_amount_reached_and_storage_restart(
            self,
    ):
        messages_before_db_amount_reached = []
        put_results = []
        for i in range(1800):
            fat_msg = "X" * 32768
            result = self.sqlite_storage.put(f"{i}:{fat_msg}")

            put_results.append(result)
            if not result:
                break
            messages_before_db_amount_reached.append(f"{i}:{fat_msg}")
            sleep(0.07)
        sleep(2.0)
        self.sqlite_storage.stop()
        storage2 = SQLiteEventStorage(self.settings, LOG, self.stop_event)
        dbs = sorted(f for f in listdir(self.directory) if f.endswith(".db"))
        self.assertEqual(
            len(dbs),
            self.config["max_db_amount"],
            f"Expected exactly {self.config['max_db_amount']} files, got {len(dbs)}",
        )
        self.assertIn(
            False,
            put_results,
            "Expected storage.put(...) eventually to return False once max_db_amount was reached",
        )
        storage2.stop()
