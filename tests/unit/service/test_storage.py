from logging import getLogger
from os import listdir, remove, removedirs, path
from random import randint
from shutil import rmtree
from tempfile import mkdtemp
from threading import Event
from time import sleep
from unittest import TestCase

from thingsboard_gateway.storage.file.file_event_storage import FileEventStorage
from thingsboard_gateway.storage.memory.memory_event_storage import MemoryEventStorage
from thingsboard_gateway.storage.sqlite.sqlite_event_storage import SQLiteEventStorage

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
        self.tmpdir = mkdtemp()
        self.base_db = path.join(self.tmpdir, "data.db")
        self.config = {
            "data_file_path": self.base_db,
            "messages_ttl_check_in_hours": 1,
            "messages_ttl_in_days": 7,
            "max_read_records_count": 1000,
            "size_limit": 0.025,
            "max_db_amount": 3,
            "oversize_check_period": 1 / 6,
            "writing_batch_size": 10000,
        }
        self.stop_event = Event()

    def tearDown(self):
        self.stop_event.set()
        rmtree(self.tmpdir)

    @staticmethod
    def _drain_storage(storage: SQLiteEventStorage):
        out = []
        while True:
            batch = storage.get_event_pack()
            if not batch:
                break
            out.extend(batch)
            storage.event_pack_processing_done()
        return out

    def _fill_storage(self, storage, count, delay=0.0):

        for i in range(count):
            result = storage.put(str(i))
            self.assertTrue(result, f"put() failed at index {i}")
            if delay:
                sleep(delay)

    def _db_files(self):
        return sorted(f for f in listdir(self.tmpdir) if f.endswith(".db"))

    def test_write_read_without_rotation(self):
        storage = SQLiteEventStorage(self.config, LOG, self.stop_event)
        self._fill_storage(storage, 50)
        sleep(0.5)
        self.assertListEqual(self._db_files(), ["data.db"])
        all_messages = self._drain_storage(storage)
        self.assertEqual(len(all_messages), 50)
        self.assertListEqual(all_messages, [str(i) for i in range(50)])

        storage.stop()

    def test_rotation_creates_new_db_and_reads_all_data(self):
        DATA_RANGE = 1000

        storage = SQLiteEventStorage(self.config, LOG, self.stop_event)
        self._fill_storage(storage, DATA_RANGE, delay=0.07)
        sleep(1.0)

        dbs = self._db_files()
        with self.subTest("db count after rotation"):
            self.assertEqual(len(dbs), 2)
            self.assertLessEqual(len(dbs), self.config["max_db_amount"])

        with self.subTest("round-trip all messages"):
            all_messages = self._drain_storage(storage)
            self.assertEqual(len(all_messages), DATA_RANGE)
            self.assertListEqual(all_messages, [str(i) for i in range(DATA_RANGE)])

        storage.stop()

    def test_rotation_persists_across_restart(self):
        DATA_RANGE = 1000
        storage1 = SQLiteEventStorage(self.config, LOG, self.stop_event)
        self._fill_storage(storage1, DATA_RANGE, delay=0.07)
        sleep(1.0)
        storage1.stop()
        storage2 = SQLiteEventStorage(self.config, LOG, self.stop_event)
        all_messages = self._drain_storage(storage2)
        self.assertEqual(len(all_messages), DATA_RANGE)
        self.assertListEqual(all_messages, [str(i) for i in range(DATA_RANGE)])
        storage2.stop()

    def test_no_new_database_appear_after_max_db_amount_reached(self):
        messages_before_db_amount_reached = []
        storage = SQLiteEventStorage(self.config, LOG, self.stop_event)
        put_results = []
        for i in range(3000):
            result = storage.put(str(i))

            put_results.append(result)
            if not result:
                break
            messages_before_db_amount_reached.append(i)
            sleep(0.07)
        sleep(2.0)
        dbs = sorted(f for f in listdir(self.tmpdir) if f.endswith(".db"))

        self.assertEqual(
            len(dbs),
            self.config["max_db_amount"],
            f"Expected exactly {self.config['max_db_amount']} files, got {len(dbs)}",
        )
        self.assertIn(
            False,
            put_results,
            "Expected storage.put(...) to eventually return False once max_db_amount was reached",
        )
        all_messages = self._drain_storage(storage)
        self.assertEqual(len(all_messages), len(messages_before_db_amount_reached))
        self.assertListEqual(
            all_messages,
            [str(i) for i in range(len(messages_before_db_amount_reached))],
        )
        storage.stop()

    def test_sqlite_storage_is_operational_after_max_db_amount_reached_and_storage_restart(
        self,
    ):
        messages_before_db_amount_reached = []
        storage = SQLiteEventStorage(self.config, LOG, self.stop_event)
        put_results = []
        for i in range(3000):
            result = storage.put(str(i))

            put_results.append(result)
            if not result:
                break
            messages_before_db_amount_reached.append(i)
            sleep(0.07)
        sleep(2.0)
        storage.stop()
        storage2 = SQLiteEventStorage(self.config, LOG, self.stop_event)
        dbs = sorted(f for f in listdir(self.tmpdir) if f.endswith(".db"))
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
        all_messages = list(self._drain_storage(storage2))
        self.assertEqual(len(all_messages), len(messages_before_db_amount_reached))
        self.assertListEqual(
            all_messages,
            [str(i) for i in range(len(messages_before_db_amount_reached))],
        )
        storage2.stop()
