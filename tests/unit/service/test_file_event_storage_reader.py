#     Copyright 2026. ThingsBoard
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

from base64 import b64encode
from json import dump, dumps
from logging import getLogger
from os import sep
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase

from thingsboard_gateway.storage.file.event_storage_files import EventStorageFiles
from thingsboard_gateway.storage.file.event_storage_reader import EventStorageReader
from thingsboard_gateway.storage.file.file_event_storage_settings import FileEventStorageSettings


LOG = getLogger("TEST")
LOG.trace = LOG.debug


class TestFileEventStorageReader(TestCase):
    DATA_FILE = "data_test.txt"
    STATE_FILE = "state_file.txt"

    def setUp(self):
        self.temporary_directory = TemporaryDirectory()
        self.data_directory = Path(self.temporary_directory.name)
        self.data_file = self.data_directory / self.DATA_FILE
        self.state_file = self.data_directory / self.STATE_FILE
        with self.state_file.open("w", encoding="utf-8") as state_file:
            dump({"position": 0, "file": self.DATA_FILE}, state_file)
        self.reader = None

    def tearDown(self):
        if self.reader is not None and self.reader.buffered_reader is not None:
            self.reader.buffered_reader.close()
        self.temporary_directory.cleanup()

    def _create_reader(self, additional_data_files=None):
        data_files = {self.DATA_FILE: False}
        if additional_data_files:
            data_files.update({file_name: False for file_name in additional_data_files})
        files = EventStorageFiles(self.STATE_FILE, data_files)
        settings = FileEventStorageSettings({
            "data_folder_path": str(self.data_directory) + sep,
            "max_records_per_file": 100,
            "max_read_records_count": 100,
        })
        self.reader = EventStorageReader(files, settings, LOG)
        return self.reader

    @staticmethod
    def _event(timestamp):
        event = {
            "deviceName": "Test Device",
            "telemetry": [{"ts": timestamp, "values": {f"key_{index}": index for index in range(100)}}],
        }
        return dumps(event, separators=(",", ":"))

    @classmethod
    def _encoded_event(cls, timestamp):
        return b64encode(cls._event(timestamp).encode("utf-8"))

    def test_waits_for_newline_before_returning_first_record(self):
        encoded_event = self._encoded_event(1)
        partial_length = 64
        self.data_file.write_bytes(encoded_event[:partial_length])
        reader = self._create_reader()

        self.assertListEqual(reader.read(), [])

        with self.data_file.open("ab") as data_file:
            data_file.write(encoded_event[partial_length:] + b"\n")

        self.assertListEqual(reader.read(), [self._event(1)])

    def test_does_not_skip_partial_record_when_next_file_exists(self):
        encoded_event = self._encoded_event(1)
        next_data_file = "data_zzzz.txt"
        self.data_file.write_bytes(encoded_event[:64])
        (self.data_directory / next_data_file).write_bytes(self._encoded_event(2) + b"\n")
        reader = self._create_reader([next_data_file])

        self.assertListEqual(reader.read(), [])
        self.assertTrue(self.data_file.exists())

        with self.data_file.open("ab") as data_file:
            data_file.write(encoded_event[64:] + b"\n")

        self.assertListEqual(reader.read(), [self._event(1), self._event(2)])

    def test_preserves_order_when_complete_record_precedes_partial_record(self):
        first_event = self._encoded_event(1)
        second_event = self._encoded_event(2)
        partial_length = 64
        self.data_file.write_bytes(first_event + b"\n" + second_event[:partial_length])
        reader = self._create_reader()

        first_batch = reader.read()
        reader.discard_batch()
        with self.data_file.open("ab") as data_file:
            data_file.write(second_event[partial_length:] + b"\n")
        second_batch = reader.read()

        self.assertEqual(1, len(first_batch))
        self.assertEqual(1, len(second_batch))
        self.assertIn('"ts":1', first_batch[0])
        self.assertIn('"ts":2', second_batch[0])

    def test_preserves_partial_record_after_reader_restart(self):
        first_event = self._encoded_event(1)
        second_event = self._encoded_event(2)
        partial_length = 64
        self.data_file.write_bytes(first_event + b"\n" + second_event[:partial_length])
        reader = self._create_reader()

        self.assertListEqual(reader.read(), [self._event(1)])
        reader.discard_batch()
        reader.buffered_reader.close()

        restarted_reader = self._create_reader()
        self.assertListEqual(restarted_reader.read(), [])

        with self.data_file.open("ab") as data_file:
            data_file.write(second_event[partial_length:] + b"\n")

        self.assertListEqual(restarted_reader.read(), [self._event(2)])
