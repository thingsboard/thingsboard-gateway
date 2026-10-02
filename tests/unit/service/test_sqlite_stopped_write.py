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

from pathlib import Path
from tempfile import TemporaryDirectory
from threading import Event, Thread
from unittest import TestCase
from unittest.mock import Mock

from thingsboard_gateway.storage.sqlite.database_connector import DatabaseConnector

INSERT = "INSERT INTO messages (timestamp, message) VALUES (?, ?);"


class TestSQLiteWriteAfterStop(TestCase):
    """A write must return once the storage is stopped (thingsboard-gateway#2196)."""

    def setUp(self):
        self.directory = TemporaryDirectory()
        self.stopped = Event()
        self.connector = DatabaseConnector(
            str(Path(self.directory.name) / "test.db"), Mock(), self.stopped
        )
        self.connector.connect()
        self.connector.connection.execute(
            "CREATE TABLE messages (id INTEGER PRIMARY KEY, timestamp INTEGER, message TEXT);"
        )

    def tearDown(self):
        self.connector.close()
        self.directory.cleanup()

    def assert_returns_after_stop(self, write, *args):
        self.stopped.set()
        thread = Thread(target=write, args=args, daemon=True)
        thread.start()
        thread.join(timeout=2)
        self.assertFalse(thread.is_alive(), "write did not return after the database was stopped")

    def test_execute_many_write_returns_after_stop(self):
        self.assert_returns_after_stop(self.connector.execute_many_write, INSERT, [(1, "a")])

    def test_execute_write_returns_after_stop(self):
        self.assert_returns_after_stop(self.connector.execute_write, INSERT, (1, "a"))

    def test_execute_many_write_still_writes_when_running(self):
        self.connector.execute_many_write(INSERT, [(1, "a"), (2, "b")])
        self.connector.commit()
        count = self.connector.connection.execute("SELECT COUNT(*) FROM messages;").fetchone()[0]
        self.assertEqual(count, 2)
