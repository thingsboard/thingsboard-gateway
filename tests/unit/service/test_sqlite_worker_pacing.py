# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from pathlib import Path
from queue import Queue
from tempfile import TemporaryDirectory
from threading import Event
from unittest import TestCase
from unittest.mock import Mock, patch

from thingsboard_gateway.storage.sqlite.database import Database
from thingsboard_gateway.storage.sqlite.storage_settings import StorageSettings


class TestSQLiteWorkerPacing(TestCase):
    def run_iteration(self, should_write, pending_write, elapsed=0.05):
        with TemporaryDirectory() as directory:
            settings = StorageSettings({"data_file_path": directory + "/"})
            settings.data_file_path = str(Path(directory) / "test.db")
            queue = Queue()
            if pending_write:
                queue.put("pending telemetry")
            stopped = Event()
            logger = Mock()
            database = Database(
                settings,
                queue,
                logger,
                stopped,
                should_read=True,
                should_write=should_write,
            )
            try:
                # Execute exactly one loop iteration without real-time waits.
                with (
                    patch.object(stopped, "is_set", side_effect=[False, True]),
                    patch.object(database, "read_data", return_value=[]) as read,
                    patch.object(database, "process") as process,
                    patch(
                        "thingsboard_gateway.storage.sqlite.database.monotonic",
                        side_effect=[0, 0, elapsed, elapsed],
                    ),
                    patch("thingsboard_gateway.storage.sqlite.database.sleep") as sleep,
                ):
                    database.run()
                logger.exception.assert_not_called()
                read.assert_called_once_with()
                self.assertEqual(process.call_count, int(should_write))
                self.assertEqual(queue.qsize(), int(pending_write))
                return sleep
            finally:
                database.db.close()

    def test_read_only_worker_waits_with_shared_write_backlog(self):
        sleep = self.run_iteration(should_write=False, pending_write=True)
        sleep.assert_called_once()
        self.assertAlmostEqual(sleep.call_args.args[0], 0.15)

    def test_read_only_worker_waits_with_empty_queue(self):
        sleep = self.run_iteration(should_write=False, pending_write=False)
        sleep.assert_called_once()
        self.assertAlmostEqual(sleep.call_args.args[0], 0.15)

    def test_writer_keeps_processing_backlog_without_extra_wait(self):
        sleep = self.run_iteration(should_write=True, pending_write=True)
        sleep.assert_not_called()

    def test_idle_writer_waits(self):
        sleep = self.run_iteration(should_write=True, pending_write=False)
        sleep.assert_called_once()

    def test_slow_reader_does_not_wait_again(self):
        sleep = self.run_iteration(should_write=False, pending_write=True, elapsed=0.3)
        sleep.assert_not_called()
