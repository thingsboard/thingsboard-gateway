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

"""
Regression test for duplicate telemetry caused by SQLite event storage.

Bug summary:
    The Database background thread pre-fetches the next batch of records into
    an in-memory cache (__next_batch). The pre-fetch is unlocked by
    can_prepare_new_batch() which is called inside get_event_pack() — BEFORE
    event_pack_processing_done() deletes the records from the database.

    This creates a race window where the Database thread can SELECT the same
    records that haven't been deleted yet, cache them, and serve them again
    on the next get_event_pack() call — producing duplicate telemetry.

    Memory storage is not affected because Queue.get_nowait() is a destructive
    read — once consumed, data cannot be re-read.

Affected files:
    - storage/sqlite/sqlite_event_storage.py  (get_event_pack, event_pack_processing_done)
    - storage/sqlite/database.py              (run, read_data, can_prepare_new_batch)

How to run:
    python -m pytest tests/unit/service/test_sqlite_duplicate_race_condition.py -v -s
"""

import logging
from os import path
from shutil import rmtree
from threading import Event
from time import sleep
from unittest import TestCase

from thingsboard_gateway.storage.memory.memory_event_storage import MemoryEventStorage
from thingsboard_gateway.storage.sqlite.sqlite_event_storage import SQLiteEventStorage
from thingsboard_gateway.storage.sqlite.storage_settings import StorageSettings

LOG = logging.getLogger("TEST")
LOG.setLevel(logging.DEBUG)
LOG.trace = LOG.debug

# Add console handler so test output is visible when running with -s
if not LOG.handlers:
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
    LOG.addHandler(handler)


class TestSQLiteDuplicateRaceCondition(TestCase):
    """
    Proves that SQLite event storage returns duplicate data due to a race
    condition between the Database thread's pre-fetch mechanism and the
    record deletion cycle.

    The test uses time.sleep() to reliably widen the race window, simulating
    the real-world scenario where the Database thread has time to pre-fetch
    between get_event_pack() and event_pack_processing_done().
    """

    TOTAL_MESSAGES = 20
    BATCH_SIZE = 10

    def setUp(self):
        self.directory = "./test_race_condition_data/"
        self.config = {
            "data_file_path": self.directory,
            "messages_ttl_check_in_hours": 1,
            "messages_ttl_in_days": 7,
            "max_read_records_count": self.BATCH_SIZE,
            "writing_batch_size": 1000,
            "size_limit": 1024,
            "max_db_amount": 10,
            "oversize_check_period": 1,
        }
        self.settings = StorageSettings(self.config, enable_validation=False)
        self.stop_event = Event()

    def tearDown(self):
        self.stop_event.set()
        sleep(1)
        if path.exists(self.directory):
            rmtree(self.directory, ignore_errors=True)

    def test_sqlite_prefetch_returns_stale_data_after_deletion(self):
        """
        Reproduces the race condition that causes duplicate telemetry.

        The sequence mirrors what happens in production inside
        tb_gateway_service.py's __read_data_from_storage() loop:

            events = self._event_storage.get_event_pack()     # read batch
            # ... process and send via MQTT ...
            self._event_storage.event_pack_processing_done()  # delete batch

        The problem: get_event_pack() calls can_prepare_new_batch() which
        unlocks the Database thread to pre-fetch. The thread does a SELECT
        and caches the same records (not deleted yet). After deletion, the
        next get_event_pack() returns the stale cache — duplicates.

        Expected (correct):
            batch1 = ["msg_0", ..., "msg_9"]
            batch2 = ["msg_10", ..., "msg_19"]

        Actual (bug):
            batch1 = ["msg_0", ..., "msg_9"]
            batch2 = ["msg_0", ..., "msg_9"]   <-- DUPLICATE
        """
        storage = SQLiteEventStorage(self.settings, LOG, self.stop_event)

        # ── Step 1: Insert messages ──────────────────────────────────────
        # Simulates connectors writing telemetry data to storage.
        LOG.info("Inserting %d messages into SQLite storage...", self.TOTAL_MESSAGES)
        for i in range(self.TOTAL_MESSAGES):
            result = storage.put(f"msg_{i}")
            self.assertTrue(result, f"Failed to insert msg_{i}")

        # Wait for the Database write thread to flush messages to disk.
        # The Database.process() method batches writes every 0.1-0.2s.
        sleep(1)
        LOG.info("All messages written to SQLite.")

        # ── Step 2: Read first batch ─────────────────────────────────────
        # This mirrors: events = self._event_storage.get_event_pack()
        # Internally, get_event_pack() does:
        #   1. read_data() → SELECT id, timestamp, message FROM messages LIMIT 10
        #   2. process_event_storage_data() → extract message strings
        #   3. can_prepare_new_batch() → sets __can_prepare_new_batch = True
        #      ^^^ This unlocks the Database thread to pre-fetch the next batch
        batch1 = storage.get_event_pack()
        self.assertGreater(len(batch1), 0, "batch1 should not be empty")
        LOG.info("BATCH 1: got %d messages: %s", len(batch1), batch1)

        # ── Step 3: Wait for the pre-fetch race condition ────────────────
        # At this point:
        #   - can_prepare_new_batch() has been called (inside get_event_pack)
        #   - __can_prepare_new_batch = True, __next_batch = []
        #   - The Database thread (database.py line 117) sees the flag and runs:
        #       self.__next_batch = self.read_data()
        #     which does SELECT ... LIMIT 10 and gets the SAME records
        #     (they haven't been deleted yet!)
        #
        # We sleep to ensure the Database thread has time to execute this
        # pre-fetch. In production, this happens naturally because sending
        # data via MQTT takes time between get_event_pack() and
        # event_pack_processing_done().
        LOG.info("Sleeping 0.5s to let Database thread pre-fetch (race window)...")
        sleep(0.5)

        # ── Step 4: Delete the first batch ───────────────────────────────
        # This mirrors: self._event_storage.event_pack_processing_done()
        # Internally does: DELETE FROM messages WHERE id <= <last_id_from_batch1>
        #
        # AFTER this call, the records from batch1 no longer exist in SQLite.
        # However, the Database thread's __next_batch cache still holds them
        # because it pre-fetched BEFORE the deletion happened.
        LOG.info("Calling event_pack_processing_done() — deleting batch1 records from DB...")
        storage.event_pack_processing_done()
        LOG.info("Batch1 records deleted from database.")

        # ── Step 5: Read second batch ────────────────────────────────────
        # This mirrors the next iteration: events = self._event_storage.get_event_pack()
        # Internally, read_data() checks:
        #   if self.__next_batch:
        #       return self.__next_batch   <-- returns stale cache WITHOUT querying DB!
        #
        # BUG: __next_batch still contains batch1 records (pre-fetched in step 3)
        # even though they were deleted from the database in step 4.
        batch2 = storage.get_event_pack()
        LOG.info("BATCH 2: got %d messages: %s", len(batch2), batch2)

        # ── Step 6: Verify duplicates ────────────────────────────────────
        if batch2:
            batch1_set = set(batch1)
            batch2_set = set(batch2)
            duplicates = batch1_set.intersection(batch2_set)
            only_in_batch2 = batch2_set - batch1_set

            LOG.info("──────────── RESULTS ────────────")
            LOG.info("Batch1 messages: %s", sorted(batch1))
            LOG.info("Batch2 messages: %s", sorted(batch2))
            LOG.info("Duplicates (in both batches): %d", len(duplicates))
            LOG.info("New messages (only in batch2): %d", len(only_in_batch2))

            if duplicates:
                LOG.error("BUG CONFIRMED: these messages were returned TWICE:")
                for dup in sorted(duplicates):
                    LOG.error("  - %s", dup)

            if only_in_batch2:
                LOG.info("New messages in batch2 (expected): %s", sorted(only_in_batch2))

            # If the storage works correctly, batch2 should contain ONLY new
            # messages (msg_10..msg_19) with ZERO overlap with batch1.
            self.assertEqual(
                len(duplicates), 0,
                f"\n\nDUPLICATE TELEMETRY BUG DETECTED!\n"
                f"  {len(duplicates)} out of {len(batch2)} messages in batch2 are duplicates from batch1.\n"
                f"  batch1: {sorted(batch1)}\n"
                f"  batch2: {sorted(batch2)}\n"
                f"  duplicates: {sorted(duplicates)}\n\n"
                f"  Root cause: can_prepare_new_batch() is called inside get_event_pack()\n"
                f"  BEFORE event_pack_processing_done() deletes the records. The Database\n"
                f"  thread pre-fetches the same records into __next_batch cache, and the\n"
                f"  next get_event_pack() returns the stale cache.\n\n"
                f"  Fix: move can_prepare_new_batch() from get_event_pack() to\n"
                f"  event_pack_processing_done(), AFTER delete_data()."
            )

        storage.stop()

    def test_memory_storage_no_duplicates(self):
        """
        Control test: same flow as the SQLite test, but with MemoryEventStorage.
        This MUST always pass, proving the issue is specific to SQLite storage.

        Memory storage uses Queue.get_nowait() which is a destructive read —
        once an item is dequeued, it's gone. No cache, no pre-fetch, no re-read.
        """
        memory_config = {
            "type": "memory",
            "read_records_count": self.BATCH_SIZE,
            "max_records_count": 1000,
        }

        storage = MemoryEventStorage(memory_config, LOG, self.stop_event)

        LOG.info("Inserting %d messages into memory storage...", self.TOTAL_MESSAGES)
        for i in range(self.TOTAL_MESSAGES):
            storage.put(f"msg_{i}")

        # Read first batch (destructive read — items removed from queue)
        batch1 = storage.get_event_pack()
        self.assertEqual(len(batch1), self.BATCH_SIZE)
        LOG.info("MEMORY BATCH 1: got %d messages: %s", len(batch1), batch1)

        # Same sleep as SQLite test to keep the comparison fair
        sleep(0.5)

        # Mark as processed (just clears the internal list)
        storage.event_pack_processing_done()

        # Read second batch — always gets NEW data with memory storage
        batch2 = storage.get_event_pack()
        LOG.info("MEMORY BATCH 2: got %d messages: %s", len(batch2), batch2)

        batch1_set = set(batch1)
        batch2_set = set(batch2)
        duplicates = batch1_set.intersection(batch2_set)

        LOG.info("──────────── MEMORY RESULTS ────────────")
        LOG.info("Duplicates: %d (expected: 0)", len(duplicates))

        self.assertEqual(
            len(duplicates), 0,
            f"Memory storage should never produce duplicates, but found {len(duplicates)}.\n"
            f"  batch1: {sorted(batch1)}\n"
            f"  batch2: {sorted(batch2)}\n"
            f"  duplicates: {sorted(duplicates)}"
        )

        storage.stop()
