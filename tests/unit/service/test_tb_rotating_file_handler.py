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

import logging
import os
import time
from datetime import datetime, timezone
from logging.handlers import TimedRotatingFileHandler as BaseTimedRotatingFileHandler
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase, skipUnless
from unittest.mock import patch

from thingsboard_gateway.tb_utility.tb_rotating_file_handler import TimedRotatingFileHandler


class TestTimedRotatingFileHandler(TestCase):
    START = 1735732800  # 2025-01-01 12:00:00 UTC

    def setUp(self):
        directory = TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.directory = Path(directory.name)
        environment = patch.dict(os.environ, {"TB_GW_LOGS_PATH": str(self.directory)})
        environment.start()
        self.addCleanup(environment.stop)
        clock = patch("time.time", return_value=self.START)
        self.clock = clock.start()
        self.addCleanup(clock.stop)

    def prepare_file(self, filename):
        path = self.directory / filename
        path.touch()
        os.utime(path, (self.START, self.START))
        return str(path)

    def create_handler(self, filename="test.log", **kwargs):
        handler = TimedRotatingFileHandler(self.prepare_file(filename), **kwargs)
        self.addCleanup(handler.close)
        return handler

    def check_clone(self, logger_name):
        for when in ("S", "M", "H", "D", "midnight", "W0", "W6"):
            for interval in (1, 3):
                with self.subTest(when=when, interval=interval):
                    template = self.create_handler(
                        when=when, interval=interval, backupCount=4,
                        encoding="utf-8", delay=True, utc=True, maxBytes=128)
                    template.setFormatter(logging.Formatter("%(levelname)s: %(message)s"))
                    self.prepare_file("clone.log")
                    with patch.object(logging.getLogger(logger_name), "handlers", [template]):
                        factory = getattr(TimedRotatingFileHandler, f"get_{logger_name}_file_handler")
                        clone = factory("clone")
                    self.addCleanup(clone.close)
                    for attribute in ("interval", "rolloverAt", "when", "backupCount", "encoding",
                                      "delay", "utc", "maxBytes", "formatter"):
                        self.assertEqual(getattr(clone, attribute), getattr(template, attribute), attribute)

    def test_connector_clone_preserves_rotation_settings(self):
        self.check_clone("connector")

    def test_converter_clone_preserves_rotation_settings(self):
        self.check_clone("converter")

    def write_records(self, handler, start, stop, clock_step=0):
        for index in range(start, stop):
            self.clock.return_value = self.START + index * clock_step
            handler.handle(logging.LogRecord(
                "test", logging.INFO, "test", 1,
                "record-%02d %s", (index, "x" * 50), None))
        handler.flush()

    def check_records(self, handler, expected, archive_count):
        files = list(self.directory.glob(Path(handler.baseFilename).name + "*"))
        self.assertEqual(len(files), archive_count + 1)
        records = []
        for path in files:
            self.assertLessEqual(path.stat().st_size, 128)
            records.extend(path.read_text().splitlines())
        self.assertCountEqual(records, ["record-%02d %s" % (index, "x" * 50) for index in expected])

    def test_repeated_size_rollovers_retain_newest_records(self):
        for when, clock_step in (("D", 0), ("midnight", 1)):
            for backups in (0, 1, 4):
                for delay in (False, True):
                    with self.subTest(when=when, backups=backups, delay=delay):
                        filename = f"test_{when}_{backups}_{delay}".lower() + ".log"
                        handler = self.create_handler(
                            filename, when=when, interval=3, utc=True, delay=delay,
                            maxBytes=128, backupCount=backups)
                        self.write_records(handler, 0, 40, clock_step)
                        kept = 40 if not backups else 2 * (backups + 1)
                        self.check_records(handler, range(40 - kept, 40), 19 if not backups else backups)

    def test_restart_with_existing_archives_continues_size_rotation(self):
        handler = self.create_handler(when="D", interval=3, utc=True, maxBytes=128, backupCount=4)
        self.write_records(handler, 0, 20)
        handler.close()
        handler = self.create_handler(when="D", interval=3, utc=True, maxBytes=128, backupCount=4)
        self.write_records(handler, 20, 40)
        self.check_records(handler, range(30, 40), 4)

    def test_size_rollover_prunes_existing_timed_archives_and_preserves_other_files(self):
        previous = self.directory / "test.log.2024-12-31_12-00-00"
        previous.write_text("old archive\n")
        unrelated = self.directory / "other.log.2024-12-31_12-00-00"
        unrelated.write_text("keep\n")
        handler = self.create_handler(when="D", interval=3, utc=True, maxBytes=128, backupCount=4)
        self.write_records(handler, 0, 40)
        self.check_records(handler, range(30, 40), 4)
        self.assertFalse(previous.exists())
        self.assertEqual(unrelated.read_text(), "keep\n")

    def test_custom_archive_namer_preserves_records_and_retention(self):
        for name_type in (str, Path):
            with self.subTest(name_type=name_type):
                filename = "test_" + name_type.__name__.lower() + ".log"
                handler = self.create_handler(filename, when="D", interval=3, utc=True,
                                              maxBytes=128, backupCount=4)
                handler.namer = lambda filename: name_type(filename + ".archive")
                self.write_records(handler, 0, 40)
                self.check_records(handler, range(30, 40), 4)

    def test_timed_rollover_continues_after_size_rollovers(self):
        handler = self.create_handler(when="midnight", utc=True, maxBytes=128, backupCount=4)
        self.write_records(handler, 0, 20, clock_step=1)
        deadline = handler.rolloverAt
        self.clock.return_value = deadline
        handler.handle(logging.makeLogRecord({"msg": "timed"}))
        handler.flush()
        self.assertGreater(handler.rolloverAt, deadline)
        self.assertEqual(Path(handler.baseFilename).read_text(), "timed\n")

    def test_timed_schedule_matches_standard_handler(self):
        for when in ("S", "M", "H", "D", "midnight", "W0"):
            for max_bytes in (0, 128):
                with self.subTest(when=when, maxBytes=max_bytes):
                    filename = f"test_{when}_{max_bytes}".lower() + ".log"
                    handler = self.create_handler(filename, when=when, interval=3, utc=True, maxBytes=max_bytes)
                    reference = BaseTimedRotatingFileHandler(
                        self.prepare_file("reference_" + filename), when=when, interval=3, utc=True)
                    self.addCleanup(reference.close)
                    self.assertEqual(handler.rolloverAt, reference.rolloverAt)
                    for _ in range(2):
                        self.clock.return_value = reference.rolloverAt
                        record = logging.makeLogRecord({"msg": "timed"})
                        reference.handle(record)
                        handler.handle(record)
                        self.assertEqual(handler.rolloverAt, reference.rolloverAt)

    @skipUnless(hasattr(time, "tzset"), "requires tzset")
    def test_local_midnight_schedule_preserves_daylight_saving_adjustments(self):
        try:
            with patch.dict(os.environ, {"TZ": "EST5EDT,M3.2.0,M11.1.0"}):
                time.tzset()
                for date in ((2025, 3, 8, 17), (2025, 11, 1, 16)):
                    with self.subTest(date=date):
                        self.START = int(datetime(*date, tzinfo=timezone.utc).timestamp())
                        self.clock.return_value = self.START
                        filename = f"test_dst_{date[1]}.log"
                        handler = self.create_handler(filename, when="midnight", maxBytes=128)
                        reference = BaseTimedRotatingFileHandler(
                            self.prepare_file("reference_" + filename), when="midnight")
                        self.addCleanup(reference.close)
                        for _ in range(2):
                            self.assertEqual(handler.rolloverAt, reference.rolloverAt)
                            self.clock.return_value = reference.rolloverAt
                            record = logging.makeLogRecord({"msg": "timed"})
                            reference.handle(record)
                            handler.handle(record)
                            self.assertEqual(handler.rolloverAt, reference.rolloverAt)
        finally:
            time.tzset()
