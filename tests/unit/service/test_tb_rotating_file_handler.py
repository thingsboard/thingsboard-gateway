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
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase
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
