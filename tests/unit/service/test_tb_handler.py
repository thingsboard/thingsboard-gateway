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
from unittest import TestCase
from unittest.mock import MagicMock

from thingsboard_gateway.tb_utility.tb_handler import TBRemoteLoggerHandler


class _NonThreadingRemoteLoggerHandler(TBRemoteLoggerHandler):
    """Skips starting the real background log-sending thread, so handle() can be tested in isolation."""

    def activate(self):
        self.activated = True


class TestTBRemoteLoggerHandler(TestCase):
    def setUp(self):
        self.gateway = MagicMock()
        self.gateway.stopped = False
        self.handler = _NonThreadingRemoteLoggerHandler(self.gateway)

    @staticmethod
    def make_record(name, level=logging.INFO, message="test message"):
        return logging.LogRecord(name, level, "test.py", 1, message, None, None)

    def test_handle_ignores_record_from_unregistered_logger(self):
        # Regression test for IOTGW-820: third-party loggers (e.g. python-can's
        # "can.interfaces.socketcan.socketcan") are not pre-registered in self.loggers,
        # so handle() must not blow up trying to unpack None.
        record = self.make_record("can.interfaces.socketcan.socketcan")

        self.handler.handle(record)

        self.assertTrue(self.handler._logs_queue.empty())

    def test_handle_queues_message_for_registered_logger(self):
        self.handler.activate_remote_logging_for_level(logging.DEBUG)
        record = self.make_record("service", message="hello")

        self.handler.handle(record)

        log_msg = self.handler._logs_queue.get_nowait()
        self.assertIn("SERVICE_LOGS", log_msg["values"])
        self.assertEqual(log_msg["values"]["SERVICE_LOGS"], log_msg["values"]["LOGS"])

    def test_handle_respects_remote_logging_level(self):
        self.handler.activate_remote_logging_for_level(logging.ERROR)
        record = self.make_record("service", level=logging.INFO)

        self.handler.handle(record)

        self.assertTrue(self.handler._logs_queue.empty())
