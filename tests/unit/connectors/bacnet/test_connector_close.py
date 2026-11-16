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

import asyncio
import logging
import socket
from time import monotonic, sleep
from unittest import TestCase
from unittest.mock import MagicMock, patch

from thingsboard_gateway.connectors.bacnet.bacnet_connector import AsyncBACnetConnector

HOST = '127.0.0.1'


def _free_udp_port():
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
        s.bind((HOST, 0))
        return s.getsockname()[1]


def _port_is_free(port):
    """Exclusive bind (no SO_REUSEPORT): fails while any other socket still holds host:port."""
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
        try:
            s.bind((HOST, port))
            return True
        except OSError:
            return False


def _wait_for(predicate, timeout=5.0):
    deadline = monotonic() + timeout
    while monotonic() < deadline:
        if predicate():
            return True
        sleep(.05)
    return predicate()


class BacnetConnectorCloseTestCase(TestCase):
    def setUp(self):
        self.port = _free_udp_port()
        self.connectors = []

    def tearDown(self):
        for connector in self.connectors:
            if connector.is_alive():
                connector.close()

    def _make_connector(self):
        config = {
            'name': 'BACnet close test',
            'id': 'bacnet-close-test',
            'application': {
                'objectName': 'TB_gateway',
                'host': HOST,
                'port': self.port,
                'objectIdentifier': 599,
                'vendorIdentifier': 15,
                'maxApduLengthAccepted': 1476,
                'segmentationSupported': 'segmentedBoth',
            },
            'devices': [],
        }
        with patch('thingsboard_gateway.connectors.bacnet.bacnet_connector.init_logger',
                   side_effect=lambda *args, **kwargs: logging.getLogger(args[1])):
            connector = AsyncBACnetConnector(MagicMock(), config, 'bacnet')
        self.connectors.append(connector)
        return connector

    def _start_and_wait_bound(self, connector):
        connector.open()
        self.assertTrue(_wait_for(lambda: not _port_is_free(self.port)),
                        'connector did not bind %s:%s' % (HOST, self.port))

    def _assert_stopped_and_released(self, connector, max_close_seconds):
        started = monotonic()
        connector.close()
        took = monotonic() - started

        self.assertTrue(connector.is_stopped())
        self.assertFalse(connector.is_alive(), 'connector thread still alive after close()')
        self.assertLess(took, max_close_seconds)
        self.assertTrue(connector.loop.is_closed())
        self.assertTrue(_port_is_free(self.port),
                        'UDP port %s:%s still bound after close()' % (HOST, self.port))

    def test_close_releases_socket_and_port_can_be_bound_again(self):
        first = self._make_connector()
        self._start_and_wait_bound(first)
        self._assert_stopped_and_released(first, max_close_seconds=2)

        # a new connector (as created by a remote configuration update) re-binds the same host:port
        second = self._make_connector()
        self._start_and_wait_bound(second)
        self._assert_stopped_and_released(second, max_close_seconds=2)

    def test_close_does_not_wait_for_long_running_tasks(self):
        # Simulates a Who-Is sweep / read that is still awaiting a reply from an unresponsive device
        async def long_discovery(_self):
            await asyncio.sleep(60)

        with patch.object(AsyncBACnetConnector, '_AsyncBACnetConnector__discover_devices', long_discovery):
            connector = self._make_connector()
            self._start_and_wait_bound(connector)
            # must be well inside the remote configurator's 5 second limit
            self._assert_stopped_and_released(connector, max_close_seconds=2)

    def test_close_before_start(self):
        connector = self._make_connector()
        connector.close()

        self.assertTrue(connector.is_stopped())
        self.assertFalse(connector.is_alive())
        self.assertTrue(_port_is_free(self.port))
