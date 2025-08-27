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

import logging
from os import path
from unittest.mock import MagicMock

from tests.unit.connectors.mqtt.mqtt_base_test import MqttBaseTest


class MqttOnConnectRequestTest(MqttBaseTest):
    def setUp(self):
        super().setUp()
        gw = self.connector._MqttConnector__gateway
        gw.add_device = MagicMock()
        self.logger = logging.getLogger("tb.mqtt.connector.test")
        self.connector._MqttConnector__log = self.logger
        self.message = MagicMock()
        self.message.properties = []
        self.message.qos = 0
        self.payload_connect_handler = self.convert_json(
            path.join(self.CONFIG_PATH, 'connect_requests/on_connect_mqtt_config.json')
        )

    def tearDown(self):
        self.message = None
        super().tearDown()

    def test_connect_request_with_device_name_in_payload(self):
        self.connector._MqttConnector__connect_requests_sub_topics = {
            'sensor/connect': self.payload_connect_handler
        }
        self.message.topic = 'sensor/connect'
        self.message.payload = b'{"serialNumber":"SN-002"}'

        handled, _ = self.connector._MqttConnector__process_connect(self.message, None)
        self.assertTrue(handled)
        self.assertTrue(self.connector._MqttConnector__gateway.add_device.called)
        args, kwargs = self.connector._MqttConnector__gateway.add_device.call_args
        self.assertEqual(args[0], "SN-002")
        self.assertIs(args[1].get("connector"), self.connector)
        self.assertEqual(kwargs.get("device_type"), "Thermometer")
        with self.assertLogs("tb.mqtt.connector.test", level="INFO") as cm:
            self.logger.info("Connecting device %s of type %s", "SN-002", "Thermometer")

    def test_connect_request_with_device_name_from_topic(self):
        self.payload_connect_handler = self.convert_json(
            path.join(self.CONFIG_PATH, 'connect_requests/on_connect_mqtt_config_topic_device_section.json')
        )

        self.connector._MqttConnector__connect_requests_sub_topics = {
            'sensor/[^/]+/connect': self.payload_connect_handler
        }
        self.message.topic = 'sensor/SN-001/connect'
        self.message.payload = b''

        handled, _ = self.connector._MqttConnector__process_connect(self.message, None)
        self.assertTrue(handled)

        self.connector._MqttConnector__gateway.add_device.assert_called_once()
        args, kwargs = self.connector._MqttConnector__gateway.add_device.call_args
        self.assertEqual(args[0], "SN-001")
        self.assertIs(args[1].get("connector"), self.connector)
        self.assertEqual(kwargs.get("device_type"), "Thermometer")

    def test_connect_request_logs_error_when_device_name_missing(self):
        self.connector._MqttConnector__connect_requests_sub_topics = {
            'sensor/connect': self.payload_connect_handler
        }
        self.message.topic = 'sensor/connect'
        self.message.payload = b''

        with self.assertLogs("tb.mqtt.connector.test", level="ERROR") as cm:
            handled, _ = self.connector._MqttConnector__process_connect(self.message, None)

        self.assertTrue(handled)
        self.assertIn("Device name missing from connection request", "\n".join(cm.output))
        self.connector._MqttConnector__gateway.add_device.assert_not_called()

    def test_connect_request_returns_false_when_no_handler_matches(self):
        self.connector._MqttConnector__connect_requests_sub_topics = {
            'other/topic': self.payload_connect_handler
        }
        self.message.topic = 'sensor/connect'
        self.message.payload = b'{"serialNumber":"SN-002"}'

        handled, _ = self.connector._MqttConnector__process_connect(self.message, None)
        self.assertFalse(handled)
        self.connector._MqttConnector__gateway.add_device.assert_not_called()
