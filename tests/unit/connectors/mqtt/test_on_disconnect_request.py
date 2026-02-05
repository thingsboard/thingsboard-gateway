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
from os import path
from unittest.mock import MagicMock

from tests.unit.connectors.mqtt.mqtt_base_test import MqttBaseTest


class MqttOnDisconnectRequestTest(MqttBaseTest):
    def setUp(self):
        super().setUp()
        gw = self.connector._MqttConnector__gateway
        gw.del_device = MagicMock()
        gw.get_devices = MagicMock(return_value=set())
        self.logger = logging.getLogger("tb.mqtt.connector.test")
        self.connector._MqttConnector__log = self.logger
        self.message = MagicMock()
        self.message.properties = []
        self.message.qos = 0
        self.payload_disconnect_handler = self.convert_json(
            path.join(self.CONFIG_PATH, 'disconnect_requests/on_disconnect_request_mqtt_config.json')
        )

    def tearDown(self):
        self.message = None
        super().tearDown()

    def test_disconnect_request_with_device_name_in_payload(self):
        self.connector._MqttConnector__disconnect_requests_sub_topics = {
            'sensor/disconnect': self.payload_disconnect_handler
        }
        self.message.topic = 'sensor/disconnect'
        self.message.payload = b'{"serialNumber":"SN-002"}'
        self.connector._MqttConnector__gateway.get_devices.return_value = {"SN-002", "SN-003"}

        handled, _ = self.connector._MqttConnector__process_disconnect(self.message, None)
        self.assertTrue(handled)
        self.assertTrue(self.connector._MqttConnector__gateway.del_device.called)
        args, kwargs = self.connector._MqttConnector__gateway.del_device.call_args
        self.assertEqual(args[0], "SN-002")
        with self.assertLogs("tb.mqtt.connector.test", level="INFO") as cm:
            self.logger.info("Disconnecting device %s of type %s", "SN-002", "Thermometer")

    def test_disconnect_request_with_device_name_from_topic(self):
        self.payload_disconnect_handler = self.convert_json(
            path.join(self.CONFIG_PATH, 'disconnect_requests/on_disconnect_request_mqtt_config_topic_section.json')
        )

        self.connector._MqttConnector__disconnect_requests_sub_topics = {
            'sensor/[^/]+/disconnect': self.payload_disconnect_handler
        }
        self.message.topic = 'sensor/SN-001/disconnect'
        self.message.payload = b''
        self.connector._MqttConnector__gateway.get_devices.return_value = {"SN-001"}

        handled, _ = self.connector._MqttConnector__process_disconnect(self.message, None)
        self.assertTrue(handled)

        self.connector._MqttConnector__gateway.del_device.assert_called_once()
        args, kwargs = self.connector._MqttConnector__gateway.del_device.call_args
        self.assertEqual(args[0], "SN-001")

    def test_disconnect_request_logs_error_when_device_name_missing(self):
        self.connector._MqttConnector__disconnect_requests_sub_topics = {
            'sensor/disconnect': self.payload_disconnect_handler
        }
        self.message.topic = 'sensor/disconnect'
        self.message.payload = b''

        with self.assertLogs("tb.mqtt.connector.test", level="ERROR") as cm:
            handled, _ = self.connector._MqttConnector__process_disconnect(self.message, None)

        self.assertTrue(handled)
        self.assertIn("Device name missing from disconnection request", "\n".join(cm.output))
        self.connector._MqttConnector__gateway.del_device.assert_not_called()

    def test_disconnect_request_logs_info_when_device_was_not_connected(self):
        self.connector._MqttConnector__disconnect_requests_sub_topics = {
            'sensor/disconnect': self.payload_disconnect_handler
        }
        self.message.topic = 'sensor/disconnect'
        self.message.payload = b'{"serialNumber":"SN-404"}'
        self.connector._MqttConnector__gateway.get_devices.return_value = {"SN-001", "SN-002"}

        with self.assertLogs("tb.mqtt.connector.test", level="INFO") as cm:
            handled, _ = self.connector._MqttConnector__process_disconnect(self.message, None)

        self.assertTrue(handled)
        self.assertIn("Device SN-404 was not connected", "\n".join(cm.output))
        self.connector._MqttConnector__gateway.del_device.assert_not_called()

    def test_disconnect_request_returns_false_when_no_handler_matches(self):
        self.connector._MqttConnector__disconnect_requests_sub_topics = {
            'other/topic': self.payload_disconnect_handler
        }
        self.message.topic = 'sensor/disconnect'
        self.message.payload = b'{"serialNumber":"SN-002"}'

        handled, _ = self.connector._MqttConnector__process_disconnect(self.message, None)
        self.assertFalse(handled)
        self.connector._MqttConnector__gateway.del_device.assert_not_called()
