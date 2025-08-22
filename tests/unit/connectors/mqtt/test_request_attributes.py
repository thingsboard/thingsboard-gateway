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

import orjson
from os import path
from unittest.mock import MagicMock, call
from tests.unit.connectors.mqtt.mqtt_base_test import MqttBaseTest
from thingsboard_gateway.connectors.mqtt.mqtt_connector import MqttConnector


class MqttOnAttributeRequestsTest(MqttBaseTest):

    def setUp(self):
        super().setUp()
        gw = self.connector._MqttConnector__gateway
        gw.tb_client.client.gw_request_shared_attributes = MagicMock()
        gw.tb_client.client.gw_request_client_attributes = MagicMock()
        token = MagicMock()
        token.wait_for_publish.return_value = None
        self.connector._client.publish.return_value = token
        self.message = MagicMock()
        self.message.topic = 'v1/devices/me/attributes/request'
        self.message.properties = []
        self.message.qos = 0
        self.single_attr_handler = self.convert_json(
            path.join(self.CONFIG_PATH, 'attribute_requests/on_attribute_request_mqtt_config_subtopics_section.json')
        )

    def tearDown(self):
        self.message = None
        super().tearDown()

    def test_request_shared_attributes(self):
        self.single_attr_handler = self.convert_json(
            path.join(self.CONFIG_PATH, 'attribute_requests/on_attribute_request_mqtt_config_subtopics_section.json')
        )
        self.connector._MqttConnector__attribute_requests_sub_topics = {
            'v1/devices/me/attributes/request': self.single_attr_handler
        }
        self.message.payload = b'{"serialNumber":"SN-002","versionAttribute":"firmwareVersion2"}'

        handled, _ = self.connector._MqttConnector__process_attribute_request(self.message, None)
        self.assertTrue(handled)

        (dev, keys,
         cb), _ = self.connector._MqttConnector__gateway.tb_client.client.gw_request_shared_attributes.call_args
        self.assertEqual(dev, "SN-002")
        self.assertEqual(keys, ["firmwareVersion2"])
        self.assertTrue(callable(cb))
        cb({"device": "SN-002", "value": 7.1})
        self.connector._client.publish.assert_called_with(
            "devices/SN-002/attrs",
            '{"firmwareVersion2": 7.1}',
            qos=0,
            retain=False
        )

    def test_request_shared_attributes_on_multiple_attributes(self):
        self.multi_attr_handler = self.convert_json(
            path.join(self.CONFIG_PATH,
                      'attribute_requests/on_attribute_request_mqtt_config_subtopic_multiple_attributes.json')
        )
        self.connector._MqttConnector__attribute_requests_sub_topics = {
            'v1/devices/me/attributes/request': self.multi_attr_handler
        }
        self.message.payload = b'{"serialNumber":"SN-002","versionAttribute":"firmwareVersion2","versionModel":"model3"}'

        handled, _ = self.connector._MqttConnector__process_attribute_request(self.message, None)
        self.assertTrue(handled)

        (dev, keys,
         cb), _ = self.connector._MqttConnector__gateway.tb_client.client.gw_request_shared_attributes.call_args
        self.assertEqual(dev, "SN-002")
        self.assertEqual(keys, ["firmwareVersion2", "model3"])

        values = {"firmwareVersion2": 7.1, "model3": "M-123"}
        cb({"device": "SN-002", "values": values})
        expected_payload = orjson.dumps(values)
        self.connector._client.publish.assert_called_with(
            "devices/SN-002/attrs",
            expected_payload,
            qos=0,
            retain=False
        )

    def test_request_client_side_attributes(self):
        pass

    def test_client_scope_routes_to_client_attributes_api_and_publishes(self):
        """
        Same as single-key happy path but with 'client' scope in handler -> routes to client attributes API.
        """
        handler = dict(self.single_attr_handler)
        handler["scope"] = "client"
        self.connector._MqttConnector__attribute_requests_sub_topics = {
            'v1/devices/me/attributes/request': handler
        }
        self.message.payload = b'{"serialNumber":"SN-002","versionAttribute":"firmwareVersion2"}'

        handled, _ = self.connector._MqttConnector__process_attribute_request(self.message, None)
        self.assertTrue(handled)

        (dev, keys,
         cb), _ = self.connector._MqttConnector__gateway.tb_client.client.gw_request_client_attributes.call_args
        self.assertEqual(dev, "SN-002")
        self.assertEqual(keys, ["firmwareVersion2"])

        cb({"device": "SN-002", "value": 5})
        self.connector._client.publish.assert_called_with(
            "devices/SN-002/attrs",
            '{"firmwareVersion2": 5}',
            qos=0,
            retain=False
        )

    def test_missing_device_name_logs_error_and_skips_request(self):
        self.connector._MqttConnector__attribute_requests_sub_topics = {
            'v1/devices/me/attributes/request': self.single_attr_handler
        }
        self.connector._MqttConnector__log = MagicMock()
        self.message.payload = b'{"versionAttribute":"firmwareVersion2", "serialNumber":"invalid-device"}'
        with self.assertLogs(level="ERROR") as log:
            handled = self.connector._MqttConnector__process_attribute_request(self.message, None)
        self.assertTrue(handled)

        self.connector._MqttConnector__gateway.tb_client.client.gw_request_shared_attributes.assert_not_called()
        self.connector._MqttConnector__gateway.tb_client.client.gw_request_client_attributes.assert_not_called()
        self.connector._client.publish.assert_not_called()