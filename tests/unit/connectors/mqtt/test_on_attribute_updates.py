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

from unittest.mock import MagicMock, call
from tests.unit.connectors.mqtt.mqtt_base_test import MqttBaseTest


class MqttOnAttributeUpdatesTest(MqttBaseTest):

    def setUp(self):
        super().setUp()
        self.connector._publish = MagicMock()
        self.connector._MqttConnector__attribute_updates = self.extract_attribute_updates_section(
            'attribute_updates/on_attribute_update_mqtt_config.json')

    def tearDown(self):
        super().tearDown()

    def test_errors_when_attribute_updates_config_missing(self):
        self.connector._MqttConnector__attribute_updates = self.extract_attribute_updates_section(
            'attribute_updates/on_attributes_update_empty_section.json')
        content = {'data': {'firmwareVersion': 5.8}, 'device': 'SN-002'}
        with self.assertLogs(level="ERROR") as log:
            self.connector.on_attributes_update(content)
        self.connector._publish.assert_not_called()
        self.assertIn("Attributes update config not found.", log.output[0])

    def test_skips_when_device_name_filter_does_not_match(self):
        content = {'data': {'firmwareVersion': 6.4}, 'device': 'SN-003'}
        self.connector._MqttConnector__attribute_updates = self.extract_attribute_updates_section(
            'attribute_updates/on_attributes_updates_mqtt_config_filter_no_match.json')
        with self.assertLogs(level="ERROR") as log:
            self.connector.on_attributes_update(content)
        self.connector._publish.assert_not_called()
        self.assertIn("Cannot find deviceName by filter", log.output[0])

    def test_publishes_single_matching_attribute_with_expected_topic_and_payload(self):
        content = {'data': {'firmwareVersion': 6.6}, 'device': 'SN-002'}
        with self.assertLogs(level="DEBUG") as log:
            self.connector.on_attributes_update(content)
        expected_topic = 'sensor/SN-002/firmwareVersion'
        expected_data = '{"firmwareVersion":"6.6"}'
        self.connector._publish.assert_called_once_with(expected_topic, expected_data, True, 0)
        self.assertIn("Got attributes update: {'data': {'firmwareVersion': 6.6}, 'device': 'SN-002'}", log.output[0])

    def test_errors_when_required_fields_missing_in_attribute_update_rule(self):
        content = {'data': {'firmwareVersion': 6.6}, 'device': 'SN-002'}
        self.connector._MqttConnector__attribute_updates = self.extract_attribute_updates_section(
            'attribute_updates/on_attribute_updates_no_required_field.json')
        with self.assertLogs(level="ERROR") as log:
            self.connector.on_attributes_update(content)

        self.connector._publish.assert_not_called()
        self.assertIn(
            "Cannot find topicExpression or valueExpression or attributeFilter in attribute update configuration",
            log.output[0])

    def test_publishes_for_multiple_matching_attribute_update_rules(self):
        content = {'data': {'firmwareVersion2': 7.1, 'model3': 'M-123'}, 'device': 'SN-002'}
        self.connector._MqttConnector__attribute_updates = self.extract_attribute_updates_section(
            'attribute_updates/on_attribute_updates_multiple_attributes_section.json')

        self.connector.on_attributes_update(content)
        expected_calls = [
            call("sensor/SN-002/firmwareVersion", "{\"firmwareVersion2\":\"7.1\"}", True, 0),
            call("sensor/SN-002/model", "{\"model3\":\"M-123\"}", True, 0)

        ]
        self.connector._publish.assert_has_calls(expected_calls, any_order=True)
        self.assertEqual(self.connector._publish.call_count, 2)

    def test_continues_after_nonmatching_attribute_and_publishes_remaining_for_device(self):
        content = {'data': {'model': 6.9, 'firmwareVersion2': 7.1}, 'device': 'SN-002'}
        self.connector._MqttConnector__log = MagicMock()

        self.connector.on_attributes_update(content)
        self.connector._publish.assert_called_once_with(
            "sensor/SN-002/firmwareVersion",
            "{\"firmwareVersion2\":\"7.1\"}",
            True,
            0
        )
        self.connector._MqttConnector__log.error.assert_any_call(
            "Attribute %s does not match filter %s, skipping",
            "model",
            r"^firmwareVersion\.*"
        )

    def test_logs_exception_when_publish_raises_keyerro(self):
        self.connector._publish.side_effect = KeyError("boom")
        self.connector._MqttConnector__log = MagicMock()
        content = {'data': {'firmwareVersion2': 7.1}, 'device': 'SN-002'}
        self.connector.on_attributes_update(content)
        self.assertEqual(self.connector._publish.call_count, 1)
        self.connector._MqttConnector__log.exception.assert_any_call(
            "Cannot form topic/value for attribute configuration section %s",
            self.connector._MqttConnector__attribute_updates[0]
        )
