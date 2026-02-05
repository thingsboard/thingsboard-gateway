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
from tests.unit.BaseUnitTest import BaseUnitTest
from simplejson import load
from thingsboard_gateway.connectors.mqtt.mqtt_connector import MqttConnector


class MqttBaseTest(BaseUnitTest):
    CONFIG_PATH = path.join(path.dirname(path.abspath(__file__)), 'data')
    DEVICE_NAME = 'SN-001'
    CONNECTOR_TYPE = 'mqtt'

    def setUp(self):
        self.connector = MqttConnector.__new__(MqttConnector)
        self.connector._connected = True
        self.connector._client = MagicMock()
        self.connector._client.is_connected.return_value = True
        self.connector._publish = MagicMock()
        self.connector._MqttConnector__log = logging.getLogger("mqtt.tests")
        self.connector._format_value = MqttConnector._format_value
        if not hasattr(self.connector, '_MqttConnector__gateway'):
            self.connector._MqttConnector__gateway = MagicMock()
        super().setUp()

    def tearDown(self):
        log = logging.getLogger('mqtt.tests')
        for handler in list(log.handlers):
            log.removeHandler(handler)
        self.connector = None
        super().tearDown()

    @staticmethod
    def convert_json(config_path):
        with open(config_path, 'r') as file:
            config = load(file)
        return config

    def extract_attribute_updates_section(self, config_path):
        config = self.convert_json(path.join(self.CONFIG_PATH, config_path))
        return config.get('requestsMapping', {}).get('attributeUpdates', [])
