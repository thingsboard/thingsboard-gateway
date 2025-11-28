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

from time import sleep
from tests.blackbox.connectors.opcua.test_base_opcua import BaseOpcuaTest


class OpcuaAttributesUpdatesTest(BaseOpcuaTest):

    def setUp(self):
        super(OpcuaAttributesUpdatesTest, self).setUp()
        sleep(self.GENERAL_TIMEOUT * 2)

    def test_attributes_update_full_path(self):
        self.update_device_and_connector_shared_attributes(
            'configs/attrs_update_configs/opcua_attribute_updates_full_path.json',
            'test_values/attrs_update/opcua_full_path_attributes_update_values.json')
        sleep(self.GENERAL_TIMEOUT * 2)
        expected_values = self.load_configuration(
            self.CONFIG_PATH + 'test_values/attrs_update/opcua_full_path_attributes_update_values.json')
        actual_values = self.client.get_latest_timeseries(self.device.id,
                                                          ','.join([key for (key, _) in expected_values.items()]))
        for (key, value) in expected_values.items():
            self.assertEqual(value, actual_values[key][0]['value'],
                             f'Value is not equal for the next telemetry key: {key}')
        self.reset_node_default_values(
            path_to_default_values='test_values/attrs_update/opcua_paths_default_values.json')

    def test_attributes_update_relative_path(self):
        self.update_device_and_connector_shared_attributes(
            'configs/attrs_update_configs/opcua_attribute_updates_relative_path.json',
            'test_values/attrs_update/opcua_relative_path_attributes_update_values.json')
        sleep(self.GENERAL_TIMEOUT * 2)
        expected_values = self.load_configuration(
            self.CONFIG_PATH + 'test_values/attrs_update/opcua_relative_path_attributes_update_values.json')
        actual_values = self.client.get_latest_timeseries(self.device.id,
                                                          ','.join([key for (key, _) in expected_values.items()]))
        for (key, value) in expected_values.items():
            self.assertEqual(value, actual_values[key][0]['value'],
                             f'Value is not equal for the next telemetry key: {key}')
        self.reset_node_default_values(
            path_to_default_values='test_values/attrs_update/opcua_paths_default_values.json')

    def test_attributes_update_identifiers(self):
        self.update_device_and_connector_shared_attributes(
            'configs/attrs_update_configs/opcua_attribute_updates_identifier.json',
            'test_values/attrs_update/opcua_identifier_attributes_update_values.json')
        sleep(self.GENERAL_TIMEOUT * 2)
        expected_values = self.load_configuration(
            self.CONFIG_PATH + 'test_values/attrs_update/opcua_identifier_attributes_update_values.json')
        actual_values = self.client.get_latest_timeseries(self.device.id,
                                                          ','.join([key for (key, _) in expected_values.items()]))
        for (key, value) in expected_values.items():
            self.assertEqual(value, actual_values[key][0]['value'],
                             f'Value is not equal for the next telemetry key: {key}')
        self.reset_node_default_values(
            path_to_default_values='test_values/attrs_update/opcua_identifier_default_values.json')

    def test_attributes_update_different_types(self):
        self.update_device_and_connector_shared_attributes(
            'configs/attrs_update_configs/opcua_attribute_updates_different_types.json',
            'test_values/attrs_update/opcua_different_types_attributes_update_values.json')
        sleep(self.GENERAL_TIMEOUT * 2)
        expected_values = self.load_configuration(
            self.CONFIG_PATH + 'test_values/attrs_update/opcua_different_types_attributes_update_values.json')
        actual_values = self.client.get_latest_timeseries(self.device.id,
                                                          ','.join([key for (key, _) in expected_values.items()]))
        for (key, value) in expected_values.items():
            self.assertEqual(value, actual_values[key][0]['value'],
                             f'Value is not equal for the next telemetry key: {key}')
        self.reset_node_default_values(
            path_to_default_values='test_values/attrs_update/opcua_different_types_attributes_update_default_values.json')
