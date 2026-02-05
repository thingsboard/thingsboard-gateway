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

import unittest
from time import sleep
from tests.blackbox.connectors.opcua.test_base_opcua import BaseOpcuaTest


class OpcUaUplinkMessagesTest(BaseOpcuaTest):

    def test_opcua_absolute_path(self):
        (config, _) = self.change_connector_configuration(
            self.CONFIG_PATH + 'configs/uplink_configs/opcua_uplink_converter_absolute_path.json')
        telemetry_keys = [
            key['key']
            for conf_list in config[self.CONNECTOR_NAME]['configurationJson']['mapping']
            for key in conf_list['timeseries']
        ]
        sleep(self.GENERAL_TIMEOUT)
        actual_values = self.client.get_latest_timeseries(self.device.id, ','.join(telemetry_keys))
        expected_values = self.load_configuration(
            self.CONFIG_PATH + 'test_values/uplink/different_paths_finding_methods_values.json'
        )
        for (_type, value) in expected_values.items():
            self.assertEqual(value, actual_values[_type][0]['value'],
                             f'Value is not equal for the next telemetry key: {_type}')

    def test_opcua_relative_path(self):
        (config, _) = self.change_connector_configuration(
            self.CONFIG_PATH + 'configs/uplink_configs/opcua_uplink_converter_relative_path.json')
        telemetry_keys = [
            key['key']
            for conf_list in config[self.CONNECTOR_NAME]['configurationJson']['mapping']
            for key in conf_list['timeseries']
        ]
        sleep(self.GENERAL_TIMEOUT)
        actual_values = self.client.get_latest_timeseries(self.device.id, ','.join(telemetry_keys))
        expected_values = self.load_configuration(
            self.CONFIG_PATH + 'test_values/uplink/different_paths_finding_methods_values.json'
        )
        for (_type, value) in expected_values.items():
            self.assertEqual(value, actual_values[_type][0]['value'],
                             f'Value is not equal for the next telemetry key: {_type}')

    def test_opcua_with_different_identifier_types(self):
        (config, _) = self.change_connector_configuration(
            self.CONFIG_PATH + 'configs/uplink_configs/opcua_uplink_converter_identifier.json')
        telemetry_keys = [
            key['key']
            for conf_list in config[self.CONNECTOR_NAME]['configurationJson']['mapping']
            for key in conf_list['timeseries']
        ]
        sleep(self.GENERAL_TIMEOUT)
        actual_values = self.client.get_latest_timeseries(self.device.id, ','.join(telemetry_keys))
        expected_values = self.load_configuration(
            self.CONFIG_PATH + 'test_values/uplink/different_identifier_finding_methods_values.json'
        )
        for (_type, value) in expected_values.items():
            self.assertEqual(value, actual_values[_type][0]['value'],
                             f'Value is not equal for the next telemetry key: {_type}')

    def test_opcua_with_different_types(self):
        (config, _) = self.change_connector_configuration(
            self.CONFIG_PATH + 'configs/uplink_configs/opcua_uplink_converter_different_types.json')
        telemetry_keys = [
            key['key']
            for conf_list in config[self.CONNECTOR_NAME]['configurationJson']['mapping']
            for key in conf_list['timeseries']
        ]
        sleep(self.GENERAL_TIMEOUT)
        actual_values = self.client.get_latest_timeseries(self.device.id, ','.join(telemetry_keys))
        expected_values = self.load_configuration(
            self.CONFIG_PATH + 'test_values/uplink/different_types_finding_methods_values.json'
        )
        for (_type, value) in expected_values.items():
            self.assertEqual(value, actual_values[_type][0]['value'],
                             f'Value is not equal for the next telemetry key: {_type}')

    def test_opcua_with_relative_path_and_identifier_as_device_node(self):
        (config, _) = self.change_connector_configuration(
            self.CONFIG_PATH + 'configs/uplink_configs/opcua_uplink_converter_relative_path_with_identifier_device_node.json')
        telemetry_keys = [
            key['key']
            for conf_list in config[self.CONNECTOR_NAME]['configurationJson']['mapping']
            for key in conf_list['timeseries']
        ]
        sleep(self.GENERAL_TIMEOUT)
        actual_values = self.client.get_latest_timeseries(self.device.id, ','.join(telemetry_keys))
        expected_values = self.load_configuration(
            self.CONFIG_PATH + 'test_values/uplink/different_paths_finding_methods_values.json'
        )
        for (_type, value) in expected_values.items():
            self.assertEqual(value, actual_values[_type][0]['value'],
                             f'Value is not equal for the next telemetry key: {_type}')


if __name__ == '__main__':
    unittest.main()
