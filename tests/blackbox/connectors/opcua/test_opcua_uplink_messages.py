import unittest
from time import sleep, time

from tests.blackbox.connectors.opcua.test_base_opcua import BaseOpcuaTest
from tests.test_utils.gateway_device_util import GatewayDeviceUtil


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


if __name__ == '__main__':
    unittest.main()
