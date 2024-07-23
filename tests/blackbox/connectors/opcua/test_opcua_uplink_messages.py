import unittest
from time import sleep, time
from unittest import skip

from tests.blackbox.connectors.opcua.test_base_opcua import BaseOpcuaTest
from tests.test_utils.gateway_device_util import GatewayDeviceUtil


class OpcuaAsyncioUplinkMessagesTest(BaseOpcuaTest):
    def test_gateway_connection(self):
        self.assertEqual(self.client.get_attributes_by_scope(self.gateway.id, 'SERVER_SCOPE', 'active')[0]['value'],
                         True)

    @skip('Unimplemented')
    def test_reading_from_views(self):
        config = self.load_configuration(
            self.CONFIG_PATH + 'configs/uplink_configs/reading_from_views_config.json')
        GatewayDeviceUtil.update_connector_config(self.CONNECTOR_NAME, config)
        sleep(5)
        telemetry_keys = [key['key'] for node in config['Opcua']['configurationJson']['server']['mapping'] for key in
                          node['timeseries']]
        actual_values = self.client.get_latest_timeseries(self.device.id, ','.join(telemetry_keys))
        expected_values = self.load_configuration(
            self.CONFIG_PATH + 'test_values/uplink/reading_from_views_values.json')

        for (_type, value) in expected_values.items():
            self.assertEqual(value, actual_values[_type][0]['value'],
                             f'Value is not equal for the next telemetry key: {_type}')

    @skip('Unimplemented')
    def test_reading_from_variable_in_the_variable(self):
        config = self.load_configuration(
            self.CONFIG_PATH + 'configs/uplink_configs/reading_from_variable_in_the_variable_config.json')
        GatewayDeviceUtil.update_connector_config(self.CONNECTOR_NAME, config)
        sleep(5)
        telemetry_keys = [key['key'] for node in config['Opcua']['configurationJson']['server']['mapping'] for key in
                          node['timeseries']]
        actual_values = self.client.get_latest_timeseries(self.device.id, ','.join(telemetry_keys))
        expected_values = self.load_configuration(
            self.CONFIG_PATH + 'test_values/uplink/reading_from_variable_in_the_variable_values.json')

        for (_type, value) in expected_values.items():
            self.assertEqual(value, actual_values[_type][0]['value'],
                             f'Value is not equal for the next telemetry key: {_type}')

    def test_gateway_restarted(self):
        sleep(8)
        GatewayDeviceUtil.restart_gateway()
        restarted_time = time() * 1000
        config = self.load_configuration(
            self.CONFIG_PATH + 'configs/uplink_configs/different_node_finding_methods_config_path.json')
        GatewayDeviceUtil.update_connector_config(
            self.CONNECTOR_NAME,
            self.CONFIG_PATH + 'configs/uplink_configs/different_node_finding_methods_config_path.json')
        sleep(self.GENERAL_TIMEOUT)
        telemetry_keys = [key['key'] for node in config['Opcua']['configurationJson']['server']['mapping'] for
                          key in node['timeseries']]
        actual_values = self.client.get_latest_timeseries(self.device.id, ','.join(telemetry_keys))
        for actual_value in actual_values.values():
            self.assertGreater(actual_value[0]['ts'], restarted_time)

    def test_sending_attrs_and_telemetries_after_connection_lost(self):
        GatewayDeviceUtil.update_credentials({"credentialsType": "ACCESS_TOKEN",
                                              "credentialsId": "SOME_ACCESS_TOKEN"})
        sleep(self.GENERAL_TIMEOUT)

        connection_lost_start_time = time() * 1000

        GatewayDeviceUtil.update_credentials({"credentialsType": "ACCESS_TOKEN",
                                              "credentialsId": "YOUR_ACCESS_TOKEN"})
        sleep(self.GENERAL_TIMEOUT)

        config = self.load_configuration(
            self.CONFIG_PATH + 'configs/uplink_configs/different_node_finding_methods_config_path.json')
        GatewayDeviceUtil.update_connector_config(
            self.CONNECTOR_NAME,
            self.CONFIG_PATH + 'configs/uplink_configs/different_node_finding_methods_config_path.json')
        sleep(self.GENERAL_TIMEOUT)
        telemetry_keys = [key['key'] for node in config['Opcua']['configurationJson']['server']['mapping'] for key in
                          node['timeseries']]
        actual_values = self.client.get_latest_timeseries(self.device.id, ','.join(telemetry_keys))
        for actual_value in actual_values.values():
            self.assertGreater(actual_value[0]['ts'], connection_lost_start_time)

    def test_different_node_finding_methods_path(self):
        config = self.load_configuration(
            self.CONFIG_PATH + 'configs/uplink_configs/different_node_finding_methods_config_path.json')
        GatewayDeviceUtil.update_connector_config(
            self.CONNECTOR_NAME,
            self.CONFIG_PATH + 'configs/uplink_configs/different_node_finding_methods_config_path.json')
        sleep(5)
        telemetry_keys = [key['key'] for node in config[self.CONNECTOR_NAME]['configurationJson']['server']['mapping']
                          for key in node['timeseries']]
        actual_values = self.client.get_latest_timeseries(self.device.id, ','.join(telemetry_keys))
        expected_values = self.load_configuration(
            self.CONFIG_PATH + 'test_values/uplink/different_node_finding_methods_values.json')

        for (_type, value) in expected_values.items():
            self.assertEqual(value, actual_values[_type][0]['value'],
                             f'Value is not equal for the next telemetry key: {_type}')

    def test_different_node_finding_methods_i(self):
        config = self.load_configuration(
            self.CONFIG_PATH + 'configs/uplink_configs/different_node_finding_methods_config_i.json')
        GatewayDeviceUtil.update_connector_config(
            self.CONNECTOR_NAME,
            self.CONFIG_PATH + 'configs/uplink_configs/different_node_finding_methods_config_i.json')
        sleep(5)
        telemetry_keys = [key['key'] for node in config['Opcua']['configurationJson']['server']['mapping'] for key in
                          node['timeseries']]
        actual_values = self.client.get_latest_timeseries(self.device.id, ','.join(telemetry_keys))
        expected_values = self.load_configuration(
            self.CONFIG_PATH + 'test_values/uplink/different_node_finding_methods_values.json')

        for (_type, value) in expected_values.items():
            self.assertEqual(value, actual_values[_type][0]['value'],
                             f'Value is not equal for the next telemetry key: {_type}')

    def test_different_node_finding_methods_s(self):
        config = self.load_configuration(
            self.CONFIG_PATH + 'configs/uplink_configs/different_node_finding_methods_config_s.json')
        GatewayDeviceUtil.update_connector_config(
            self.CONNECTOR_NAME,
            self.CONFIG_PATH + 'configs/uplink_configs/different_node_finding_methods_config_s.json')
        sleep(5)
        telemetry_keys = [key['key'] for node in config['Opcua']['configurationJson']['server']['mapping'] for key in
                          node['timeseries']]
        actual_values = self.client.get_latest_timeseries(self.device.id, ','.join(telemetry_keys))
        expected_values = self.load_configuration(
            self.CONFIG_PATH + 'test_values/uplink/different_node_finding_methods_values.json')

        for (_type, value) in expected_values.items():
            self.assertEqual(value, actual_values[_type][0]['value'],
                             f'Value is not equal for the next telemetry key: {_type}')

    def test_different_node_finding_methods_g(self):
        config = self.load_configuration(
            self.CONFIG_PATH + 'configs/uplink_configs/different_node_finding_methods_config_g.json')
        GatewayDeviceUtil.update_connector_config(
            self.CONNECTOR_NAME,
            self.CONFIG_PATH + 'configs/uplink_configs/different_node_finding_methods_config_g.json')
        sleep(5)
        telemetry_keys = [key['key'] for node in config['Opcua']['configurationJson']['server']['mapping'] for key in
                          node['timeseries']]
        actual_values = self.client.get_latest_timeseries(self.device.id, ','.join(telemetry_keys))
        expected_values = self.load_configuration(
            self.CONFIG_PATH + 'test_values/uplink/different_node_finding_methods_values.json')

        for (_type, value) in expected_values.items():
            self.assertEqual(value, actual_values[_type][0]['value'],
                             f'Value is not equal for the next telemetry key: {_type}')

    def test_different_node_finding_methods_b(self):
        config = self.load_configuration(
            self.CONFIG_PATH + 'configs/uplink_configs/different_node_finding_methods_config_b.json')
        GatewayDeviceUtil.update_connector_config(
            self.CONNECTOR_NAME,
            self.CONFIG_PATH + 'configs/uplink_configs/different_node_finding_methods_config_b.json')
        sleep(5)
        telemetry_keys = [key['key'] for node in config['Opcua']['configurationJson']['server']['mapping'] for key in
                          node['timeseries']]
        actual_values = self.client.get_latest_timeseries(self.device.id, ','.join(telemetry_keys))
        expected_values = self.load_configuration(
            self.CONFIG_PATH + 'test_values/uplink/different_node_finding_methods_values.json')

        for (_type, value) in expected_values.items():
            self.assertEqual(value, actual_values[_type][0]['value'],
                             f'Value is not equal for the next telemetry key: {_type}')


if __name__ == '__main__':
    unittest.main()
