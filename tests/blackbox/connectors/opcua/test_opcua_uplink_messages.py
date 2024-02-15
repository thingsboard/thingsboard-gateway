import unittest
from time import sleep, time

from tb_rest_client.models.models_ce import DeviceCredentials

from tests.blackbox.connectors.opcua.test_base_opcua import BaseOpcuaTest
from tests.test_utils.gateway_device_util import GatewayDeviceUtil


class OpcuaUplinkMessagesTest(BaseOpcuaTest):
    def test_gateway_connection(self):
        """
        Test case checks if the gateway connection is active.

        It fetches the 'active' attribute of the gateway from the server scope and asserts that its value is True.
        If the value is not True, the test case will fail indicating that the gateway connection is not active.
        """

        self.assertEqual(self.client.get_attributes_by_scope(self.gateway.id, 'SERVER_SCOPE', 'active')[0]['value'],
                         True)

    def test_reading_from_views(self):
        """
        This test case checks if the readings from the views are as expected.

        It first loads the configuration for the views from a JSON file and updates the connector configuration.
        After a delay of 3 seconds, it fetches the telemetry keys from the configuration and gets the latest timeseries
        data for these keys from the client.

        It then loads the expected values from a JSON file and compares these with the actual values fetched from
        the client.
        If the actual value for any telemetry key does not match the expected value, the test case will fail with
        a message indicating the key for which the value did not match.
        """

        config = self.load_configuration(
            self.CONFIG_PATH + 'configs/uplink_configs/reading_from_views_config.json')
        GatewayDeviceUtil.update_connector_config(self.CONNECTOR_NAME, config)
        sleep(3)
        telemetry_keys = [key['key'] for node in config['Opcua']['configurationJson']['mapping'] for key in
                          node['timeseries']]
        actual_values = self.client.get_latest_timeseries(self.device.id, ','.join(telemetry_keys))
        expected_values = self.load_configuration(
            self.CONFIG_PATH + 'test_values/uplink/reading_from_views_values.json')

        for (_type, value) in expected_values.items():
            self.assertEqual(value, actual_values[_type][0]['value'],
                             f'Value is not equal for the next telemetry key: {_type}')

    def test_reading_from_variable_in_the_variable(self):
        config = self.load_configuration(
            self.CONFIG_PATH + 'configs/uplink_configs/reading_from_variable_in_the_variable_config.json')
        GatewayDeviceUtil.update_connector_config(self.CONNECTOR_NAME, config)
        sleep(3)
        telemetry_keys = [key['key'] for node in config['Opcua']['configurationJson']['mapping'] for key in
                          node['timeseries']]
        actual_values = self.client.get_latest_timeseries(self.device.id, ','.join(telemetry_keys))
        expected_values = self.load_configuration(
            self.CONFIG_PATH + 'test_values/uplink/reading_from_variable_in_the_variable_values.json')

        for (_type, value) in expected_values.items():
            self.assertEqual(value, actual_values[_type][0]['value'],
                             f'Value is not equal for the next telemetry key: {_type}')

    def test_gateway_restarted(self):
        GatewayDeviceUtil.restart_gateway()
        restarted_time = time() * 1000
        config = self.load_configuration(
            self.CONFIG_PATH + 'configs/uplink_configs/different_node_finding_methods_config.json')
        GatewayDeviceUtil.update_connector_config(self.CONNECTOR_NAME, config)
        sleep(3)
        telemetry_keys = [key['key'] for node in config['Opcua']['configurationJson']['mapping'] for key in
                          node['timeseries']]
        actual_values = self.client.get_latest_timeseries(self.device.id, ','.join(telemetry_keys))
        for actual_value in actual_values.values():
            self.assertGreater(actual_value[0]['ts'], restarted_time)

    def test_sending_attrs_and_telemetries_after_connection_lost(self):
        self.client.update_device_credentials(DeviceCredentials())
        sleep(5)
        connection_lost_start_time = time() * 1000

        self.client.update_device_credentials(DeviceCredentials())
        sleep(5)

        config = self.load_configuration(self.CONFIG_PATH + 'configs/opcua_base_config.json')
        GatewayDeviceUtil.update_connector_config(self.CONNECTOR_NAME, config)
        sleep(2)

        telemetry_keys = [key['key'] for node in config['Opcua']['configurationJson']['mapping'] for key in
                          node['timeseries']]
        actual_values = self.client.get_latest_timeseries(self.device.id, ','.join(telemetry_keys))
        for actual_value in actual_values.values():
            self.assertGreater(actual_value[0]['ts'], connection_lost_start_time)

    def test_different_node_finding_methods_path(self):
        config = self.load_configuration(
            self.CONFIG_PATH + 'configs/uplink_configs/different_node_finding_methods_config_path.json')
        GatewayDeviceUtil.update_connector_config(self.CONNECTOR_NAME, config)
        sleep(3)
        telemetry_keys = [key['key'] for node in config['Opcua']['configurationJson']['mapping'] for key in
                          node['timeseries']]
        actual_values = self.client.get_latest_timeseries(self.device.id, ','.join(telemetry_keys))
        expected_values = self.load_configuration(
            self.CONFIG_PATH + 'test_values/uplink/different_node_finding_methods_values.json')

        for (_type, value) in expected_values.items():
            self.assertEqual(value, actual_values[_type][0]['value'],
                             f'Value is not equal for the next telemetry key: {_type}')

    def test_different_node_finding_methods_i(self):
        config = self.load_configuration(
            self.CONFIG_PATH + 'configs/uplink_configs/different_node_finding_methods_config_i.json')
        GatewayDeviceUtil.update_connector_config(self.CONNECTOR_NAME, config)
        sleep(3)
        telemetry_keys = [key['key'] for node in config['Opcua']['configurationJson']['mapping'] for key in
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
        GatewayDeviceUtil.update_connector_config(self.CONNECTOR_NAME, config)
        sleep(3)
        telemetry_keys = [key['key'] for node in config['Opcua']['configurationJson']['mapping'] for key in
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
        GatewayDeviceUtil.update_connector_config(self.CONNECTOR_NAME, config)
        sleep(3)
        telemetry_keys = [key['key'] for node in config['Opcua']['configurationJson']['mapping'] for key in
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
        GatewayDeviceUtil.update_connector_config(self.CONNECTOR_NAME, config)
        sleep(3)
        telemetry_keys = [key['key'] for node in config['Opcua']['configurationJson']['mapping'] for key in
                          node['timeseries']]
        actual_values = self.client.get_latest_timeseries(self.device.id, ','.join(telemetry_keys))
        expected_values = self.load_configuration(
            self.CONFIG_PATH + 'test_values/uplink/different_node_finding_methods_values.json')

        for (_type, value) in expected_values.items():
            self.assertEqual(value, actual_values[_type][0]['value'],
                             f'Value is not equal for the next telemetry key: {_type}')


class OpcuaAsyncioUplinkMessagesTest(BaseOpcuaTest):
    def test_gateway_connection(self):
        self.assertEqual(self.client.get_attributes_by_scope(self.gateway.id, 'SERVER_SCOPE', 'active')[0]['value'],
                         True)

    def test_reading_from_views(self):
        config = self.load_configuration(
            self.CONFIG_PATH + 'configs/uplink_configs/reading_from_views_config.json')
        GatewayDeviceUtil.update_connector_config(self.CONNECTOR_NAME, config, connector_type='opcua_asyncio')
        sleep(3)
        telemetry_keys = [key['key'] for node in config['Opcua']['configurationJson']['mapping'] for key in
                          node['timeseries']]
        actual_values = self.client.get_latest_timeseries(self.device.id, ','.join(telemetry_keys))
        expected_values = self.load_configuration(
            self.CONFIG_PATH + 'test_values/uplink/reading_from_views_values.json')

        for (_type, value) in expected_values.items():
            self.assertEqual(value, actual_values[_type][0]['value'],
                             f'Value is not equal for the next telemetry key: {_type}')

    def test_reading_from_variable_in_the_variable(self):
        config = self.load_configuration(
            self.CONFIG_PATH + 'configs/uplink_configs/reading_from_variable_in_the_variable_config.json')
        GatewayDeviceUtil.update_connector_config(self.CONNECTOR_NAME, config, connector_type='opcua_asyncio')
        sleep(3)
        telemetry_keys = [key['key'] for node in config['Opcua']['configurationJson']['mapping'] for key in
                          node['timeseries']]
        actual_values = self.client.get_latest_timeseries(self.device.id, ','.join(telemetry_keys))
        expected_values = self.load_configuration(
            self.CONFIG_PATH + 'test_values/uplink/reading_from_variable_in_the_variable_values.json')

        for (_type, value) in expected_values.items():
            self.assertEqual(value, actual_values[_type][0]['value'],
                             f'Value is not equal for the next telemetry key: {_type}')

    def test_gateway_restarted(self):
        GatewayDeviceUtil.restart_gateway()
        restarted_time = time() * 1000
        config = self.load_configuration(
            self.CONFIG_PATH + 'configs/uplink_configs/different_node_finding_methods_config.json')
        GatewayDeviceUtil.update_connector_config(self.CONNECTOR_NAME, config, connector_type='opcua_asyncio')
        sleep(3)
        telemetry_keys = [key['key'] for node in config['Opcua']['configurationJson']['mapping'] for key in
                          node['timeseries']]
        actual_values = self.client.get_latest_timeseries(self.device.id, ','.join(telemetry_keys))
        for actual_value in actual_values.values():
            self.assertGreater(actual_value[0]['ts'], restarted_time)

    def test_sending_attrs_and_telemetries_after_connection_lost(self):
        self.client.update_device_credentials(DeviceCredentials())
        sleep(5)
        connection_lost_start_time = time() * 1000

        self.client.update_device_credentials(DeviceCredentials())
        sleep(5)

        config = self.load_configuration(self.CONFIG_PATH + 'configs/opcua_base_config.json')
        GatewayDeviceUtil.update_connector_config(self.CONNECTOR_NAME, config, connector_type='opcua_asyncio')
        sleep(2)

        telemetry_keys = [key['key'] for node in config['Opcua']['configurationJson']['mapping'] for key in
                          node['timeseries']]
        actual_values = self.client.get_latest_timeseries(self.device.id, ','.join(telemetry_keys))
        for actual_value in actual_values.values():
            self.assertGreater(actual_value[0]['ts'], connection_lost_start_time)

    def test_different_node_finding_methods_path(self):
        config = self.load_configuration(
            self.CONFIG_PATH + 'configs/uplink_configs/different_node_finding_methods_config_path.json')
        GatewayDeviceUtil.update_connector_config(self.CONNECTOR_NAME, config, connector_type='opcua_asyncio')
        sleep(3)
        telemetry_keys = [key['key'] for node in config['Opcua']['configurationJson']['mapping'] for key in
                          node['timeseries']]
        actual_values = self.client.get_latest_timeseries(self.device.id, ','.join(telemetry_keys))
        expected_values = self.load_configuration(
            self.CONFIG_PATH + 'test_values/uplink/different_node_finding_methods_values.json')

        for (_type, value) in expected_values.items():
            self.assertEqual(value, actual_values[_type][0]['value'],
                             f'Value is not equal for the next telemetry key: {_type}')

    def test_different_node_finding_methods_i(self):
        config = self.load_configuration(
            self.CONFIG_PATH + 'configs/uplink_configs/different_node_finding_methods_config_i.json')
        GatewayDeviceUtil.update_connector_config(self.CONNECTOR_NAME, config, connector_type='opcua_asyncio')
        sleep(3)
        telemetry_keys = [key['key'] for node in config['Opcua']['configurationJson']['mapping'] for key in
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
        GatewayDeviceUtil.update_connector_config(self.CONNECTOR_NAME, config, connector_type='opcua_asyncio')
        sleep(3)
        telemetry_keys = [key['key'] for node in config['Opcua']['configurationJson']['mapping'] for key in
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
        GatewayDeviceUtil.update_connector_config(self.CONNECTOR_NAME, config, connector_type='opcua_asyncio')
        sleep(3)
        telemetry_keys = [key['key'] for node in config['Opcua']['configurationJson']['mapping'] for key in
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
        GatewayDeviceUtil.update_connector_config(self.CONNECTOR_NAME, config, connector_type='opcua_asyncio')
        sleep(3)
        telemetry_keys = [key['key'] for node in config['Opcua']['configurationJson']['mapping'] for key in
                          node['timeseries']]
        actual_values = self.client.get_latest_timeseries(self.device.id, ','.join(telemetry_keys))
        expected_values = self.load_configuration(
            self.CONFIG_PATH + 'test_values/uplink/different_node_finding_methods_values.json')

        for (_type, value) in expected_values.items():
            self.assertEqual(value, actual_values[_type][0]['value'],
                             f'Value is not equal for the next telemetry key: {_type}')


if __name__ == '__main__':
    unittest.main()
