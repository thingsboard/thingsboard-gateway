from time import sleep

from tb_rest_client.models.models_ce import DeviceCredentials

from tests.blackbox.connectors.opcua.test_base_opcua import BaseOpcuaTest
from tests.test_utils.gateway_device_util import GatewayDeviceUtil


class OpcuaRPCTest(BaseOpcuaTest):
    def test_rpc_set(self):
        """
        Test method for the RPC set operation (with different nodes and vars finding methods).

        The method then iterates over the RPC methods in the configuration.
        For each RPC method, it pops the method name and sends a two-way device RPC request.
        The result of the request is compared with the expected value for the RPC method.
        If the result does not match the expected value, an assertion error is raised.
        """

        (config, _) = GatewayDeviceUtil.update_connector_config(
            self.CONNECTOR_NAME,
            self.CONFIG_PATH + 'configs/rpc_configs/input_registers_writing_rpc_little.json')
        sleep(5)
        expected_values = self.load_configuration(
            self.CONFIG_PATH + 'test_values/rpc/input_registers_values_writing_little.json')
        telemetry_keys = [key['key'] for slave in config[self.CONNECTOR_NAME]['configurationJson']['mapping'] for key in
                          slave['timeseries']]

        for rpc in config[self.CONNECTOR_NAME]['configurationJson']['mapping'][0]['rpc_methods']:
            rpc_method = rpc.pop('method')
            self.client.handle_two_way_device_rpc_request(self.device.id,
                                                          {
                                                              "method": rpc_method,
                                                              "params": expected_values[rpc_method]
                                                          })
        sleep(3)
        latest_ts = self.client.get_latest_timeseries(self.device.id, ','.join(telemetry_keys))
        for (_type, value) in expected_values.items():
            self.assertEqual(value, latest_ts[_type][0]['value'],
                             f'Value is not equal for the next telemetry key: {_type}')

    def test_rpc_get(self):
        """
        Test method for the RPC get operation (with different nodes and vars finding methods).
        """

        (config, _) = GatewayDeviceUtil.update_connector_config(
            self.CONNECTOR_NAME,
            self.CONFIG_PATH + 'configs/rpc_configs/discrete_inputs_reading_rpc_little.json')
        sleep(3)
        expected_values = self.load_configuration(
            self.CONFIG_PATH + 'test_values/rpc/discrete_and_coils_registers_values_reading_little.json')

        for rpc in config[self.CONNECTOR_NAME]['configurationJson']['mapping'][0]['rpc_methods']:
            rpc_method = rpc.pop('method')
            result = self.client.handle_two_way_device_rpc_request(self.device.id,
                                                                   {
                                                                       "method": rpc_method,
                                                                       "params": rpc
                                                                   })
            self.assertEqual(
                result, expected_values[rpc_method], f'Value is not equal for the next rpc: {rpc_method}')

    def test_rpc_method(self):
        """
        Test method for a generic RPC method. It updates the connector configuration, loads expected values,
        and iterates over RPC methods. For each method, it sends a two-way device RPC request and compares the result
        with the expected value.
        """

        (config, _) = GatewayDeviceUtil.update_connector_config(
            self.CONNECTOR_NAME, self.CONFIG_PATH + 'configs/rpc_configs/server_rpc_method.json')
        sleep(3)
        expected_values = self.load_configuration(
            self.CONFIG_PATH + 'test_values/rpc/server_rpc_method_values.json')

        for rpc in config[self.CONNECTOR_NAME]['configurationJson']['mapping'][0]['rpc_methods']:
            rpc_method = rpc.pop('method')
            result = self.client.handle_two_way_device_rpc_request(self.device.id,
                                                                   {
                                                                       "method": rpc_method,
                                                                       "params": rpc
                                                                   })
            self.assertEqual(result, expected_values[rpc_method], f'Value is not equal for the next rpc: {rpc_method}')

    def test_rps_after_gateway_restart(self):
        """
        Method tests the RPC operations after a gateway restart. It first restarts the gateway and then loads the
        configuration and expected values. It then updates the connector configuration and sleeps for 3 seconds.
        It iterates over the RPC methods, for each method, it sends a two-way device RPC request and compares the result
        with the expected value. It then sleeps for 3 seconds and gets the latest timeseries. It asserts that the value
        from the latest timeseries is equal to the expected value.
        """

        GatewayDeviceUtil.restart_gateway()
        config = self.load_configuration(
            self.CONFIG_PATH + 'configs/uplink_configs/different_node_finding_methods_config.json')
        expected_values = self.load_configuration(
            self.CONFIG_PATH + 'test_values/rpc/input_registers_values_writing_little.json')
        GatewayDeviceUtil.update_connector_config(self.CONNECTOR_NAME, config)
        sleep(3)
        telemetry_keys = [key['key'] for node in config['Opcua']['configurationJson']['mapping'] for key in
                          node['timeseries']]
        for rpc in config[self.CONNECTOR_NAME]['configurationJson']['mapping'][0]['rpc_methods']:
            rpc_method = rpc.pop('method')
            self.client.handle_two_way_device_rpc_request(self.device.id,
                                                          {
                                                              "method": rpc_method,
                                                              "params": expected_values[rpc_method]
                                                          })
        sleep(3)
        latest_ts = self.client.get_latest_timeseries(self.device.id, ','.join(telemetry_keys))
        for (_type, value) in expected_values.items():
            self.assertEqual(value, latest_ts[_type][0]['value'],
                             f'Value is not equal for the next telemetry key: {_type}')

    def test_rps_after_gateway_connection_lost(self):
        """
        Method tests the RPC operations after a gateway connection is lost and then restored. It first updates the
        device credentials and sleeps for 5 seconds.
        It then updates the device credentials again and sleeps for another 5 seconds. It then loads the configuration
        and expected values.
        It then updates the connector configuration and sleeps for 2 seconds. It iterates over the RPC methods, for
        each method, it sends a two-way device RPC request and compares the result
        with the expected value. If the result does not match the expected value, an assertion error is raised.
        """

        self.client.update_device_credentials(DeviceCredentials())
        sleep(5)

        self.client.update_device_credentials(DeviceCredentials())
        sleep(5)

        config = self.load_configuration(self.CONFIG_PATH + 'configs/opcua_base_config.json')
        GatewayDeviceUtil.update_connector_config(self.CONNECTOR_NAME, config)
        sleep(2)

        expected_values = self.load_configuration(
            self.CONFIG_PATH + 'test_values/rpc/server_rpc_method_values.json')

        for rpc in config[self.CONNECTOR_NAME]['configurationJson']['mapping'][0]['rpc_methods']:
            rpc_method = rpc.pop('method')
            result = self.client.handle_two_way_device_rpc_request(self.device.id,
                                                                   {
                                                                       "method": rpc_method,
                                                                       "params": rpc
                                                                   })
            self.assertEqual(result, expected_values[rpc_method], f'Value is not equal for the next rpc: {rpc_method}')


class OpcuaAsyncioRPCTest(BaseOpcuaTest):
    def test_rpc_set(self):
        """
        (AsyncIO) Test method for the RPC set operation (with different nodes and vars finding methods).

        The method then iterates over the RPC methods in the configuration.
        For each RPC method, it pops the method name and sends a two-way device RPC request.
        The result of the request is compared with the expected value for the RPC method.
        If the result does not match the expected value, an assertion error is raised.
        """

        (config, _) = GatewayDeviceUtil.update_connector_config(
            self.CONNECTOR_NAME,
            self.CONFIG_PATH + 'configs/rpc_configs/input_registers_writing_rpc_little.json',
            connector_type='opcua_asyncio')
        sleep(5)
        expected_values = self.load_configuration(
            self.CONFIG_PATH + 'test_values/rpc/input_registers_values_writing_little.json')
        telemetry_keys = [key['key'] for slave in config[self.CONNECTOR_NAME]['configurationJson']['mapping'] for key in
                          slave['timeseries']]

        for rpc in config[self.CONNECTOR_NAME]['configurationJson']['mapping'][0]['rpc_methods']:
            rpc_method = rpc.pop('method')
            self.client.handle_two_way_device_rpc_request(self.device.id,
                                                          {
                                                              "method": rpc_method,
                                                              "params": expected_values[rpc_method]
                                                          })
        sleep(3)
        latest_ts = self.client.get_latest_timeseries(self.device.id, ','.join(telemetry_keys))
        for (_type, value) in expected_values.items():
            self.assertEqual(value, latest_ts[_type][0]['value'],
                             f'Value is not equal for the next telemetry key: {_type}')

    def test_rpc_get(self):
        """
        (AsyncIO) Test method for the RPC get operation (with different nodes and vars finding methods).
        """

        (config, _) = GatewayDeviceUtil.update_connector_config(
            self.CONNECTOR_NAME,
            self.CONFIG_PATH + 'configs/rpc_configs/discrete_inputs_reading_rpc_little.json',
            connector_type='opcua_asyncio')
        sleep(3)
        expected_values = self.load_configuration(
            self.CONFIG_PATH + 'test_values/rpc/discrete_and_coils_registers_values_reading_little.json')

        for rpc in config[self.CONNECTOR_NAME]['configurationJson']['mapping'][0]['rpc_methods']:
            rpc_method = rpc.pop('method')
            result = self.client.handle_two_way_device_rpc_request(self.device.id,
                                                                   {
                                                                       "method": rpc_method,
                                                                       "params": rpc
                                                                   })
            self.assertEqual(
                result, expected_values[rpc_method], f'Value is not equal for the next rpc: {rpc_method}')

    def test_rpc_method(self):
        """
        (AsyncIO) Test method for a generic RPC method. It updates the connector configuration, loads expected values,
        and iterates over RPC methods. For each method, it sends a two-way device RPC request and compares the result
        with the expected value.
        """

        (config, _) = GatewayDeviceUtil.update_connector_config(
            self.CONNECTOR_NAME, self.CONFIG_PATH + 'configs/rpc_configs/server_rpc_method.json',
            connector_type='opcua_asyncio')
        sleep(3)
        expected_values = self.load_configuration(
            self.CONFIG_PATH + 'test_values/rpc/server_rpc_method_values.json')

        for rpc in config[self.CONNECTOR_NAME]['configurationJson']['mapping'][0]['rpc_methods']:
            rpc_method = rpc.pop('method')
            result = self.client.handle_two_way_device_rpc_request(self.device.id,
                                                                   {
                                                                       "method": rpc_method,
                                                                       "params": rpc
                                                                   })
            self.assertEqual(result, expected_values[rpc_method], f'Value is not equal for the next rpc: {rpc_method}')

    def test_rps_after_gateway_restart(self):
        """
        (AsyncIO) Method tests the RPC operations after a gateway restart. It first restarts the gateway and then loads the
        configuration and expected values. It then updates the connector configuration and sleeps for 3 seconds.
        It iterates over the RPC methods, for each method, it sends a two-way device RPC request and compares the result
        with the expected value. It then sleeps for 3 seconds and gets the latest timeseries. It asserts that the value
        from the latest timeseries is equal to the expected value.
        """

        GatewayDeviceUtil.restart_gateway()
        config = self.load_configuration(
            self.CONFIG_PATH + 'configs/uplink_configs/different_node_finding_methods_config.json')
        expected_values = self.load_configuration(
            self.CONFIG_PATH + 'test_values/rpc/input_registers_values_writing_little.json')
        GatewayDeviceUtil.update_connector_config(self.CONNECTOR_NAME, config, connector_type='opcua_asyncio')
        sleep(3)
        telemetry_keys = [key['key'] for node in config['Opcua']['configurationJson']['mapping'] for key in
                          node['timeseries']]
        for rpc in config[self.CONNECTOR_NAME]['configurationJson']['mapping'][0]['rpc_methods']:
            rpc_method = rpc.pop('method')
            self.client.handle_two_way_device_rpc_request(self.device.id,
                                                          {
                                                              "method": rpc_method,
                                                              "params": expected_values[rpc_method]
                                                          })
        sleep(3)
        latest_ts = self.client.get_latest_timeseries(self.device.id, ','.join(telemetry_keys))
        for (_type, value) in expected_values.items():
            self.assertEqual(value, latest_ts[_type][0]['value'],
                             f'Value is not equal for the next telemetry key: {_type}')

    def test_rps_after_gateway_connection_lost(self):
        """
        (AsyncIO) Method tests the RPC operations after a gateway connection is lost and then restored. It first updates the
        device credentials and sleeps for 5 seconds.
        It then updates the device credentials again and sleeps for another 5 seconds. It then loads the configuration
        and expected values.
        It then updates the connector configuration and sleeps for 2 seconds. It iterates over the RPC methods, for
        each method, it sends a two-way device RPC request and compares the result
        with the expected value. If the result does not match the expected value, an assertion error is raised.
        """

        self.client.update_device_credentials(DeviceCredentials())
        sleep(5)

        self.client.update_device_credentials(DeviceCredentials())
        sleep(5)

        config = self.load_configuration(self.CONFIG_PATH + 'configs/opcua_base_config.json')
        GatewayDeviceUtil.update_connector_config(self.CONNECTOR_NAME, config)
        sleep(2)

        expected_values = self.load_configuration(
            self.CONFIG_PATH + 'test_values/rpc/server_rpc_method_values.json')

        for rpc in config[self.CONNECTOR_NAME]['configurationJson']['mapping'][0]['rpc_methods']:
            rpc_method = rpc.pop('method')
            result = self.client.handle_two_way_device_rpc_request(self.device.id,
                                                                   {
                                                                       "method": rpc_method,
                                                                       "params": rpc
                                                                   })
            self.assertEqual(result, expected_values[rpc_method], f'Value is not equal for the next rpc: {rpc_method}')
