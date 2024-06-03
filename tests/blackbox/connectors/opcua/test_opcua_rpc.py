from time import sleep
from unittest import skip

from tests.blackbox.connectors.opcua.test_base_opcua import BaseOpcuaTest
from tests.test_utils.gateway_device_util import GatewayDeviceUtil


@skip('Flaky test')
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
            self.CONFIG_PATH + 'configs/rpc_configs/rpc_set_method.json')
        sleep(self.GENERAL_TIMEOUT)
        expected_values = self.load_configuration(
            self.CONFIG_PATH + 'test_values/rpc/rpc_set_method_values.json')
        telemetry_keys = [key['key'] for node in config[self.CONNECTOR_NAME]['configurationJson']['server']['mapping']
                          for key in
                          node['timeseries']]

        self.client.handle_two_way_device_rpc_request(self.device.id,
                                                      {
                                                          "method": "set",
                                                          "params": "ns=3;i=2;12"
                                                      })
        sleep(self.GENERAL_TIMEOUT)
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
            self.CONFIG_PATH + 'configs/rpc_configs/rpc_get_method.json')
        sleep(self.GENERAL_TIMEOUT)
        expected_values = self.load_configuration(
            self.CONFIG_PATH + 'test_values/rpc/rpc_get_method_values.json')

        result = self.client.handle_two_way_device_rpc_request(self.device.id,
                                                               {
                                                                   "method": "get",
                                                                   "params": "ns=3;i=3;"
                                                               })
        sleep(self.GENERAL_TIMEOUT)
        self.assertEqual(
            result['get']['value'], expected_values["s"], f'Value is not equal for the next rpc: get')

    def test_rpc_method(self):
        """
        (AsyncIO) Test method for a generic RPC method. It updates the connector configuration, loads expected values,
        and iterates over RPC methods. For each method, it sends a two-way device RPC request and compares the result
        with the expected value.
        """

        (config, _) = GatewayDeviceUtil.update_connector_config(
            self.CONNECTOR_NAME, self.CONFIG_PATH + 'configs/rpc_configs/rpc_server_method.json')
        sleep(self.GENERAL_TIMEOUT)
        expected_values = self.load_configuration(
            self.CONFIG_PATH + 'test_values/rpc/rpc_server_method_values.json')

        for rpc in config[self.CONNECTOR_NAME]['configurationJson']['server']['mapping'][0]['rpc_methods']:
            rpc_method = rpc.pop('method')
            result = self.client.handle_two_way_device_rpc_request(self.device.id,
                                                                   {
                                                                       "method": rpc_method,
                                                                       "params": rpc['arguments']
                                                                   })
            self.assertEqual(result[rpc_method]['result'], expected_values[rpc_method],
                             f'Value is not equal for the next rpc: {rpc_method}')

    # def test_rps_after_gateway_restart(self):
    #     """
    #     (AsyncIO) Method tests the RPC operations after a gateway restart. It first restarts the gateway and then
    #     loads the configuration and expected values.
    #     It then updates the connector configuration and sleeps for 3 seconds.
    #     It iterates over the RPC methods, for each method, it sends a two-way device RPC request and compares the result
    #     with the expected value. It then sleeps for 3 seconds and gets the latest timeseries. It asserts that the value
    #     from the latest timeseries is equal to the expected value.
    #     """
    #
    #     GatewayDeviceUtil.restart_gateway()
    #     (config, _) = GatewayDeviceUtil.update_connector_config(
    #         self.CONNECTOR_NAME, self.CONFIG_PATH + 'configs/rpc_configs/rpc_server_method.json')
    #     sleep(15)
    #     expected_values = self.load_configuration(
    #         self.CONFIG_PATH + 'test_values/rpc/rpc_server_method_values.json')
    #
    #     for rpc in config[self.CONNECTOR_NAME]['configurationJson']['server']['mapping'][0]['rpc_methods']:
    #         rpc_method = rpc.pop('method')
    #         result = self.client.handle_two_way_device_rpc_request(self.device.id,
    #                                                                {
    #                                                                    "method": rpc_method,
    #                                                                    "params": rpc['arguments']
    #                                                                })
    #         self.assertEqual(result[rpc_method]['result'], expected_values[rpc_method],
    #                          f'Value is not equal for the next rpc: {rpc_method}')

    def test_rpc_after_gateway_connection_lost(self):
        """
        (AsyncIO) Method tests the RPC operations after a gateway connection is lost and then restored. It first
        updates the device credentials and sleeps for 5 seconds.
        It then updates the device credentials again and sleeps for another 5 seconds. It then loads the configuration
        and expected values.
        It then updates the connector configuration and sleeps for 2 seconds. It iterates over the RPC methods, for
        each method, it sends a two-way device RPC request and compares the result
        with the expected value. If the result does not match the expected value, an assertion error is raised.
        """

        GatewayDeviceUtil.update_credentials({"credentialsType": "ACCESS_TOKEN",
                                              "credentialsId": "SOME_ACCESS_TOKEN"})
        sleep(15)

        GatewayDeviceUtil.update_credentials({"credentialsType": "ACCESS_TOKEN",
                                              "credentialsId": "YOUR_ACCESS_TOKEN"})
        sleep(15)

        (config, _) = GatewayDeviceUtil.update_connector_config(
            self.CONNECTOR_NAME, self.CONFIG_PATH + 'configs/rpc_configs/rpc_server_method.json')
        sleep(self.GENERAL_TIMEOUT)
        expected_values = self.load_configuration(
            self.CONFIG_PATH + 'test_values/rpc/rpc_server_method_values.json')

        for rpc in config[self.CONNECTOR_NAME]['configurationJson']['server']['mapping'][0]['rpc_methods']:
            rpc_method = rpc.pop('method')
            result = self.client.handle_two_way_device_rpc_request(self.device.id,
                                                                   {
                                                                       "method": rpc_method,
                                                                       "params": rpc['arguments']
                                                                   })
            self.assertEqual(result[rpc_method]['result'], expected_values[rpc_method],
                             f'Value is not equal for the next rpc: {rpc_method}')
