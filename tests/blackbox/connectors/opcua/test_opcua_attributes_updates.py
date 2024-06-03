from time import sleep
from unittest import skip

from tests.blackbox.connectors.opcua.test_base_opcua import BaseOpcuaTest
from tests.test_utils.gateway_device_util import GatewayDeviceUtil


@skip('Flaky test')
class OpcuaAsyncioAttributesUpdatesTest(BaseOpcuaTest):
    def test_attr_update(self):
        self.update_device_and_connector_shared_attributes(
            'configs/attrs_update_configs/attrs_update_node.json',
            'test_values/attrs_update/attrs_update_node_values.json')
        sleep(self.GENERAL_TIMEOUT)
        expected_values = self.load_configuration(
            self.CONFIG_PATH + 'test_values/attrs_update/attrs_update_node_values.json')
        actual_values = self.client.get_latest_timeseries(self.device.id,
                                                          ','.join([key for (key, _) in expected_values.items()]))
        for (_type, value) in expected_values.items():
            self.assertEqual(str(value), actual_values[_type][0]['value'],
                             f'Value is not equal for the next telemetry key: {_type}')

    # def test_attr_update_after_gateway_restart(self):
    #     GatewayDeviceUtil.restart_gateway()
    #
    #     self.update_device_and_connector_shared_attributes(
    #         'configs/attrs_update_configs/attrs_update_node.json',
    #         'test_values/attrs_update/attrs_update_restart_node_values.json')
    #     sleep(self.GENERAL_TIMEOUT)
    #     expected_values = self.load_configuration(
    #         self.CONFIG_PATH + 'test_values/attrs_update/attrs_update_restart_node_values.json')
    #     actual_values = self.client.get_latest_timeseries(self.device.id,
    #                                                       ','.join([key for (key, _) in expected_values.items()]))
    #     for (_type, value) in expected_values.items():
    #         self.assertEqual(str(value), actual_values[_type][0]['value'],
    #                          f'Value is not equal for the next telemetry key: {_type}')
    #
    #     # reset node values to default
    #     self.reset_node_default_values()
    #     sleep(self.GENERAL_TIMEOUT)

    def test_attr_update_after_connection_lost(self):
        GatewayDeviceUtil.update_credentials({"credentialsType": "ACCESS_TOKEN",
                                              "credentialsId": "SOME_ACCESS_TOKEN"})
        sleep(self.GENERAL_TIMEOUT)

        GatewayDeviceUtil.update_credentials({"credentialsType": "ACCESS_TOKEN",
                                              "credentialsId": "YOUR_ACCESS_TOKEN"})
        sleep(self.GENERAL_TIMEOUT)

        self.update_device_and_connector_shared_attributes(
            'configs/attrs_update_configs/attrs_update_node.json',
            'test_values/attrs_update/attrs_update_restart_node_values.json')
        sleep(self.GENERAL_TIMEOUT)
        expected_values = self.load_configuration(
            self.CONFIG_PATH + 'test_values/attrs_update/attrs_update_restart_node_values.json')
        actual_values = self.client.get_latest_timeseries(self.device.id,
                                                          ','.join([key for (key, _) in expected_values.items()]))
        for (_type, value) in expected_values.items():
            self.assertEqual(str(value), actual_values[_type][0]['value'],
                             f'Value is not equal for the next telemetry key: {_type}')

        # reset node values to default
        self.reset_node_default_values()
        sleep(self.GENERAL_TIMEOUT)
