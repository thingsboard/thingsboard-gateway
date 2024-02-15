from time import sleep

from tb_rest_client.models.models_ce import DeviceCredentials

from tests.blackbox.connectors.opcua.test_base_opcua import BaseOpcuaTest
from tests.test_utils.gateway_device_util import GatewayDeviceUtil


class OpcuaAttributesUpdatesTest(BaseOpcuaTest):
    def test_attr_update(self):
        self.update_device_and_connector_shared_attributes(
            'configs/attrs_update_configs/attrs_update_coils_registers_big.json',
            'test_values/attrs_update/discrete_and_coils_registers_values_big.json'
        )
        sleep(3)
        expected_values = self.load_configuration(
            self.CONFIG_PATH + 'test_values/attrs_update/discrete_and_coils_registers_values_big.json')
        actual_values = self.client.get_latest_timeseries(self.device.id,
                                                          ','.join([key for (key, _) in expected_values.items()]))
        for (_type, value) in expected_values.items():
            self.assertEqual(value, actual_values[_type][0]['value'],
                             f'Value is not equal for the next telemetry key: {_type}')

        # reset node values to default
        self.reset_node_default_values()

    def test_attr_update_after_gateway_restart(self):
        GatewayDeviceUtil.restart_gateway()

        self.update_device_and_connector_shared_attributes(
            'configs/attrs_update_configs/attrs_update_coils_registers_big.json',
            'test_values/attrs_update/discrete_and_coils_registers_values_big.json'
        )
        sleep(3)
        expected_values = self.load_configuration(
            self.CONFIG_PATH + 'test_values/attrs_update/discrete_and_coils_registers_values_big.json')
        actual_values = self.client.get_latest_timeseries(self.device.id,
                                                          ','.join([key for (key, _) in expected_values.items()]))
        for (_type, value) in expected_values.items():
            self.assertEqual(value, actual_values[_type][0]['value'],
                             f'Value is not equal for the next telemetry key: {_type}')

        # reset node values to default
        self.reset_node_default_values()

    def test_attr_update_after_connection_lost(self):
        self.client.update_device_credentials(DeviceCredentials())
        sleep(5)

        self.client.update_device_credentials(DeviceCredentials())
        sleep(5)

        config = self.load_configuration(self.CONFIG_PATH + 'configs/opcua_base_config.json')
        GatewayDeviceUtil.update_connector_config(self.CONNECTOR_NAME, config)
        sleep(2)

        self.update_device_and_connector_shared_attributes(
            'configs/attrs_update_configs/attrs_update_coils_registers_big.json',
            'test_values/attrs_update/discrete_and_coils_registers_values_big.json'
        )
        sleep(3)
        expected_values = self.load_configuration(
            self.CONFIG_PATH + 'test_values/attrs_update/discrete_and_coils_registers_values_big.json')
        actual_values = self.client.get_latest_timeseries(self.device.id,
                                                          ','.join([key for (key, _) in expected_values.items()]))
        for (_type, value) in expected_values.items():
            self.assertEqual(value, actual_values[_type][0]['value'],
                             f'Value is not equal for the next telemetry key: {_type}')

        # reset node values to default
        self.reset_node_default_values()


class OpcuaAsyncioAttributesUpdatesTest(BaseOpcuaTest):
    def test_attr_update(self):
        self.update_device_and_connector_shared_attributes(
            'configs/attrs_update_configs/attrs_update_coils_registers_big.json',
            'test_values/attrs_update/discrete_and_coils_registers_values_big.json',
            connector_type='opcua_asyncio'
        )
        sleep(3)
        expected_values = self.load_configuration(
            self.CONFIG_PATH + 'test_values/attrs_update/discrete_and_coils_registers_values_big.json')
        actual_values = self.client.get_latest_timeseries(self.device.id,
                                                          ','.join([key for (key, _) in expected_values.items()]))
        for (_type, value) in expected_values.items():
            self.assertEqual(value, actual_values[_type][0]['value'],
                             f'Value is not equal for the next telemetry key: {_type}')

    def test_attr_update_after_gateway_restart(self):
        GatewayDeviceUtil.restart_gateway()

        self.update_device_and_connector_shared_attributes(
            'configs/attrs_update_configs/attrs_update_coils_registers_big.json',
            'test_values/attrs_update/discrete_and_coils_registers_values_big.json',
            connector_type='opcua_asyncio'
        )
        sleep(3)
        expected_values = self.load_configuration(
            self.CONFIG_PATH + 'test_values/attrs_update/discrete_and_coils_registers_values_big.json')
        actual_values = self.client.get_latest_timeseries(self.device.id,
                                                          ','.join([key for (key, _) in expected_values.items()]))
        for (_type, value) in expected_values.items():
            self.assertEqual(value, actual_values[_type][0]['value'],
                             f'Value is not equal for the next telemetry key: {_type}')

        # reset node values to default
        self.reset_node_default_values()

    def test_attr_update_after_connection_lost(self):
        self.client.update_device_credentials(DeviceCredentials())
        sleep(5)

        self.client.update_device_credentials(DeviceCredentials())
        sleep(5)

        config = self.load_configuration(self.CONFIG_PATH + 'configs/opcua_base_config.json')
        GatewayDeviceUtil.update_connector_config(self.CONNECTOR_NAME, config)
        sleep(2)

        self.update_device_and_connector_shared_attributes(
            'configs/attrs_update_configs/attrs_update_coils_registers_big.json',
            'test_values/attrs_update/discrete_and_coils_registers_values_big.json',
            connector_type='opcua_asyncio'
        )
        sleep(3)
        expected_values = self.load_configuration(
            self.CONFIG_PATH + 'test_values/attrs_update/discrete_and_coils_registers_values_big.json')
        actual_values = self.client.get_latest_timeseries(self.device.id,
                                                          ','.join([key for (key, _) in expected_values.items()]))
        for (_type, value) in expected_values.items():
            self.assertEqual(value, actual_values[_type][0]['value'],
                             f'Value is not equal for the next telemetry key: {_type}')

        # reset node values to default
        self.reset_node_default_values()
