import unittest
from os import path
from time import time, sleep
import logging

from pymodbus.exceptions import ConnectionException
import pymodbus.client as ModbusClient
from simplejson import load, loads
from tb_rest_client.rest_client_ce import *

from tests.base_test import BaseTest
from tests.test_utils.gateway_device_util import GatewayDeviceUtil

CONNECTION_TIMEOUT = 300
DEVICE_CREATION_TIMEOUT = 200
GENERAL_TIMEOUT = 6


LOG = logging.getLogger("TEST")


class ModbusAttributesUpdatesTest(BaseTest):
    CONFIG_PATH = path.join(path.dirname(path.dirname(path.dirname(path.abspath(__file__)))),
                            "data" + path.sep + "modbus" + path.sep)

    client = None
    gateway = None
    device = None

    @classmethod
    def setUpClass(cls) -> None:
        super(ModbusAttributesUpdatesTest, cls).setUpClass()

        # ThingsBoard REST API URL
        url = GatewayDeviceUtil.DEFAULT_URL

        # Default Tenant Administrator credentials
        username = GatewayDeviceUtil.DEFAULT_USERNAME
        password = GatewayDeviceUtil.DEFAULT_PASSWORD

        with RestClientCE(url) as cls.client:
            cls.client.login(username, password)

            cls.gateway = cls.client.get_tenant_devices(10, 0, text_search='Gateway').data[0]
            assert cls.gateway is not None

            start_connecting_time = time()

            while not GatewayDeviceUtil.is_gateway_connected(start_connecting_time):
                LOG.info('Gateway connecting to TB...')
                sleep(1)
                if time() - start_connecting_time > CONNECTION_TIMEOUT:
                    raise TimeoutError('Gateway is not connected to TB')

            LOG.info('Gateway connected to TB')

            (config, _) = cls.change_connector_configuration(
                cls.CONFIG_PATH + 'configs/default_modbus_config.json')

            start_device_creation_time = time()
            while time() - start_device_creation_time < DEVICE_CREATION_TIMEOUT:
                try:
                    cls.device = cls.client.get_tenant_devices(10, 0, text_search='Temp Sensor').data[0]
                except IndexError:
                    sleep(1)
                else:
                    break

            assert cls.device is not None

    @classmethod
    def tearDownClass(cls):
        GatewayDeviceUtil.clear_connectors()
        GatewayDeviceUtil.delete_device(cls.device.id)

        client = ModbusClient.ModbusTcpClient('localhost', port=5021)
        client.connect()
        try:
            # trigger register 28 to restart the modbus server
            client.write_register(28, 10, 1)
        except ConnectionException:
            # will call pymodbus.exceptions.ConnectionException because of restarting the server
            pass

        client.close()
        super(ModbusAttributesUpdatesTest, cls).tearDownClass()
        sleep(2)

    @classmethod
    def load_configuration(cls, config_file_path):
        with open(config_file_path, 'r', encoding="UTF-8") as config:
            config = load(config)
        return config

    @classmethod
    def change_connector_configuration(cls, config_file_path):
        """
        Change the configuration of the connector.

        Args:
            config_file_path (str): The path to the configuration file.

        Returns:
            tuple: A tuple containing the modified configuration and the response of the save_device_attributes method.
        """

        config = cls.load_configuration(config_file_path)
        config['Modbus']['ts'] = int(time() * 1000)
        response = cls.client.save_device_attributes(cls.gateway.id, 'SHARED_SCOPE', config)
        sleep(GENERAL_TIMEOUT)
        return config, response

    def reset_slave_default_values(self):
        default_slave_values = self.load_configuration(
            self.CONFIG_PATH + 'test_values/default_slave_values.json')
        self.client.save_device_attributes(self.device.id, 'SHARED_SCOPE', default_slave_values)

    def update_device_shared_attributes(self, config_file_path):
        config = self.load_configuration(self.CONFIG_PATH + config_file_path)
        self.client.save_device_attributes(self.device.id, 'SHARED_SCOPE', config)

    def update_device_and_connector_shared_attributes(self, connector_config_file_path, device_config_file_path):
        self.change_connector_configuration(self.CONFIG_PATH + connector_config_file_path)
        self.update_device_shared_attributes(device_config_file_path)

    def test_input_register_attrs_update_little_endian(self):
        """
        This function tests the update of input register attributes in little endian format.

        It changes the connector configuration, loads must-be values, saves device attributes, sleeps,
        checks for equality of values, and resets slave values to default.

        Parameters:
        - self: the test class instance

        Returns:
        - None
        """

        self.update_device_and_connector_shared_attributes(
            'configs/attrs_update_configs/attrs_update_input_registers_little.json',
            'test_values/attrs_update/input_registers_values_little.json'
        )
        sleep(2)

        expected_values = self.load_configuration(
            self.CONFIG_PATH + 'test_values/attrs_update/input_registers_values_little.json')
        actual_values = self.client.get_latest_timeseries(self.device.id,
                                                          ','.join([key for (key, _) in expected_values.items()]))
        for (_type, value) in expected_values.items():
            if _type == 'bits':
                actual_values[_type][0]['value'] = loads(actual_values[_type][0]['value'])

            self.assertEqual(value, actual_values[_type][0]['value'],
                             f'Value is not equal for the next telemetry key: {_type}')

        # reset slave values to default
        self.reset_slave_default_values()

    def test_holding_register_attrs_update_little_endian(self):
        """
        Test for updating holding register attributes in little endian format.

        This function updates the device and connector shared attributes using specific JSON
        configuration files. It then retrieves the expected and actual values and compares them.
        Finally, it resets the slave values to default.

        Parameters:
            self (obj): The object instance
        Returns:
            None
        """

        self.update_device_and_connector_shared_attributes(
            'configs/attrs_update_configs/attrs_update_holding_registers_little.json',
            'test_values/attrs_update/holding_registers_values_little.json'
        )
        sleep(GENERAL_TIMEOUT)
        expected_values = self.load_configuration(
            self.CONFIG_PATH + 'test_values/attrs_update/holding_registers_values_little.json')
        actual_values = self.client.get_latest_timeseries(self.device.id,
                                                          ','.join([key for (key, _) in expected_values.items()]))
        for (_type, value) in expected_values.items():
            if _type == 'bits':
                actual_values[_type][0]['value'] = loads(actual_values[_type][0]['value'])

            self.assertEqual(value, actual_values[_type][0]['value'],
                             f'Value is not equal for the next telemetry key: {_type}')

        # reset slave values to default
        self.reset_slave_default_values()

    def test_coils_attrs_update_little_endian(self):
        """
        Function to test the update of little endian coils attributes.

        This function updates the device and connector shared attributes using the provided configuration
        files. It then sleeps for 2 seconds. It loads the expected values from the configuration file and
        retrieves the actual values from the client. It compares the expected and actual values, and resets the
        slave values to default at the end.

        Parameters:
            self: the reference to the current instance of the class.

        Returns:
            None
        """

        self.update_device_and_connector_shared_attributes(
            'configs/attrs_update_configs/attrs_update_coils_registers_little.json',
            'test_values/attrs_update/discrete_and_coils_registers_values_little.json')
        sleep(GENERAL_TIMEOUT)
        expected_values = self.load_configuration(
            self.CONFIG_PATH + 'test_values/attrs_update/discrete_and_coils_registers_values_little.json')
        actual_values = self.client.get_latest_timeseries(self.device.id,
                                                          ','.join([key for (key, _) in expected_values.items()]))
        for (_type, value) in expected_values.items():
            if _type == 'bits' or _type == 'bit':
                actual_values[_type][0]['value'] = loads(actual_values[_type][0]['value'])

            self.assertEqual(value, actual_values[_type][0]['value'],
                             f'Value is not equal for the next telemetry key: {_type}')

        # reset slave values to default
        self.reset_slave_default_values()

    def test_discrete_input_attrs_update_little_endian(self):
        """
        This function tests the update of input register attributes in little endian format.

        Parameters:
        - self: the test class instance

        Returns:
        - None
        """

        self.update_device_and_connector_shared_attributes(
            'configs/attrs_update_configs/attrs_update_discrete_input_little.json',
            'test_values/attrs_update/discrete_and_coils_registers_values_little.json'
        )
        sleep(GENERAL_TIMEOUT)
        expected_values = self.load_configuration(
            self.CONFIG_PATH + 'test_values/attrs_update/discrete_and_coils_registers_values_little.json')
        actual_values = self.client.get_latest_timeseries(self.device.id,
                                                          ','.join([key for (key, _) in expected_values.items()]))
        for (_type, value) in expected_values.items():
            if _type == 'bits' or _type == 'bit':
                actual_values[_type][0]['value'] = loads(actual_values[_type][0]['value'])

            self.assertEqual(value, actual_values[_type][0]['value'],
                             f'Value is not equal for the next telemetry key: {_type}')

        # reset slave values to default
        self.reset_slave_default_values()

    def test_input_register_attrs_update_big_endian(self):
        """
        Update the shared attributes for input registers in big endian format.

        :param self: The object itself.
        :return: None
        """

        self.update_device_and_connector_shared_attributes(
            'configs/attrs_update_configs/attrs_update_input_registers_big.json',
            'test_values/attrs_update/input_registers_values_big.json'
        )
        sleep(GENERAL_TIMEOUT)
        expected_values = self.load_configuration(
            self.CONFIG_PATH + 'test_values/attrs_update/input_registers_values_big.json')
        actual_values = self.client.get_latest_timeseries(self.device.id,
                                                          ','.join([key for (key, _) in expected_values.items()]))
        for (_type, value) in expected_values.items():
            if _type == 'bits':
                actual_values[_type][0]['value'] = loads(actual_values[_type][0]['value'])

            self.assertEqual(value, actual_values[_type][0]['value'],
                             f'Value is not equal for the next telemetry key: {_type}')

        # reset slave values to default
        self.reset_slave_default_values()

    def test_holding_register_attrs_update_big_endian(self):
        """
        Test for updating holding register attributes in big endian format.

        This function does the following:
        - Calls the update_device_and_connector_shared_attributes method with specific JSON files
        - Waits for 2 seconds
        - Loads expected values from a configuration file
        - Retrieves actual values from the client
        - Compares the expected and actual values
        - Resets slave values to default

        Parameters:
        - self: the instance of the test class

        Returns:
        - None
        """

        self.update_device_and_connector_shared_attributes(
            'configs/attrs_update_configs/attrs_update_holding_registers_big.json',
            'test_values/attrs_update/holding_registers_values_big.json'
        )
        sleep(GENERAL_TIMEOUT)
        expected_values = self.load_configuration(
            self.CONFIG_PATH + 'test_values/attrs_update/holding_registers_values_big.json')
        actual_values = self.client.get_latest_timeseries(self.device.id,
                                                          ','.join([key for (key, _) in expected_values.items()]))
        for (_type, value) in expected_values.items():
            if _type == 'bits':
                actual_values[_type][0]['value'] = loads(actual_values[_type][0]['value'])

            self.assertEqual(value, actual_values[_type][0]['value'],
                             f'Value is not equal for the next telemetry key: {_type}')

        # reset slave values to default
        self.reset_slave_default_values()

    def test_coils_attrs_update_big_endian(self):
        """
        Function to test the update of shared attributes in big endian format for coils.

        Parameters:
        - self: the object itself

        Returns:
        - None
        """

        self.update_device_and_connector_shared_attributes(
            'configs/attrs_update_configs/attrs_update_coils_registers_big.json',
            'test_values/attrs_update/discrete_and_coils_registers_values_big.json'
        )
        sleep(GENERAL_TIMEOUT)
        expected_values = self.load_configuration(
            self.CONFIG_PATH + 'test_values/attrs_update/discrete_and_coils_registers_values_big.json')
        actual_values = self.client.get_latest_timeseries(self.device.id,
                                                          ','.join([key for (key, _) in expected_values.items()]))
        for (_type, value) in expected_values.items():
            if _type == 'bits' or _type == 'bit':
                actual_values[_type][0]['value'] = loads(actual_values[_type][0]['value'])

            self.assertEqual(value, actual_values[_type][0]['value'],
                             f'Value is not equal for the next telemetry key: {_type}')

        # reset slave values to default
        self.reset_slave_default_values()

    def test_discrete_input_attrs_update_big_endian(self):
        """
        Updates shared attributes with a big-endian discrete input configuration
        and tests the expected values against the actual values.

        :param self: the object itself
        :return: None
        """

        self.update_device_and_connector_shared_attributes(
            'configs/attrs_update_configs/attrs_update_discrete_input_big.json',
            'test_values/attrs_update/discrete_and_coils_registers_values_big.json'
        )
        sleep(GENERAL_TIMEOUT)
        expected_values = self.load_configuration(
            self.CONFIG_PATH + 'test_values/attrs_update/discrete_and_coils_registers_values_big.json')
        actual_values = self.client.get_latest_timeseries(self.device.id,
                                                          ','.join([key for (key, _) in expected_values.items()]))
        for (_type, value) in expected_values.items():
            if _type == 'bits' or _type == 'bit':
                LOG.info(f'Actual value: {actual_values[_type][0]["value"]}')
                actual_values[_type][0]['value'] = loads(actual_values[_type][0]['value'])

            self.assertEqual(value, actual_values[_type][0]['value'],
                             f'Value is not equal for the next telemetry key: {_type}')

        # reset slave values to default
        self.reset_slave_default_values()

    def test_gateway_restarted(self):
        self.client.handle_two_way_device_rpc_request(self.gateway.id, {"method": "gateway_restart"})
        start_time = time()
        sleep(15)
        while not GatewayDeviceUtil.is_gateway_connected(start_time):
            LOG.info('Gateway connecting to TB...')
            sleep(1)
        self.update_device_and_connector_shared_attributes(
            'configs/attrs_update_configs/attrs_update_input_registers_little.json',
            'test_values/attrs_update/input_registers_values_little.json'
        )
        sleep(GENERAL_TIMEOUT)
        expected_values = self.load_configuration(
            self.CONFIG_PATH + 'test_values/attrs_update/input_registers_values_little.json')
        actual_values = self.client.get_latest_timeseries(self.device.id,
                                                          ','.join([key for (key, _) in expected_values.items()]))
        for (_type, value) in expected_values.items():
            if _type == 'bits' or _type == 'bit':
                actual_values[_type][0]['value'] = loads(actual_values[_type][0]['value'])

            self.assertEqual(value, actual_values[_type][0]['value'],
                             f'Value is not equal for the next telemetry key: {_type}')

        # reset slave values to default
        self.reset_slave_default_values()


if __name__ == '__main__':
    unittest.main()
