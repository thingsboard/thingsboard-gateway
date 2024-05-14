import unittest
from os import path
import logging
from time import sleep, time

from pymodbus.exceptions import ConnectionException
import pymodbus.client as ModbusClient
from simplejson import load
from tb_rest_client.rest_client_ce import *

from tests.base_test import BaseTest
from tests.test_utils.gateway_device_util import GatewayDeviceUtil

CONNECTION_TIMEOUT = 300
DEVICE_CREATION_TIMEOUT = 200
GENERAL_TIMEOUT = 6

LOG = logging.getLogger("TEST")


class ModbusUplinkMessagesTest(BaseTest):
    CONFIG_PATH = path.join(path.dirname(path.dirname(path.dirname(path.abspath(__file__)))),
                            "data" + path.sep + "modbus" + path.sep)

    client = None
    gateway = None
    device = None

    @classmethod
    def setUpClass(cls) -> None:
        super(ModbusUplinkMessagesTest, cls).setUpClass()

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
        super(ModbusUplinkMessagesTest, cls).tearDownClass()
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

    def test_send_only_on_data_changed(self):
        """
        Test the send_only_on_data_changed method.

        This method tests the behavior of the send_only_on_data_changed method in the MyClass class.
        The method performs the following steps:
        1. Changes the connector configuration using the change_connector_configuration method with
           the specified configuration file.
        2. Retrieves the telemetry keys from the modified configuration.
        3. Retrieves the latest timeseries data for the device.
        4. Pauses the execution for 5 seconds.
        5. Retrieves the latest timeseries data for the device again.
        6. Compares the timestamps of the two sets of timeseries data for each telemetry key.

        Parameters:
        - self: The instance of the ModbusUplinkMessagesTest class.

        Returns:
        None.
        """

        (config, _) = self.change_connector_configuration(
            self.CONFIG_PATH + 'configs/initial_modbus_uplink_converter_only_on_change_config.json')
        telemetry_keys = [key['tag'] for slave in config['Modbus']['configurationJson']['master']['slaves'] for key in
                          slave['timeseries']]
        actual_values = self.client.get_latest_timeseries(self.device.id, ','.join(telemetry_keys))
        sleep(GENERAL_TIMEOUT)
        latest_ts_1 = self.client.get_latest_timeseries(self.device.id, ','.join(telemetry_keys))

        # check that timestamps are equal
        for ts_key in telemetry_keys:
            self.assertEqual(actual_values[ts_key][0]['ts'], latest_ts_1[ts_key][0]['ts'],
                             f'Timestamps are not equal for the next telemetry key: {ts_key}')

    def test_input_register_reading_little_endian(self):
        """
        Test the input register reading in little endian format.

        This function tests the reading of input registers in little endian format. It performs the following steps:

        1. Changes the connector configuration by loading the JSON file located at
           'data/modbus/configs/modbus_uplink_converter_input_registers_reading_little.json'.
        2. Retrieves the telemetry keys from the configuration.
        3. Waits for 2 seconds.
        4. Retrieves the latest timeseries data for the specified telemetry keys.
        5. Loads the expected values for the input registers from the JSON file located at
           'data/modbus/test_values/input_registers_values.json'.
        6. Compares the expected values with the actual values obtained from the timeseries data.

        Parameters:
            self (ModbusUplinkMessagesTest): The object instance.

        Returns:
            None
        """

        (config, _) = self.change_connector_configuration(
            self.CONFIG_PATH + 'configs/uplink_configs/modbus_uplink_converter_input_registers_reading_little.json')
        telemetry_keys = [key['tag'] for slave in config['Modbus']['configurationJson']['master']['slaves'] for key in
                          slave['timeseries']]
        sleep(GENERAL_TIMEOUT)
        actual_values = self.client.get_latest_timeseries(self.device.id, ','.join(telemetry_keys))
        expected_values = self.load_configuration(
            self.CONFIG_PATH + 'test_values/uplink/input_registers_values_little.json')

        for (_type, value) in expected_values.items():
            self.assertEqual(value, actual_values[_type][0]['value'],
                             f'Value is not equal for the next telemetry key: {_type}')

    def test_holding_register_reading_little_endian(self):
        """
        Test the reading of holding registers in little-endian format.

        This function is responsible for testing the reading of holding registers in
        little-endian format. It performs the following steps:

        1. Changes the connector configuration using the specified JSON file.
        2. Retrieves the telemetry keys from the configuration.
        3. Waits for 2 seconds.
        4. Retrieves the latest timeseries data for the device using the retrieved telemetry keys.
        5. Loads the expected values from the specified JSON file.
        6. Compares the expected values with the retrieved timeseries data.

        Parameters:
            self (ModbusUplinkMessagesTest): The object instance.

        Returns:
            None
        """

        (config, _) = self.change_connector_configuration(
            self.CONFIG_PATH + 'configs/uplink_configs/modbus_uplink_converter_holding_registers_reading_little.json')
        telemetry_keys = [key['tag'] for slave in config['Modbus']['configurationJson']['master']['slaves'] for key in
                          slave['timeseries']]
        sleep(GENERAL_TIMEOUT)
        actual_values = self.client.get_latest_timeseries(self.device.id, ','.join(telemetry_keys))
        expected_values = self.load_configuration(
            self.CONFIG_PATH + 'test_values/uplink/holding_registers_values_little.json')

        for (_type, value) in expected_values.items():
            self.assertEqual(value, actual_values[_type][0]['value'],
                             f'Value is not equal for the next telemetry key: {_type}')

    def test_coils_reading_little_endian(self):
        """
        Test the function test_coils_reading_little_endian.

        This function is responsible for testing the functionality of the
        test_coils_reading_little_endian method. It performs the following steps:

        1. Changes the connector configuration.
        2. Retrieves telemetry keys.
        3. Waits for 2 seconds.
        4. Gets the latest timeseries.
        5. Loads the test values.
        6. Compares the values with the latest timeseries.

        Parameters:
        - self: The object instance.

        Returns:
        - None
        """

        (config, _) = self.change_connector_configuration(
            self.CONFIG_PATH + 'configs/uplink_configs/modbus_uplink_converter_coils_reading_little.json')
        telemetry_keys = [key['tag'] for slave in config['Modbus']['configurationJson']['master']['slaves'] for key in
                          slave['timeseries']]
        sleep(GENERAL_TIMEOUT)
        actual_values = self.client.get_latest_timeseries(self.device.id, ','.join(telemetry_keys))
        expected_values = self.load_configuration(
            self.CONFIG_PATH + 'test_values/uplink/discrete_and_coils_registers_values_little.json')

        for (_type, value) in expected_values.items():
            self.assertEqual(value, actual_values[_type][0]['value'],
                             f'Value is not equal for the next telemetry key: {_type}')

    def test_discrete_input_reading_little_endian(self):
        """
        Test reading little endian discrete input.

        Parameters:
        self: the object instance

        Returns:
        None
        """

        (config, _) = self.change_connector_configuration(
            self.CONFIG_PATH + 'configs/uplink_configs/modbus_uplink_converter_discrete_input_reading_little.json')
        telemetry_keys = [key['tag'] for slave in config['Modbus']['configurationJson']['master']['slaves'] for key in
                          slave['timeseries']]
        sleep(GENERAL_TIMEOUT)
        actual_values = self.client.get_latest_timeseries(self.device.id, ','.join(telemetry_keys))
        expected_values = self.load_configuration(
            self.CONFIG_PATH + 'test_values/uplink/discrete_and_coils_registers_values_little.json')

        for (_type, value) in expected_values.items():
            self.assertEqual(value, actual_values[_type][0]['value'],
                             f'Value is not equal for the next telemetry key: {_type}')

    def test_input_register_reading_big_endian(self):
        """
        Function to test the input register reading in big endian format.

        This function changes the connector configuration, retrieves telemetry keys,
        retrieves latest timeseries, loads configuration, and performs assertions.

        Parameters:
            self: the object instance

        Returns:
            None
        """

        (config, _) = self.change_connector_configuration(
            self.CONFIG_PATH + 'configs/uplink_configs/modbus_uplink_converter_input_registers_reading_big.json')
        telemetry_keys = [key['tag'] for slave in config['Modbus']['configurationJson']['master']['slaves'] for key in
                          slave['timeseries']]
        sleep(GENERAL_TIMEOUT)
        actual_values = self.client.get_latest_timeseries(self.device.id, ','.join(telemetry_keys))
        expected_values = self.load_configuration(
            self.CONFIG_PATH + 'test_values/uplink/input_registers_values_big.json')

        for (_type, value) in expected_values.items():
            self.assertEqual(value, actual_values[_type][0]['value'],
                             f'Value is not equal for the next telemetry key: {_type}')

    def test_holding_register_reading_big_endian(self):
        """
        Test for holding register reading in big endian format.

        Parameters:
        self (object): The instance of the test class.

        Returns:
        None
        """

        (config, _) = self.change_connector_configuration(
            self.CONFIG_PATH + 'configs/uplink_configs/modbus_uplink_converter_holding_registers_reading_big.json')
        telemetry_keys = [key['tag'] for slave in config['Modbus']['configurationJson']['master']['slaves'] for key in
                          slave['timeseries']]
        sleep(GENERAL_TIMEOUT)
        actual_values = self.client.get_latest_timeseries(self.device.id, ','.join(telemetry_keys))
        expected_values = self.load_configuration(
            self.CONFIG_PATH + 'test_values/uplink/holding_registers_values_big.json')

        for (_type, value) in expected_values.items():
            self.assertEqual(value, actual_values[_type][0]['value'],
                             f'Value is not equal for the next telemetry key: {_type}')

    def test_coils_reading_big_endian(self):
        """
        Test the reading of coils in big endian format.

        Parameters:
        - self: the object instance
        - No explicit parameters

        Returns:
        - No explicit return value
        """

        (config, _) = self.change_connector_configuration(
            self.CONFIG_PATH + 'configs/uplink_configs/modbus_uplink_converter_coils_reading_big.json')
        telemetry_keys = [key['tag'] for slave in config['Modbus']['configurationJson']['master']['slaves'] for key in
                          slave['timeseries']]
        sleep(GENERAL_TIMEOUT)
        actual_values = self.client.get_latest_timeseries(self.device.id, ','.join(telemetry_keys))
        expected_values = self.load_configuration(
            self.CONFIG_PATH + 'test_values/uplink/discrete_and_coils_registers_values_big.json')

        for (_type, value) in expected_values.items():
            self.assertEqual(value, actual_values[_type][0]['value'],
                             f'Value is not equal for the next telemetry key: {_type}')

    def test_discrete_input_reading_big_endian(self):
        """
        Test for reading discrete input in big endian format.

        Parameters:
        - self: the object instance
        - No input parameters

        Returns:
        - No return value
        """

        (config, _) = self.change_connector_configuration(
            self.CONFIG_PATH + 'configs/uplink_configs/modbus_uplink_converter_discrete_input_reading_big.json')
        telemetry_keys = [key['tag'] for slave in config['Modbus']['configurationJson']['master']['slaves'] for key in
                          slave['timeseries']]
        sleep(GENERAL_TIMEOUT)
        actual_values = self.client.get_latest_timeseries(self.device.id, ','.join(telemetry_keys))
        expected_values = self.load_configuration(
            self.CONFIG_PATH + 'test_values/uplink/discrete_and_coils_registers_values_big.json')

        for (_type, value) in expected_values.items():
            self.assertEqual(value, actual_values[_type][0]['value'],
                             f'Value is not equal for the next telemetry key: {_type}')


if __name__ == '__main__':
    unittest.main()
