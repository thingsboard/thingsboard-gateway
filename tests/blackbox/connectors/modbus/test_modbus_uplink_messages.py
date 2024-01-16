import unittest
from os import path

from simplejson import load

from tb_rest_client.rest_client_ce import *

TB_URL_CE = 'http://0.0.0.0:8080'

TB_TENANT_USERNAME_CE = 'tenant@thingsboard.org'
TB_TENANT_PASSWORD_CE = 'tenant'


class ModbusUplinkMessagesTest(unittest.TestCase):
    CONFIG_PATH = path.join(path.dirname(path.dirname(path.dirname(path.abspath(__file__)))),
                            "data" + path.sep + "configs" + path.sep + "modbus" + path.sep)

    client = None
    gateway = None
    device = None

    @classmethod
    def setUpClass(cls) -> None:
        # ThingsBoard REST API URL
        url = TB_URL_CE

        # Default Tenant Administrator credentials
        username = TB_TENANT_USERNAME_CE
        password = TB_TENANT_PASSWORD_CE

        with RestClientCE(url) as cls.client:
            cls.client.login(username, password)
            # assert cls.client.token is not None

            cls.gateway = cls.client.get_tenant_devices(10, 0, text_search='Gateway').data[0]
            assert cls.gateway is not None

            cls.device = cls.client.get_tenant_devices(10, 0, text_search='Temp Sensor').data[0]
            assert cls.device is not None

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
        sleep(2)
        return config, response

    def test_gateway_connection(self):
        """
        Test the gateway connection by asserting that the value returned by
        `get_attributes_by_scope` is `True`.

        Returns:
            None
        """

        self.assertEqual(self.client.get_attributes_by_scope(self.gateway.id, 'SERVER_SCOPE', 'active')[0]['value'],
                         True)

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
        latest_ts = self.client.get_latest_timeseries(self.device.id, ','.join(telemetry_keys))
        sleep(5)
        latest_ts_1 = self.client.get_latest_timeseries(self.device.id, ','.join(telemetry_keys))

        # check that timestamps are equal
        for ts_key in telemetry_keys:
            self.assertEqual(latest_ts[ts_key][0]['ts'], latest_ts_1[ts_key][0]['ts'],
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
            self.CONFIG_PATH + 'configs/modbus_uplink_converter_input_registers_reading_little.json')
        telemetry_keys = [key['tag'] for slave in config['Modbus']['configurationJson']['master']['slaves'] for key in
                          slave['timeseries']]
        sleep(2)
        latest_ts = self.client.get_latest_timeseries(self.device.id, ','.join(telemetry_keys))
        must_be_values = self.load_configuration(self.CONFIG_PATH + 'test_values/input_registers_values.json')

        for (_type, value) in must_be_values.items():
            self.assertEqual(value, latest_ts[_type][0]['value'],
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
            self.CONFIG_PATH + 'modbus_uplink_converter_holding_registers_reading_little.json')
        telemetry_keys = [key['tag'] for slave in config['Modbus']['configurationJson']['master']['slaves'] for key in
                          slave['timeseries']]
        sleep(2)
        latest_ts = self.client.get_latest_timeseries(self.device.id, ','.join(telemetry_keys))
        must_be_values = self.load_configuration(self.CONFIG_PATH + 'test_values/holding_registers_values.json')

        for (_type, value) in must_be_values.items():
            self.assertEqual(value, latest_ts[_type][0]['value'],
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
            self.CONFIG_PATH + 'modbus_uplink_converter_coils_reading_little.json')
        telemetry_keys = [key['tag'] for slave in config['Modbus']['configurationJson']['master']['slaves'] for key in
                          slave['timeseries']]
        sleep(2)
        latest_ts = self.client.get_latest_timeseries(self.device.id, ','.join(telemetry_keys))
        must_be_values = self.load_configuration(
            self.CONFIG_PATH + 'test_values/discrete_and_coils_registers_values.json')

        for (_type, value) in must_be_values.items():
            self.assertEqual(value, latest_ts[_type][0]['value'],
                             f'Value is not equal for the next telemetry key: {_type}')

    def test_discrete_input_reading_little_endian(self):
        pass

    def test_coils_reading_big_endian(self):
        pass

    def test_discrete_input_reading_big_endian(self):
        pass

    def test_input_register_reading_big_endian(self):
        pass

    def test_holding_register_reading_big_endian(self):
        pass


if __name__ == '__main__':
    unittest.main()
