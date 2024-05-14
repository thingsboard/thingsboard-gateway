from os import path
from time import time, sleep
import logging
from unittest import skip

from pymodbus.exceptions import ConnectionException
import pymodbus.client as ModbusClient
from tb_rest_client.rest_client_ce import *
from simplejson import load, loads

from tests.base_test import BaseTest
from tests.test_utils.gateway_device_util import GatewayDeviceUtil

CONNECTION_TIMEOUT = 300
DEVICE_CREATION_TIMEOUT = 200
GENERAL_TIMEOUT = 6

LOG = logging.getLogger("TEST")


class ModbusRpcTest(BaseTest):
    CONFIG_PATH = path.join(path.dirname(path.dirname(path.dirname(path.abspath(__file__)))),
                            "data" + path.sep + "modbus" + path.sep)

    client = None
    gateway = None
    device = None

    @classmethod
    def setUpClass(cls) -> None:
        super(ModbusRpcTest, cls).setUpClass()

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
        super(ModbusRpcTest, cls).tearDownClass()
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


class ModbusRpcReadingTest(ModbusRpcTest):
    def test_input_registers_reading_rpc_little(self):
        (config, _) = self.change_connector_configuration(
            self.CONFIG_PATH + 'configs/rpc_configs/input_registers_reading_rpc_little.json')
        sleep(GENERAL_TIMEOUT)
        expected_values = self.load_configuration(
            self.CONFIG_PATH + 'test_values/rpc/input_registers_values_reading_little.json')

        for rpc in config['Modbus']['configurationJson']['master']['slaves'][0]['rpc']:
            rpc_tag = rpc.pop('tag')
            result = self.client.handle_two_way_device_rpc_request(self.device.id,
                                                                   {
                                                                       "method": rpc_tag,
                                                                       "params": rpc,
                                                                       "timeout": 5000
                                                                   })
            self.assertEqual(result, expected_values[rpc_tag], f'Value is not equal for the next rpc: {rpc_tag}')

    def test_input_registers_reading_rpc_big(self):
        (config, _) = self.change_connector_configuration(
            self.CONFIG_PATH + 'configs/rpc_configs/input_registers_reading_rpc_big.json')
        sleep(GENERAL_TIMEOUT)
        expected_values = self.load_configuration(
            self.CONFIG_PATH + 'test_values/rpc/input_registers_values_reading_big.json')

        for rpc in config['Modbus']['configurationJson']['master']['slaves'][0]['rpc']:
            rpc_tag = rpc.pop('tag')
            result = self.client.handle_two_way_device_rpc_request(self.device.id,
                                                                   {
                                                                       "method": rpc_tag,
                                                                       "params": rpc,
                                                                       "timeout": 5000
                                                                   })
            self.assertEqual(result, expected_values[rpc_tag], f'Value is not equal for the next rpc: {rpc_tag}')

    def test_holding_registers_reading_rpc_little(self):
        (config, _) = self.change_connector_configuration(
            self.CONFIG_PATH + 'configs/rpc_configs/holding_registers_reading_rpc_little.json')
        sleep(GENERAL_TIMEOUT)
        expected_values = self.load_configuration(
            self.CONFIG_PATH + 'test_values/rpc/holding_registers_values_reading_little.json')

        for rpc in config['Modbus']['configurationJson']['master']['slaves'][0]['rpc']:
            rpc_tag = rpc.pop('tag')
            result = self.client.handle_two_way_device_rpc_request(self.device.id,
                                                                   {
                                                                       "method": rpc_tag,
                                                                       "params": rpc,
                                                                       "timeout": 5000
                                                                   })
            self.assertEqual(result, expected_values[rpc_tag], f'Value is not equal for the next rpc: {rpc_tag}')

    def test_holding_registers_reading_rpc_big(self):
        (config, _) = self.change_connector_configuration(
            self.CONFIG_PATH + 'configs/rpc_configs/holding_registers_reading_rpc_big.json')
        sleep(GENERAL_TIMEOUT)
        expected_values = self.load_configuration(
            self.CONFIG_PATH + 'test_values/rpc/holding_registers_values_reading_big.json')

        for rpc in config['Modbus']['configurationJson']['master']['slaves'][0]['rpc']:
            rpc_tag = rpc.pop('tag')
            result = self.client.handle_two_way_device_rpc_request(self.device.id,
                                                                   {
                                                                       "method": rpc_tag,
                                                                       "params": rpc,
                                                                       "timeout": 5000
                                                                   })
            self.assertEqual(result, expected_values[rpc_tag], f'Value is not equal for the next rpc: {rpc_tag}')

    def test_coils_reading_rpc_little(self):
        (config, _) = self.change_connector_configuration(
            self.CONFIG_PATH + 'configs/rpc_configs/coils_reading_rpc_little.json')
        sleep(GENERAL_TIMEOUT)
        expected_values = self.load_configuration(
            self.CONFIG_PATH + 'test_values/rpc/discrete_and_coils_registers_values_reading_little.json')

        for rpc in config['Modbus']['configurationJson']['master']['slaves'][0]['rpc']:
            rpc_tag = rpc.pop('tag')
            result = self.client.handle_two_way_device_rpc_request(self.device.id,
                                                                   {
                                                                       "method": rpc_tag,
                                                                       "params": rpc,
                                                                       "timeout": 5000
                                                                   })
            self.assertEqual(result, expected_values[rpc_tag], f'Value is not equal for the next rpc: {rpc_tag}')

    def test_coils_reading_rpc_big(self):
        (config, _) = self.change_connector_configuration(
            self.CONFIG_PATH + 'configs/rpc_configs/coils_reading_rpc_big.json')
        sleep(GENERAL_TIMEOUT)
        expected_values = self.load_configuration(
            self.CONFIG_PATH + 'test_values/rpc/discrete_and_coils_registers_values_reading_big.json')

        for rpc in config['Modbus']['configurationJson']['master']['slaves'][0]['rpc']:
            rpc_tag = rpc.pop('tag')
            result = self.client.handle_two_way_device_rpc_request(self.device.id,
                                                                   {
                                                                       "method": rpc_tag,
                                                                       "params": rpc,
                                                                       "timeout": 5000
                                                                   })
            self.assertEqual(result, expected_values[rpc_tag], f'Value is not equal for the next rpc: {rpc_tag}')

    def test_discrete_inputs_reading_rpc_little(self):
        (config, _) = self.change_connector_configuration(
            self.CONFIG_PATH + 'configs/rpc_configs/discrete_inputs_reading_rpc_little.json')
        sleep(GENERAL_TIMEOUT)
        expected_values = self.load_configuration(
            self.CONFIG_PATH + 'test_values/rpc/discrete_and_coils_registers_values_reading_little.json')

        for rpc in config['Modbus']['configurationJson']['master']['slaves'][0]['rpc']:
            rpc_tag = rpc.pop('tag')
            result = self.client.handle_two_way_device_rpc_request(self.device.id,
                                                                   {
                                                                       "method": rpc_tag,
                                                                       "params": rpc,
                                                                       "timeout": 5000
                                                                   })
            self.assertEqual(result, expected_values[rpc_tag], f'Value is not equal for the next rpc: {rpc_tag}')

    def test_discrete_inputs_reading_rpc_big(self):
        (config, _) = self.change_connector_configuration(
            self.CONFIG_PATH + 'configs/rpc_configs/discrete_inputs_reading_rpc_big.json')
        sleep(GENERAL_TIMEOUT)
        expected_values = self.load_configuration(
            self.CONFIG_PATH + 'test_values/rpc/discrete_and_coils_registers_values_reading_little.json')

        for rpc in config['Modbus']['configurationJson']['master']['slaves'][0]['rpc']:
            rpc_tag = rpc.pop('tag')
            result = self.client.handle_two_way_device_rpc_request(self.device.id,
                                                                   {
                                                                       "method": rpc_tag,
                                                                       "params": rpc,
                                                                       "timeout": 5000
                                                                   })
            self.assertEqual(result, expected_values[rpc_tag], f'Value is not equal for the next rpc: {rpc_tag}')


class ModbusRpcWritingTest(ModbusRpcTest):
    @skip('This test is not working properly due to ticket #IOTGW-88')
    def test_writing_input_registers_rpc_little(self):
        (config, _) = self.change_connector_configuration(
            self.CONFIG_PATH + 'configs/rpc_configs/input_registers_writing_rpc_little.json')
        sleep(5)
        expected_values = self.load_configuration(
            self.CONFIG_PATH + 'test_values/rpc/input_registers_values_writing_little.json')
        telemetry_keys = [key['tag'] for slave in config['Modbus']['configurationJson']['master']['slaves'] for key in
                          slave['timeseries']]

        for rpc in config['Modbus']['configurationJson']['master']['slaves'][0]['rpc']:
            rpc_tag = rpc.pop('tag')
            self.client.handle_two_way_device_rpc_request(self.device.id,
                                                          {
                                                              "method": rpc_tag,
                                                              "params": expected_values[rpc_tag],
                                                              "timeout": 5000
                                                          })
        sleep(GENERAL_TIMEOUT)
        latest_ts = self.client.get_latest_timeseries(self.device.id, ','.join(telemetry_keys))
        for (_type, value) in expected_values.items():
            if _type == 'bits' and _type == '4bits':
                latest_ts[_type][0]['value'] = loads(latest_ts[_type][0]['value'])
            else:
                value = str(value)

            self.assertEqual(value, latest_ts[_type][0]['value'],
                             f'Value is not equal for the next telemetry key: {_type}')

    def test_writing_input_registers_rpc_big(self):
        (config, _) = self.change_connector_configuration(
            self.CONFIG_PATH + 'configs/rpc_configs/input_registers_writing_rpc_big.json')
        sleep(GENERAL_TIMEOUT)
        expected_values = self.load_configuration(
            self.CONFIG_PATH + 'test_values/rpc/input_registers_values_writing_big.json')
        telemetry_keys = [key['tag'] for slave in config['Modbus']['configurationJson']['master']['slaves'] for key in
                          slave['timeseries']]

        for rpc in config['Modbus']['configurationJson']['master']['slaves'][0]['rpc']:
            rpc_tag = rpc.pop('tag')
            self.client.handle_two_way_device_rpc_request(self.device.id,
                                                          {
                                                              "method": rpc_tag,
                                                              "params": expected_values[rpc_tag],
                                                              "timeout": 5000
                                                          })
        sleep(GENERAL_TIMEOUT)
        latest_ts = self.client.get_latest_timeseries(self.device.id, ','.join(telemetry_keys))
        for (_type, value) in expected_values.items():
            if _type == 'bits':
                latest_ts[_type][0]['value'] = loads(latest_ts[_type][0]['value'])
            else:
                value = str(value)

            self.assertEqual(value, latest_ts[_type][0]['value'],
                             f'Value is not equal for the next telemetry key: {_type}')

    def test_writing_holding_registers_rpc_little(self):
        (config, _) = self.change_connector_configuration(
            self.CONFIG_PATH + 'configs/rpc_configs/holding_registers_writing_rpc_little.json')
        sleep(GENERAL_TIMEOUT)
        expected_values = self.load_configuration(
            self.CONFIG_PATH + 'test_values/rpc/holding_registers_values_writing_little.json')
        telemetry_keys = [key['tag'] for slave in config['Modbus']['configurationJson']['master']['slaves'] for key in
                          slave['timeseries']]

        for rpc in config['Modbus']['configurationJson']['master']['slaves'][0]['rpc']:
            rpc_tag = rpc.pop('tag')
            self.client.handle_two_way_device_rpc_request(self.device.id,
                                                          {
                                                              "method": rpc_tag,
                                                              "params": expected_values[rpc_tag],
                                                              "timeout": 5000
                                                          })
        sleep(GENERAL_TIMEOUT)
        latest_ts = self.client.get_latest_timeseries(self.device.id, ','.join(telemetry_keys))
        for (_type, value) in expected_values.items():
            if _type == 'bits':
                latest_ts[_type][0]['value'] = loads(latest_ts[_type][0]['value'])
            else:
                value = str(value)

            self.assertEqual(value, latest_ts[_type][0]['value'],
                             f'Value is not equal for the next telemetry key: {_type}')

    def test_writing_holding_registers_rpc_big(self):
        (config, _) = self.change_connector_configuration(
            self.CONFIG_PATH + 'configs/rpc_configs/holding_registers_writing_rpc_big.json')
        sleep(GENERAL_TIMEOUT)
        expected_values = self.load_configuration(
            self.CONFIG_PATH + 'test_values/rpc/holding_registers_values_writing_big.json')
        telemetry_keys = [key['tag'] for slave in config['Modbus']['configurationJson']['master']['slaves'] for key in
                          slave['timeseries']]

        for rpc in config['Modbus']['configurationJson']['master']['slaves'][0]['rpc']:
            rpc_tag = rpc.pop('tag')
            self.client.handle_two_way_device_rpc_request(self.device.id,
                                                          {
                                                              "method": rpc_tag,
                                                              "params": expected_values[rpc_tag],
                                                              "timeout": 5000
                                                          })
        sleep(GENERAL_TIMEOUT)
        latest_ts = self.client.get_latest_timeseries(self.device.id, ','.join(telemetry_keys))
        for (_type, value) in expected_values.items():
            if _type == 'bits':
                latest_ts[_type][0]['value'] = loads(latest_ts[_type][0]['value'])
            else:
                value = str(value)

            self.assertEqual(value, latest_ts[_type][0]['value'],
                             f'Value is not equal for the next telemetry key: {_type}')

    def test_writing_coils_rpc_little(self):
        (config, _) = self.change_connector_configuration(
            self.CONFIG_PATH + 'configs/rpc_configs/coils_writing_rpc_little.json')
        sleep(GENERAL_TIMEOUT)
        expected_values = self.load_configuration(
            self.CONFIG_PATH + 'test_values/rpc/discrete_and_coils_registers_values_writing_little.json')
        telemetry_keys = [key['tag'] for slave in config['Modbus']['configurationJson']['master']['slaves'] for key in
                          slave['timeseries']]

        for rpc in config['Modbus']['configurationJson']['master']['slaves'][0]['rpc']:
            rpc_tag = rpc.pop('tag')
            self.client.handle_two_way_device_rpc_request(self.device.id,
                                                          {
                                                              "method": rpc_tag,
                                                              "params": expected_values[rpc_tag],
                                                              "timeout": 5000
                                                          })
        sleep(GENERAL_TIMEOUT)
        latest_ts = self.client.get_latest_timeseries(self.device.id, ','.join(telemetry_keys))
        for (_type, value) in expected_values.items():
            if _type == 'bits':
                latest_ts[_type][0]['value'] = loads(latest_ts[_type][0]['value'])
            else:
                value = str(value)

            self.assertEqual(value, latest_ts[_type][0]['value'],
                             f'Value is not equal for the next telemetry key: {_type}')

    def test_writing_coils_rpc_big(self):
        (config, _) = self.change_connector_configuration(
            self.CONFIG_PATH + 'configs/rpc_configs/coils_writing_rpc_big.json')
        sleep(GENERAL_TIMEOUT)
        expected_values = self.load_configuration(
            self.CONFIG_PATH + 'test_values/rpc/discrete_and_coils_registers_values_writing_big.json')
        telemetry_keys = [key['tag'] for slave in config['Modbus']['configurationJson']['master']['slaves'] for key in
                          slave['timeseries']]

        for rpc in config['Modbus']['configurationJson']['master']['slaves'][0]['rpc']:
            rpc_tag = rpc.pop('tag')
            self.client.handle_two_way_device_rpc_request(self.device.id,
                                                          {
                                                              "method": rpc_tag,
                                                              "params": expected_values[rpc_tag],
                                                              "timeout": 5000
                                                          })
        sleep(GENERAL_TIMEOUT)
        latest_ts = self.client.get_latest_timeseries(self.device.id, ','.join(telemetry_keys))
        for (_type, value) in expected_values.items():
            if _type == 'bits':
                latest_ts[_type][0]['value'] = loads(latest_ts[_type][0]['value'])
            else:
                value = str(value)

            self.assertEqual(value, latest_ts[_type][0]['value'],
                             f'Value is not equal for the next telemetry key: {_type}')

    def test_writing_discrete_inputs_rpc_little(self):
        (config, _) = self.change_connector_configuration(
            self.CONFIG_PATH + 'configs/rpc_configs/discrete_inputs_writing_rpc_little.json')
        sleep(GENERAL_TIMEOUT)
        expected_values = self.load_configuration(
            self.CONFIG_PATH + 'test_values/rpc/discrete_and_coils_registers_values_writing_little.json')
        telemetry_keys = [key['tag'] for slave in config['Modbus']['configurationJson']['master']['slaves'] for key in
                          slave['timeseries']]

        for rpc in config['Modbus']['configurationJson']['master']['slaves'][0]['rpc']:
            rpc_tag = rpc.pop('tag')
            self.client.handle_two_way_device_rpc_request(self.device.id,
                                                          {
                                                              "method": rpc_tag,
                                                              "params": expected_values[rpc_tag],
                                                              "timeout": 5000
                                                          })
        sleep(GENERAL_TIMEOUT)
        latest_ts = self.client.get_latest_timeseries(self.device.id, ','.join(telemetry_keys))
        for (_type, value) in expected_values.items():
            if _type == 'bits':
                latest_ts[_type][0]['value'] = loads(latest_ts[_type][0]['value'])
            else:
                value = str(value)

            self.assertEqual(value, latest_ts[_type][0]['value'],
                             f'Value is not equal for the next telemetry key: {_type}')

    def test_writing_discrete_inputs_rpc_big(self):
        (config, _) = self.change_connector_configuration(
            self.CONFIG_PATH + 'configs/rpc_configs/discrete_inputs_writing_rpc_big.json')
        sleep(GENERAL_TIMEOUT)
        expected_values = self.load_configuration(
            self.CONFIG_PATH + 'test_values/rpc/discrete_and_coils_registers_values_writing_big.json')
        telemetry_keys = [key['tag'] for slave in config['Modbus']['configurationJson']['master']['slaves'] for key in
                          slave['timeseries']]

        for rpc in config['Modbus']['configurationJson']['master']['slaves'][0]['rpc']:
            rpc_tag = rpc.pop('tag')
            self.client.handle_two_way_device_rpc_request(self.device.id,
                                                          {
                                                              "method": rpc_tag,
                                                              "params": expected_values[rpc_tag],
                                                              "timeout": 5000
                                                          })
        sleep(GENERAL_TIMEOUT)
        latest_ts = self.client.get_latest_timeseries(self.device.id, ','.join(telemetry_keys))
        for (_type, value) in expected_values.items():
            if _type == 'bits':
                latest_ts[_type][0]['value'] = loads(latest_ts[_type][0]['value'])
            else:
                value = str(value)

            self.assertEqual(value, latest_ts[_type][0]['value'],
                             f'Value is not equal for the next telemetry key: {_type}')
