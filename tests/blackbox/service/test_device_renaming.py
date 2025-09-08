#     Copyright 2025. ThingsBoard
#
#     Licensed under the Apache License, Version 2.0 (the "License");
#     you may not use this file except in compliance with the License.
#     You may obtain a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#     Unless required by applicable law or agreed to in writing, software
#     distributed under the License is distributed on an "AS IS" BASIS,
#     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#     See the License for the specific language governing permissions and
#     limitations under the License.

from os import path
import logging
from time import sleep, time

from pymodbus.exceptions import ConnectionException
import pymodbus.client as ModbusClient
from tb_rest_client.rest import ApiException
from tb_rest_client.rest_client_ce import RestClientCE
from simplejson import load, loads

from tests.base_test import BaseTest
from tests.test_utils.gateway_device_util import GatewayDeviceUtil

CONNECTION_TIMEOUT = 300
DEVICE_CREATION_TIMEOUT = 200
GENERAL_TIMEOUT = 6

LOG = logging.getLogger("TEST")
LOG.trace = LOG.debug


class ModbusRenameBaseTestClass(BaseTest):
    CONFIG_PATH = path.join(path.dirname(path.dirname(path.dirname(path.abspath(__file__)))),
                            "blackbox" + path.sep + "data" + path.sep + "modbus" + path.sep)

    client = None
    gateway = None
    device = None

    @classmethod
    def setUpClass(cls) -> None:
        super(ModbusRenameBaseTestClass, cls).setUpClass()

        # ThingsBoard REST API URL
        url = GatewayDeviceUtil.DEFAULT_URL

        # Default Tenant Administrator credentials
        username = GatewayDeviceUtil.DEFAULT_USERNAME
        password = GatewayDeviceUtil.DEFAULT_PASSWORD

        with RestClientCE(url) as cls.client:
            cls.client.login(username, password)

            cls.gateway = cls.client.get_tenant_devices(10, 0, text_search='Test Gateway device').data[0]

            assert cls.gateway is not None

            start_connecting_time = time()

            gateway_connected = GatewayDeviceUtil.is_gateway_connected(start_connecting_time)
            while not gateway_connected:
                LOG.info('Gateway connecting to TB...')
                gateway_connected = GatewayDeviceUtil.is_gateway_connected(start_connecting_time)
                if gateway_connected:
                    break
                sleep(1)
                if time() - start_connecting_time > CONNECTION_TIMEOUT:
                    raise TimeoutError('Gateway is not connected to TB')

            LOG.info('Gateway connected to TB')

            sleep(3)

            (config, _) = cls.change_connector_configuration(
                cls.CONFIG_PATH + 'modbus_rename_configs/default_modbus_config.json')

            start_device_creation_time = time()
            while time() - start_device_creation_time < DEVICE_CREATION_TIMEOUT:
                try:
                    cls.device = cls.client.get_tenant_devices(10, 0, text_search='Temp Sensor').data[0]
                except IndexError:
                    sleep(1)
                else:
                    break

            assert cls.device is not None
        cls.GATEWAY_DEVICE_NAME = cls.gateway.name

    def rename_device(self, new_name: str):
        try:
            self.device = self.client.get_device_by_id(self.device.id)
            self.device.name = new_name
            updated_device = self.client.save_device(body=self.device)
            return updated_device
        except ConnectionException as e:
            LOG.debug("Connection exception: %s", e)

    def update_device_shared_attributes(self, config_file_path):
        config = self.load_configuration(self.CONFIG_PATH + config_file_path)
        self.client.save_device_attributes(self.device.id, 'SHARED_SCOPE', config)

    def update_device_and_connector_shared_attributes(self, connector_config_file_path, device_config_file_path):
        self.change_connector_configuration(self.CONFIG_PATH + connector_config_file_path)
        self.update_device_shared_attributes(device_config_file_path)

    @classmethod
    def tearDownClass(cls):
        GatewayDeviceUtil.clear_connectors()
        try:
            cls.client.delete_device(cls.device.id)
        except ApiException as e:
            LOG.info("Api Exception during removing device: ", exc_info=e)

        client = ModbusClient.ModbusTcpClient('localhost', port=5021)
        client.connect()
        try:
            # trigger register 28 to restart the modbus server
            client.write_register(address=28, value=10, slave=2)
        except ConnectionException:
            # will call pymodbus.exceptions.ConnectionException because of restarting the server
            pass

        client.close()
        super(ModbusRenameBaseTestClass, cls).tearDownClass()
        sleep(2)

    def setUp(self):
        self.change_connector_configuration(self.CONFIG_PATH + 'modbus_rename_configs/default_modbus_config.json')
        start_device_creation_time = time()
        while time() - start_device_creation_time < DEVICE_CREATION_TIMEOUT:
            try:

                self.device = self.client.get_tenant_devices(10, 0, text_search='Temp Sensor').data[0]

            except IndexError:
                sleep(1)

            else:
                break

        assert self.device is not None

    def tearDown(self):
        GatewayDeviceUtil.clear_connectors()

        if self.device is not None:
            self.client.delete_device(self.device.id)

        GatewayDeviceUtil.restart_gateway()
        self.device = None

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


class TestModbusRename(ModbusRenameBaseTestClass):

    def test_no_device_appear_after_rename(self):
        old_device_name = self.device.name
        original_device_id = self.device.id
        renamed = self.rename_device("Renamed device")
        self.assertEqual(original_device_id, renamed.id)

        GatewayDeviceUtil.restart_gateway()

        all_device_names = [device.name for device in self.client.get_tenant_devices(10, 0).data if
                            not device.name == self.GATEWAY_DEVICE_NAME]
        self.assertNotIn(old_device_name, all_device_names)

    def test_delete_device(self):
        old_device_name = self.device.name
        self.rename_device("Renamed device")
        GatewayDeviceUtil.delete_device(self.device.id)
        all_device_names = [device.name for device in self.client.get_tenant_devices(10, 0).data if
                            not device.name == self.GATEWAY_DEVICE_NAME]
        self.assertNotIn("Renamed device", all_device_names)
        self.device = None

        GatewayDeviceUtil.restart_gateway()
        self.device = self.client.get_tenant_devices(10, 0, text_search='Temp Sensor').data[0]

        names = [device.name for device in self.client.get_tenant_devices(10, 0).data if
                 not device.name == self.GATEWAY_DEVICE_NAME]

        self.assertNotIn("Renamed device", names)
        self.assertNotIn(old_device_name, all_device_names)

    def test_device_reads_rpc_after_rename(self):

        config, _ = self.change_connector_configuration(
            self.CONFIG_PATH + 'modbus_rename_configs/modbus_uplink_converter_input_registers_reading_little.json'
        )
        updated_device = self.rename_device("Renamed device")
        sleep(GENERAL_TIMEOUT)

        expected = self.load_configuration(
            self.CONFIG_PATH + 'test_values/rpc/input_registers_values_reading_big.json'
        )
        for rpc_conf in config['Modbus']['configurationJson']['master']['slaves'][0]['rpc']:
            tag = rpc_conf.pop('tag')
            result = self.client.handle_two_way_device_rpc_request(
                updated_device.id,
                {"method": tag, "params": rpc_conf, "timeout": 5000}
            )
            self.assertEqual(
                result.get("result"),
                {'result': expected[tag]},
                f"RPC '{tag}' returned unexpected value"
            )

    def test_device_writes_rpc_after_rename(self):
        (config, _) = self.change_connector_configuration(
            self.CONFIG_PATH + 'configs/rpc_configs/holding_registers_writing_rpc_little.json')
        updated_device = self.rename_device("Renamed device")
        sleep(GENERAL_TIMEOUT)
        expected_values = self.load_configuration(
            self.CONFIG_PATH + 'test_values/rpc/holding_registers_values_writing_little.json')
        telemetry_keys = [key['tag'] for slave in config['Modbus']['configurationJson']['master']['slaves'] for key in
                          slave['timeseries']]

        for rpc in config['Modbus']['configurationJson']['master']['slaves'][0]['rpc']:
            rpc_tag = rpc.pop('tag')
            self.client.handle_two_way_device_rpc_request(updated_device.id,
                                                          {
                                                              "method": rpc_tag,
                                                              "params": expected_values[rpc_tag],
                                                              "timeout": 5000
                                                          })
        sleep(GENERAL_TIMEOUT)
        latest_ts = self.client.get_latest_timeseries(updated_device.id, ','.join(telemetry_keys))
        for (_type, value) in expected_values.items():
            if _type == 'bits':
                latest_ts[_type][0]['value'] = loads(latest_ts[_type][0]['value'])
            else:
                value = str(value)

            self.assertEqual(value, latest_ts[_type][0]['value'],
                             f'Value is not equal for the next telemetry key: {_type}')

    def test_device_reads_telemetry_after_rename(self):
        config, _ = self.change_connector_configuration(
            self.CONFIG_PATH + 'modbus_rename_configs/modbus_uplink_converter_input_registers_reading_little.json'
        )
        updated_device = self.rename_device("Renamed device")
        sleep(GENERAL_TIMEOUT)
        keys = [k['tag'] for slave in config['Modbus']['configurationJson']['master']['slaves'] for k in
                slave['timeseries']]

        actual = self.client.get_latest_timeseries(updated_device.id, ','.join(keys))
        expected = self.load_configuration(
            self.CONFIG_PATH + 'test_values/uplink/input_registers_values_little.json'
        )
        for tag, val in expected.items():
            self.assertEqual(
                val,
                actual[tag][0]['value'],
                f"Telemetry '{tag}' mismatch after rename"
            )

    def test_device_update_shared_attributes_after_rename(self):
        self.update_device_and_connector_shared_attributes(
            'configs/attrs_update_configs/attrs_update_input_registers_little.json',
            'test_values/attrs_update/input_registers_values_little.json'
        )
        sleep(GENERAL_TIMEOUT)

        expected = self.load_configuration(
            self.CONFIG_PATH + 'test_values/attrs_update/input_registers_values_little.json'
        )
        updated_device = self.rename_device("Renamed device")
        actual = self.client.get_latest_timeseries(
            updated_device.id,
            ','.join(expected.keys())
        )
        for tag, val in expected.items():
            if tag == 'bits':
                actual[tag][0]['value'] = loads(actual[tag][0]['value'])
            self.assertEqual(
                val,
                actual[tag][0]['value'],
                f"Attribute '{tag}' mismatch after rename"
            )
        self.update_device_shared_attributes("test_values/default_slave_values.json")
