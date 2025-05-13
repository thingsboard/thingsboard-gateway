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
from time import time, sleep
import logging
from unittest import skip

from pymodbus.exceptions import ConnectionException
import pymodbus.client as ModbusClient
from tb_rest_client.rest_client_ce import *
from simplejson import load, loads
from twisted.words.protocols.jabber.jstrports import client

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

            cls.gateway = cls.client.get_tenant_devices(10, 0, text_search='Gateway').data[0]

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
            client.write_register(28, 10, 2)
        except ConnectionException:
            # will call pymodbus.exceptions.ConnectionException because of restarting the server
            pass

        client.close()
        super(ModbusRenameBaseTestClass, cls).tearDownClass()
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


class TestModbusRename(ModbusRenameBaseTestClass):

    def test_rename_device(self):
        new_device_name = "Temp Sensor1"
        self.device.name = new_device_name

        original_device_id = self.device.id
        self.client.save_device(body=self.device)
        device_after_rename = self.client.get_device_by_id(self.device.id)
        self.assertEqual(new_device_name, device_after_rename.name)
        self.assertEqual(original_device_id, device_after_rename.id)

    def test_no_new_device_appear_after_gateway_restart(self):
        new_device_name = "Temp Sensor1"
        self.device.name = new_device_name
        print(self.device)
        self.client.save_device(body=self.device)
        AVAILABLE_DEVICE_NAMES = ("Temp Sensor1", "Test Gateway device")
        self.client.handle_two_way_device_rpc_request(self.gateway.id, {"method": "gateway_restart"})
        start_time = time()
        sleep(15)
        while not GatewayDeviceUtil.is_gateway_connected(start_time):
            LOG.info('Gateway connecting to TB...')
            sleep(1)
        all_devices = self.client.get_tenant_devices(10, 0).data

        for index, element in enumerate(all_devices):
            self.assertIn(element.name, AVAILABLE_DEVICE_NAMES)

    def test_no_new_device_appear_after_deletion(self):
        AVAILABLE_DEVICE_NAMES = ("Test Gateway device")
        new_device_name = "Temp Sensor1"

        self.device.name = new_device_name
        self.client.save_device(body=self.device)
        self.client.delete_device(self.device.id)
        all_devices = self.client.get_tenant_devices(10, 0).data
        all_devices_names = [device.name for device in all_devices]
        self.assertFalse(
            any(name not in AVAILABLE_DEVICE_NAMES for name in all_devices_names),
            f"Found invalid device names: {[n for n in all_devices_names if n not in AVAILABLE_DEVICE_NAMES]}"
        )

    def test_no_new_device_remain_after_deletion_and_gateway_restart(self):
        AVAILABLE_DEVICE_NAMES = ("Test Gateway device")
        new_device_name = "Temp Sensor1"

        self.device.name = new_device_name
        self.client.save_device(body=self.device)
        self.client.delete_device(self.device.id)
        all_devices = self.client.get_tenant_devices(10, 0).data
        all_devices_names = [device.name for device in all_devices]
        unexpected = set(all_devices_names) - set(AVAILABLE_DEVICE_NAMES)
        self.assertFalse(unexpected, f"Found unexpected devices: {unexpected}")
        self.client.handle_two_way_device_rpc_request(self.gateway.id, {"method": "gateway_restart"})
        start_time = time()
        sleep(15)
        while not GatewayDeviceUtil.is_gateway_connected(start_time):
            LOG.info('Gateway connecting to TB...')
            sleep(1)
        all_devices = self.client.get_tenant_devices(10, 0).data
        all_devices_names = [device.name for device in all_devices]
        self.assertFalse(
            any(name not in AVAILABLE_DEVICE_NAMES for name in all_devices_names),
            f"Found invalid device names: {[n for n in all_devices_names if n not in AVAILABLE_DEVICE_NAMES]}"
        )
