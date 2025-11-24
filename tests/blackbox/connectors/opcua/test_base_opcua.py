import logging
from time import time, sleep

from tests.base_test import BaseTest
from tb_rest_client import RestClientCE
from os import path
from simplejson import load

from tests.test_utils.gateway_device_util import GatewayDeviceUtil

CONNECTION_TIMEOUT = 300
DEVICE_CREATION_TIMEOUT = 120

LOG = logging.getLogger("TEST")
LOG.trace = LOG.debug


class BaseOpcuaTest(BaseTest):
    CONFIG_PATH = path.join(path.dirname(path.dirname(path.dirname(path.abspath(__file__)))),
                            "data" + path.sep + "opcua" + path.sep)
    CONNECTION_TIMEOUT = 300
    GENERAL_TIMEOUT = 10
    CONNECTOR_NAME = 'Opcua'

    client = None
    gateway = None
    device = None

    @classmethod
    def setUpClass(cls):
        super(BaseOpcuaTest, cls).setUpClass()

        # ThingsBoard REST API URL
        url = GatewayDeviceUtil.DEFAULT_URL

        # Default Tenant Administrator credentials
        username = GatewayDeviceUtil.DEFAULT_USERNAME
        password = GatewayDeviceUtil.DEFAULT_PASSWORD

        with RestClientCE(url) as cls.client:
            cls.client.login(username, password)

            cls.gateway = cls.client.get_tenant_devices(20, 0, text_search='Gateway').data[0]
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

            sleep(20)

            (config, _) = GatewayDeviceUtil.update_connector_config(
                cls.CONNECTOR_NAME,
                cls.CONFIG_PATH + 'configs/default_opcua_config.json')

            start_device_creation_time = time()
            while time() - start_device_creation_time < DEVICE_CREATION_TIMEOUT:
                try:
                    cls.device = cls.client.get_tenant_devices(20, 0, text_search='Temp Sensor').data[0]
                except IndexError:
                    sleep(1)
                else:
                    break

            assert cls.device is not None

    def setUp(self):
        super(BaseOpcuaTest, self).setUp()
        GatewayDeviceUtil.clear_connectors()
        sleep(self.GENERAL_TIMEOUT)

    @classmethod
    def tearDownClass(cls):
        GatewayDeviceUtil.clear_connectors()
        GatewayDeviceUtil.delete_device(cls.device.id)
        super(BaseOpcuaTest, cls).tearDownClass()
        sleep(2)

    @classmethod
    def load_configuration(cls, config_file_path):
        with open(config_file_path, 'r', encoding="UTF-8") as config:
            config = load(config)
        return config

    @classmethod
    def change_connector_configuration(cls, config_file_path):
        config = cls.load_configuration(config_file_path)
        config[cls.CONNECTOR_NAME]['ts'] = int(time() * 1000)
        response = cls.client.save_device_attributes(
            cls.gateway.id,
            'SHARED_SCOPE',
            config
        )
        sleep(cls.GENERAL_TIMEOUT)
        return config, response

    def reset_node_default_values(self, path_to_default_values=None):
        default_node_values = self.load_configuration(
            self.CONFIG_PATH + path_to_default_values)
        self.client.save_device_attributes(self.device.id, 'SHARED_SCOPE', default_node_values)

    def update_device_shared_attributes(self, config_file_path):
        config = self.load_configuration(self.CONFIG_PATH + config_file_path)
        self.client.save_device_attributes(self.device.id, 'SHARED_SCOPE', config)

    def update_device_and_connector_shared_attributes(self, connector_config_file_path, device_config_file_path):
        self.change_connector_configuration(self.CONFIG_PATH + connector_config_file_path)
        sleep(5)
        self.update_device_shared_attributes(device_config_file_path)
