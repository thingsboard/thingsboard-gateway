import asyncio
import logging
import unittest
from os import path
from time import sleep
from unittest.mock import Mock, patch

from pymodbus.pdu import ExceptionResponse
from simplejson import load

from thingsboard_gateway.connectors.modbus.entities.bytes_uplink_converter_config import BytesUplinkConverterConfig
from thingsboard_gateway.gateway.tb_client import TBClient
from thingsboard_gateway.tb_utility.tb_logger import TbLogger

try:
    from pymodbus.client import ModbusTcpClient as ModbusClient
except (ImportError, ModuleNotFoundError):
    from thingsboard_gateway.tb_utility.tb_utility import TBUtility
    TBUtility.install_package("pyserial_asyncio")
    TBUtility.install_package("pymodbus", version="3.0.0", force_install=True)
    from pymodbus.client import ModbusTcpClient as ModbusClient

from tests.base_test import BaseTest
from thingsboard_gateway.connectors.modbus.bytes_modbus_downlink_converter import BytesModbusDownlinkConverter
from thingsboard_gateway.connectors.modbus.bytes_modbus_uplink_converter import BytesModbusUplinkConverter
from thingsboard_gateway.gateway.tb_gateway_service import TBGatewayService
from thingsboard_gateway.connectors.modbus.modbus_connector import AsyncModbusConnector


class ModbusConnectorTestsBase(BaseTest):
    CONFIG_PATH = path.join(path.dirname(path.dirname(path.dirname(path.abspath(__file__)))),
                            "data" + path.sep + "modbus" + path.sep)

    def setUp(self) -> None:
        super().setUp()
        self.tb_client = Mock(spec=TBClient)
        self.gateway = Mock(spec=TBGatewayService)
        self.gateway.tb_client = self.tb_client
        self.tb_logger = Mock(spec=TbLogger)
        self.gateway.get_devices.return_value = []
        self.connector: AsyncModbusConnector = None
        self.config = None

    def tearDown(self):
        self.connector.close()
        super().tearDown()

    @patch('thingsboard_gateway.tb_utility.tb_logger.init_logger')
    def _create_connector(self, config_file_name, test_patch):
        test_patch.return_value = self.tb_logger
        try:
            with open(self.CONFIG_PATH + config_file_name, 'r', encoding="UTF-8") as file:
                self.config = load(file)
                self.config['master']['slaves'][0]['uplink_converter'] = BytesModbusUplinkConverter(
                    BytesUplinkConverterConfig(**{**self.config['master']['slaves'][0], 'deviceName': 'Test'}),
                    logger=self.tb_logger)
                self.config['master']['slaves'][0]['downlink_converter'] = BytesModbusDownlinkConverter(
                    {**self.config['master']['slaves'][0], 'deviceName': 'Test'}, logger=self.tb_logger)
                self.connector = AsyncModbusConnector(self.gateway, self.config, "modbus")
                self.connector._AsyncModbusConnector__log = Mock()
                self.connector.open()
                sleep(1)  # some time to init
        except Exception as e:
            self.log.error("Error occurred during creating connector: %s", e)


class ModbusReadRegisterTypesTests(ModbusConnectorTestsBase):
    client = None

    @classmethod
    def setUpClass(cls) -> None:
        cls.client = ModbusClient('localhost', port=5021)
        cls.client.connect()

    @classmethod
    def tearDownClass(cls) -> None:
        cls.client.close()

    def test_read_input_registers(self):
        self._create_connector('modbus_attributes.json')

        modbus_client_results = []
        attrs = self.connector._AsyncModbusConnector__config['master']['slaves'][0]['attributes'] # noqa
        for item in attrs:
            modbus_client_results.append(
                self.client.read_input_registers(item['address'], item['objectsCount'], slave=1).registers)

        modbus_connector_results = []
        for item in attrs:
            modbus_connector_results.append(asyncio.run(self.__get_available_functions()[4](address=item['address'],
                                                        count=item['objectsCount'],
                                                        unit_id=self.connector._AsyncModbusConnector__slaves[0].unit_id)).registers) # noqa

        for ir, ir1 in zip(modbus_client_results, modbus_connector_results):
            self.assertEqual(ir, ir1)

    def test_read_holding_registers(self):
        self._create_connector('modbus_attributes.json')
        modbus_client_results = []
        attrs = self.connector._AsyncModbusConnector__config['master']['slaves'][0]['attributes'] # noqa
        for item in attrs:
            modbus_client_results.append(
                self.client.read_holding_registers(item['address'], item['objectsCount'], slave=1).registers)

        modbus_connector_results = []
        for item in attrs:
            modbus_connector_results.append(asyncio.run(self.__get_available_functions()[3](address=item['address'],
                                                        count=item['objectsCount'],
                                                        unit_id=self.connector._AsyncModbusConnector__slaves[0].unit_id)).registers) # noqa

        for hr, hr1 in zip(modbus_client_results, modbus_connector_results):
            self.assertEqual(hr, hr1)

    def test_read_discrete_inputs(self):
        self._create_connector('modbus_attributes.json')
        modbus_client_results = []
        attrs = self.connector._AsyncModbusConnector__config['master']['slaves'][0]['attributes'] # noqa
        for item in attrs:
            result = self.client.read_discrete_inputs(item['address'], item['objectsCount'], slave=1)
            if not isinstance(result, ExceptionResponse):
                modbus_client_results.append(result.bits)
            else:
                modbus_client_results.append(str(result))

        modbus_connector_results = []
        for item in attrs:
            result = asyncio.run(self.__get_available_functions()[2](address=item['address'],
                                                        count=item['objectsCount'],
                                                        unit_id=self.connector._AsyncModbusConnector__slaves[0].unit_id)) # noqa
            if not isinstance(result, ExceptionResponse):
                modbus_connector_results.append(result.bits) # noqa
            else:
                modbus_connector_results.append(str(result))

        for rd, rd1 in zip(modbus_client_results, modbus_connector_results):
            self.assertEqual(rd, rd1)

    def test_read_coils_inputs(self):
        self._create_connector('modbus_attributes.json')
        modbus_client_results = []
        attrs = self.connector._AsyncModbusConnector__config['master']['slaves'][0]['attributes'] # noqa
        for item in attrs:
            rc = self.client.read_coils(item['address'], item['objectsCount'], slave=1)
            if rc and hasattr(rc, 'bits'):
                modbus_client_results.append(rc.bits)

        modbus_connector_results = []
        for item in attrs:
            rc = asyncio.run(self.__get_available_functions()[1](address=item['address'],
                                                     count=item['objectsCount'],
                                                     unit_id=self.connector._AsyncModbusConnector__slaves[0].unit_id)) # noqa
            if rc and hasattr(rc, 'bits'):
                modbus_connector_results.append(rc.bits)

        for rc, rc1 in zip(modbus_client_results, modbus_connector_results):
            self.assertEqual(rc, rc1)


    def __get_available_functions(self):
        return self.connector._AsyncModbusConnector__slaves[0].available_functions # noqa



class ModbusConnectorRpcTest(ModbusConnectorTestsBase):
    client = None

    @classmethod
    def setUpClass(cls) -> None:
        cls.client = ModbusClient('localhost', port=5021)
        cls.client.connect()

    @classmethod
    def tearDownClass(cls) -> None:
        cls.client.close()

    def test_write_type_rpc(self):
        self._create_connector('modbus_rpc.json')

        with open(self.CONFIG_PATH + 'modbus_rpc.json') as config_file:
            rpc_list = load(config_file)['master']['slaves'][0]['rpc']

        for rpc in rpc_list:
            first_value = self.client.read_input_registers(rpc['address'], rpc['objectsCount'], slave=1).registers
            test_rpc = {
                'device': 'MASTER Temp Sensor',
                'data': {
                    'id': 12,
                    'method': rpc['tag'],
                    'params': rpc['params']
                }
            }
            self.connector.server_side_rpc_handler(test_rpc)
            sleep(1)

            last_value = self.client.read_input_registers(rpc['address'], rpc['objectsCount'], slave=1).registers
            self.assertNotEqual(first_value, last_value)

    def test_deny_unknown_rpc(self):
        self._create_connector('modbus_rpc.json')
        first_value = self.client.read_input_registers(0, 2, slave=1).registers
        rpc = {
            'device': 'MASTER Temp Sensor',
            'data': {
                'id': 12,
                'method': 'setUnknown',
                'params': '1234'
            }
        }
        self.connector.server_side_rpc_handler(rpc)
        sleep(1)

        last_value = self.client.read_input_registers(0, 2, slave=1).registers
        self.assertEqual(first_value, last_value)


class ModbusConnectorAttributeUpdatesTest(ModbusConnectorTestsBase):
    client = None

    @classmethod
    def setUpClass(cls) -> None:
        cls.client = ModbusClient('localhost', port=5021)
        cls.client.connect()

    @classmethod
    def tearDownClass(cls) -> None:
        cls.client.close()

    def test_attribute_updates(self):
        self._create_connector('modbus_attribute_updates.json')

        with open(self.CONFIG_PATH + 'modbus_attribute_updates.json') as config_file:
            attribute_updates_list = load(config_file)['master']['slaves'][0]['attributeUpdates']

        for attribute_updates in attribute_updates_list:
            first_value = self.client.read_input_registers(attribute_updates['address'],
                                                             attribute_updates['objectsCount'],
                                                             slave=1).registers
            test_attribute_update = {
                'device': 'MASTER Temp Sensor',
                'data': {
                    attribute_updates['tag']: attribute_updates['params']
                }
            }
            self.connector.on_attributes_update(test_attribute_update)
            sleep(1)

            last_value = self.client.read_input_registers(attribute_updates['address'],
                                                            attribute_updates['objectsCount'],
                                                            slave=1).registers
            self.assertNotEqual(first_value, last_value)

    def test_deny_unknown_attribute_update(self):
        self._create_connector('modbus_attribute_updates.json')
        first_value = self.client.read_input_registers(0, 2, slave=1).registers

        test_attribute_update = {
            'device': 'MASTER Temp Sensor',
            'data': {
                'test_attribute': 'qwer'
            }
        }

        self.connector.on_attributes_update(test_attribute_update)
        sleep(1)

        last_value = self.client.read_input_registers(0, 2, slave=1).registers
        self.assertEqual(first_value, last_value)


if __name__ == '__main__':
    unittest.main()
