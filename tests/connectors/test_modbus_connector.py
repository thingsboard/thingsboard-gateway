import unittest
from os import path
from time import sleep
from unittest.mock import Mock

from simplejson import load

from pymodbus.client import ModbusTcpClient as ModbusClient

import thingsboard_gateway
from thingsboard_gateway.connectors.modbus.modbus_connector import ModbusConnector


class ModbusConnectorTestsBase(unittest.TestCase):
    CONFIG_PATH = path.join(path.dirname(path.dirname(path.abspath(__file__))),
                            "data" + path.sep + "modbus" + path.sep)

    def setUp(self) -> None:
        self.gateway = Mock(spec=thingsboard_gateway.TBGatewayService)
        self.connector = None
        self.config = None

    def tearDown(self):
        self.connector.close()

    def _create_connector(self, config_file_name):
        with open(self.CONFIG_PATH + config_file_name, 'r', encoding="UTF-8") as file:
            self.config = load(file)
            self.connector = ModbusConnector(self.gateway, self.config, "modbus")
            self.connector.open()
            sleep(1)  # some time to init


class ModbusReadRegisterTypesTests(ModbusConnectorTestsBase):
    def test_read_input_registers(self):
        self._create_connector('modbus_attributes.json')
        client = ModbusClient('localhost', port=5021)
        modbus_client_results = []
        attrs = self.connector._ModbusConnector__config['master']['slaves'][0]['attributes']
        for item in attrs:
            modbus_client_results.append(
                client.read_input_registers(item['address'], item['objectsCount'], unit=0x01).registers)

        client.close()

        modbus_connector_results = []
        for item in attrs:
            modbus_connector_results.append(self.connector._ModbusConnector__function_to_device(
                self.connector._ModbusConnector__slaves[0],
                {'address': item['address'], 'objectsCount': item['objectsCount'],
                 'functionCode': 4}).registers)

        for ir, ir1 in zip(modbus_client_results, modbus_connector_results):
            self.assertEqual(ir, ir1)

    def test_read_holding_registers(self):
        self._create_connector('modbus_attributes.json')
        client = ModbusClient('localhost', port=5021)
        modbus_client_results = []
        attrs = self.connector._ModbusConnector__config['master']['slaves'][0]['attributes']
        for item in attrs:
            modbus_client_results.append(
                client.read_holding_registers(item['address'], item['objectsCount'], unit=0x01).registers)

        client.close()

        modbus_connector_results = []
        for item in attrs:
            modbus_connector_results.append(self.connector._ModbusConnector__function_to_device(
                self.connector._ModbusConnector__slaves[0],
                {'address': item['address'], 'objectsCount': item['objectsCount'],
                 'functionCode': 3}).registers)

        for hr, hr1 in zip(modbus_client_results, modbus_connector_results):
            self.assertEqual(hr, hr1)

    def test_read_discrete_inputs(self):
        self._create_connector('modbus_attributes.json')
        client = ModbusClient('localhost', port=5021)
        modbus_client_results = []
        attrs = self.connector._ModbusConnector__config['master']['slaves'][0]['attributes']
        for item in attrs:
            modbus_client_results.append(
                client.read_discrete_inputs(item['address'], item['objectsCount'], unit=0x01).bits)

        client.close()

        modbus_connector_results = []
        for item in attrs:
            modbus_connector_results.append(self.connector._ModbusConnector__function_to_device(
                self.connector._ModbusConnector__slaves[0],
                {'address': item['address'], 'objectsCount': item['objectsCount'],
                 'functionCode': 2}).bits)

        for rd, rd1 in zip(modbus_client_results, modbus_connector_results):
            self.assertEqual(rd, rd1)

    def test_read_coils_inputs(self):
        self._create_connector('modbus_attributes.json')
        client = ModbusClient('localhost', port=5021)
        modbus_client_results = []
        attrs = self.connector._ModbusConnector__config['master']['slaves'][0]['attributes']
        for item in attrs:
            rc = client.read_coils(item['address'], item['objectsCount'], unit=0x01)
            if rc and hasattr(rc, 'bits'):
                modbus_client_results.append(rc.bits)

        client.close()

        modbus_connector_results = []
        for item in attrs:
            rc = self.connector._ModbusConnector__function_to_device(
                self.connector._ModbusConnector__slaves[0],
                {'address': item['address'], 'objectsCount': item['objectsCount'],
                 'functionCode': 1})
            if rc and hasattr(rc, 'bits'):
                modbus_connector_results.append(rc.bits)

        for rc, rc1 in zip(modbus_client_results, modbus_connector_results):
            self.assertEqual(rc, rc1)


class ModbusConnectorRpcTest(ModbusConnectorTestsBase):
    def test_write_type_rpc(self):
        self._create_connector('modbus_rpc.json')
        client = ModbusClient('localhost', port=5021)

        with open(self.CONFIG_PATH + 'modbus_rpc.json') as config_file:
            rpc_list = load(config_file)['master']['slaves'][0]['rpc']

        for rpc in rpc_list:
            first_value = client.read_input_registers(rpc['address'], rpc['objectsCount'], unit=0x01).registers
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

            last_value = client.read_input_registers(rpc['address'], rpc['objectsCount'], unit=0x01).registers
            self.assertNotEqual(first_value, last_value)

        client.close()

    def test_deny_unknown_rpc(self):
        self._create_connector('modbus_rpc.json')
        client = ModbusClient('localhost', port=5021)
        first_value = client.read_input_registers(0, 2, unit=0x01).registers
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

        last_value = client.read_input_registers(0, 2, unit=0x01).registers
        self.assertEqual(first_value, last_value)
        client.close()


class ModbusConnectorAttributeUpdatesTest(ModbusConnectorTestsBase):
    def test_attribute_updates(self):
        self._create_connector('modbus_attribute_updates.json')
        client = ModbusClient('localhost', port=5021)

        with open(self.CONFIG_PATH + 'modbus_attribute_updates.json') as config_file:
            attribute_updates_list = load(config_file)['master']['slaves'][0]['attributeUpdates']

        for attribute_updates in attribute_updates_list:
            first_value = client.read_input_registers(attribute_updates['address'], attribute_updates['objectsCount'],
                                                      unit=0x01).registers
            test_attribute_update = {
                'device': 'MASTER Temp Sensor',
                'data': {
                    attribute_updates['tag']: attribute_updates['params']
                }
            }
            self.connector.on_attributes_update(test_attribute_update)
            sleep(1)

            last_value = client.read_input_registers(attribute_updates['address'], attribute_updates['objectsCount'],
                                                     unit=0x01).registers
            self.assertNotEqual(first_value, last_value)

        client.close()

    def test_deny_unknown_attribute_update(self):
        self._create_connector('modbus_attribute_updates.json')
        client = ModbusClient('localhost', port=5021)
        first_value = client.read_input_registers(0, 2, unit=0x01).registers

        test_attribute_update = {
            'device': 'MASTER Temp Sensor',
            'data': {
                'test_attribute': 'qwer'
            }
        }

        self.connector.on_attributes_update(test_attribute_update)
        sleep(1)

        last_value = client.read_input_registers(0, 2, unit=0x01).registers
        self.assertEqual(first_value, last_value)
        client.close()


if __name__ == '__main__':
    unittest.main()
