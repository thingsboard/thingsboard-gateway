import unittest
from os import path
from time import sleep
from unittest.mock import Mock

from simplejson import load

from pymodbus.client.sync import ModbusTcpClient as ModbusClient

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


class ModbusTests(ModbusConnectorTestsBase):
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

        self.assertEqual(modbus_client_results, modbus_connector_results)

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

        self.assertEqual(modbus_client_results, modbus_connector_results)

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

        self.assertEqual(modbus_client_results, modbus_connector_results)

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


if __name__ == '__main__':
    unittest.main()
