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

import logging
from asyncio import Queue, new_event_loop
from os import path
from unittest import IsolatedAsyncioTestCase
from unittest.mock import MagicMock, patch, AsyncMock
from simplejson import load
from thingsboard_gateway.connectors.modbus.modbus_connector import AsyncModbusConnector
from thingsboard_gateway.connectors.modbus.slave import Slave

LOGNAME = "Modbus test"


class ModbusBaseTestCase(IsolatedAsyncioTestCase):
    CONFIG_PATH = path.join(path.dirname(path.abspath(__file__)), 'data')
    DEVICE_NAME = 'test_modbus_device'
    CONNECTOR_TYPE = 'modbus'

    async def asyncSetUp(self):
        self.connector: AsyncModbusConnector = AsyncModbusConnector.__new__(AsyncModbusConnector)
        self.connector._AsyncModbusConnector__log = logging.getLogger('Modbus test')
        self.connector.loop = new_event_loop()
        self.connector._AsyncModbusConnector__process_device_requests = Queue(1_000_000)
        self.connector._AsyncModbusConnector__slaves = []
        self.connector._AsyncModbusConnector__stopped = False
        await self.add_slaves(slaves_config=self.convert_json(
            config_path=path.join(self.CONFIG_PATH, 'attribute_updates/on_attribute_updates_modbus_config.json')).get(
            'slaves', []))

        if not hasattr(self.connector, '_AsyncModbusConnector__gateway'):
            self.connector._AsyncModbusConnector__gateway = MagicMock()

    async def asyncTearDown(self):
        log = logging.getLogger('Modbus test')
        for handler in list(log.handlers):
            log.removeHandler(handler)
        self.connector = None
        self.slave = None
        await super().asyncTearDown()

    async def add_slaves(self, slaves_config):
        for slave_config in slaves_config:
            try:
                self.add_slave(slave_config=slave_config)

            except Exception as e:
                self.connector._AsyncModbusConnector__log.error('Failed to add slave: %s', e)
        return

    def add_slave(self, slave_config):
        with patch.object(Slave, '_Slave__load_downlink_converter',
                          return_value=MagicMock(name='downlink')), \
                patch.object(Slave, '_Slave__load_uplink_converter',
                             return_value=MagicMock(name='uplink')), \
                patch(
                    'thingsboard_gateway.connectors.modbus.entities.bytes_uplink_converter_config.BytesUplinkConverterConfig') as MockCfg:
            mock_cfg = MockCfg.return_value
            mock_cfg.is_readable.return_value = False

            slave = Slave(self.connector, self.connector._AsyncModbusConnector__log, slave_config)

        self.connector._AsyncModbusConnector__slaves.append(slave)

    @staticmethod
    def convert_json(config_path):
        with open(config_path, 'r') as config_file:
            config = load(config_file)
        return config


class ServerSideRPCModbusSetUp(ModbusBaseTestCase):
    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.connector._AsyncModbusConnector__slaves.clear()
        await self.add_slaves(slaves_config=self.convert_json(
            config_path=path.join(self.CONFIG_PATH, 'server_side_rpc/modbus_server_side_rpc.json')).get(
            'slaves', []))

        self.slave = self.connector._AsyncModbusConnector__slaves[0]
        self.slave.connect = AsyncMock(return_value=True)
        self.slave.read = AsyncMock(return_value=78)
        self.slave.write = AsyncMock(return_value=78)
        self.slave.downlink_converter = MagicMock()
        self.slave.downlink_converter.convert.return_value = [78]
        self.slave.uplink_converter = MagicMock()

        self.logger = logging.getLogger(LOGNAME)
        self.connector._AsyncModbusConnector__log = self.logger
        self.connector._AsyncModbusConnector__get_name = LOGNAME

        self.gateway = self.connector._AsyncModbusConnector__gateway
        self.gateway.send_rpc_reply = MagicMock()

    async def asyncTearDown(self):
        self.slave = None
        await super().asyncTearDown()
