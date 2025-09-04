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

from threading import Thread
import asyncio
from unittest.mock import patch, MagicMock, AsyncMock
from tests.unit.connectors.modbus.modbus_base_test import ModbusBaseTestCase


class ModbusOnAttributeUpdatesTestCase(ModbusBaseTestCase):

    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.slave = self.connector._AsyncModbusConnector__slaves[0]
        self.slave.uplink_converter_config.is_readable = False
        self._loop_thread = Thread(target=self.connector.loop.run_forever, daemon=True)
        self._loop_thread.start()
        self.slave.downlink_converter = MagicMock(name="downlink")
        self.slave.connect = AsyncMock(return_value=True)
        self.slave.write = AsyncMock()

        def _create_task_on_connector_loop(coro, args, kwargs):
            return asyncio.run_coroutine_threadsafe(coro(*args, **kwargs), self.connector.loop)

        self._create_task_on_connector_loop = _create_task_on_connector_loop

    async def asyncTearDown(self):
        self.connector.loop.call_soon_threadsafe(self.connector.loop.stop)
        self._loop_thread.join(timeout=1)
        await super().asyncTearDown()

    async def test_on_attribute_update_success(self):
        payload = {'device': 'Demo Device', 'data': {'attr16': 333}}
        self.slave.downlink_converter.convert.return_value = [333]
        with patch.object(self.connector,
                          "_AsyncModbusConnector__create_on_attribute_update_task",
                          side_effect=self._create_task_on_connector_loop) as create_task_mock, \
                self.assertLogs("Modbus test", level="DEBUG") as logcap:
            self.connector.on_attributes_update(payload)

        self.assertEqual(create_task_mock.call_count, 1)
        func, args, kwargs = create_task_mock.call_args.args
        device, to_process, configuration = args
        self.assertIs(device, self.slave)
        self.assertEqual(configuration['tag'], 'attr16')
        self.assertEqual(configuration['functionCode'], 6)
        self.assertEqual(configuration['address'], 4)
        self.slave.write.assert_awaited_once_with(6, 4, 333)
        self.assertTrue(any("Attribute update processed successfully" in m for m in logcap.output))

    async def test_on_attribute_update_multiple_parameters(self):
        payload = {'device': 'Demo Device',
                   'data': {'attr16': 111, 'attrFloat32': 79.99}}
        self.slave.downlink_converter.convert.side_effect = [
            [111],
            [0x70A4, 0x4145]
        ]

        with patch.object(self.connector,
                          "_AsyncModbusConnector__create_on_attribute_update_task",
                          side_effect=self._create_task_on_connector_loop) as create_task_mock:
            self.connector.on_attributes_update(payload)
        self.assertEqual(create_task_mock.call_count, 2)
        self.slave.write.assert_any_await(6, 4, 111)
        self.slave.write.assert_any_await(16, 5, [0x70A4, 0x4145])
        self.assertEqual(self.slave.write.await_count, 2)

        call1_configuration = self.slave.downlink_converter.convert.call_args_list[0][0][0]
        call2_configuration = self.slave.downlink_converter.convert.call_args_list[1][0][0]
        self.assertEqual(call1_configuration.function_code, 6)
        self.assertEqual(call1_configuration.address, 4)
        self.assertEqual(call2_configuration.function_code, 16)
        self.assertEqual(call2_configuration.address, 5)

    async def test_on_attribute_update_no_attribute_update_mapping(self):
        self.slave.attributes_updates_config = []
        payload = {'device': 'Demo Device', 'data': {'attr16': 123}}

        with patch.object(self.connector,
                          "_AsyncModbusConnector__create_on_attribute_update_task") as ct_mock, \
                self.assertLogs("Modbus test", level="ERROR") as logcap:
            self.connector.on_attributes_update(payload)

        ct_mock.assert_not_called()
        self.slave.write.assert_not_awaited()
        self.assertTrue(any("No attribute mapping found" in m for m in logcap.output))

    async def test_on_attribute_update_no_matching_attribute_name_with_config(self):
        payload = {'device': 'Demo Device', 'data': {'invalid_attribute': 111}}

        with patch.object(self.connector,
                          "_AsyncModbusConnector__create_on_attribute_update_task") as ct_mock, \
                self.assertLogs("Modbus test", level="ERROR") as logcap:
            self.connector.on_attributes_update(payload)

        ct_mock.assert_not_called()
        self.slave.write.assert_not_awaited()
        self.assertTrue(any("No attributes found that match attributes section" in m for m in logcap.output))

    async def test_on_attribute_update_timeout(self):
        payload = {'device': 'Demo Device', 'data': {'attr16': 333}}
        fake_task = MagicMock()
        fake_task.done.return_value = False

        with patch.object(self.connector,
                          "_AsyncModbusConnector__create_on_attribute_update_task",
                          return_value=fake_task) as ct_mock, \
                patch.object(self.connector,
                             "_AsyncModbusConnector__wait_task_with_timeout",
                             return_value=(False, None)) as waiter_mock, \
                self.assertLogs("Modbus test", level="ERROR") as logcap:
            self.connector.on_attributes_update(payload)

        ct_mock.assert_called_once()
        waiter_mock.assert_called_once()
        self.slave.write.assert_not_awaited()
        self.assertTrue(any("timed out" in m.lower() for m in logcap.output))

    async def test_on_attribute_update_invalid_data_type(self):
        payload = {'device': 'Demo Device', 'data': {'attr16': 'abba'}}
        self.slave.downlink_converter.convert.side_effect = ValueError("bad cast")

        with patch.object(self.connector,
                          "_AsyncModbusConnector__create_on_attribute_update_task",
                          side_effect=self._create_task_on_connector_loop) as ct_mock, \
                self.assertLogs("Modbus test", level="ERROR") as logcap:
            self.connector.on_attributes_update(payload)

        self.assertEqual(ct_mock.call_count, 1)
        self.slave.write.assert_not_awaited()
        self.assertTrue(any("Could not process attribute update" in m for m in logcap.output))

    async def test_on_attribute_update_partial_failure_continues(self):
        payload = {'device': 'Demo Device',
                   'data': {'attr16': 'rrrr', 'attrFloat32': 80.1}}

        self.slave.downlink_converter.convert.side_effect = [
            ValueError("bad cast"),
            [0x70A4, 0x4145]
        ]

        with patch.object(self.connector,
                          "_AsyncModbusConnector__create_on_attribute_update_task",
                          side_effect=self._create_task_on_connector_loop) as ct_mock, \
                self.assertLogs("Modbus test", level="ERROR") as logcap:
            self.connector.on_attributes_update(payload)

        self.assertEqual(ct_mock.call_count, 2)
        self.slave.write.assert_any_await(16, 5, [0x70A4, 0x4145])
        self.assertTrue(any("Could not process attribute update" in m for m in logcap.output))
