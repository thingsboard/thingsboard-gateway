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

from unittest.mock import patch, MagicMock, AsyncMock
from bacpypes3.primitivedata import ObjectIdentifier
from tests.unit.connectors.bacnet.bacnet_base_test import BacnetBaseTestCase
from thingsboard_gateway.connectors.bacnet.device import Device
from concurrent.futures import TimeoutError
from threading import Thread


class BacnetOnAttributeUpdatesTestCase(BacnetBaseTestCase):

    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.connector._AsyncBACnetConnector__application = AsyncMock()
        self._loop_thread = Thread(target=self.connector.loop.run_forever, daemon=True)
        self._loop_thread.start()

    async def asyncTearDown(self):
        self.connector.loop.call_soon_threadsafe(self.connector.loop.stop)
        self._loop_thread.join(timeout=1)

        await super().asyncTearDown()

    async def test_retrieve_device_obj_by_name_success(self):
        payload = {'device': self.DEVICE_NAME, 'data': {'binaryValue2': True}}
        device = self.connector._AsyncBACnetConnector__get_device_by_name(payload=payload)
        self.assertIsInstance(device, Device)
        self.assertIs(device, self.device)
        self.assertEqual(device.name, self.DEVICE_NAME)

    async def test_get_device_by_name_missing_device_key(self):
        payload = {'data': {'binaryValue2': True}}

        device = self.connector._AsyncBACnetConnector__get_device_by_name(payload=payload)

        self.assertIsNone(device)

    async def test_retrieve_device_obj_on_non_existent_device(self):
        payload = {'device': 'test emulator device22', 'data': {'binaryValue2': True}}
        device = self.connector._AsyncBACnetConnector__get_device_by_name(payload=payload)
        self.assertIsNone(device)

    async def test_get_device_by_name_timeout(self):
        payload = {'device': self.DEVICE_NAME, 'data': {'binaryValue2': True}}
        with patch("asyncio.run_coroutine_threadsafe") as timeout_mock_task:
            fake_future = MagicMock()
            fake_future.result.side_effect = TimeoutError
            fake_future.done.return_value = False
            timeout_mock_task.return_value = fake_future
            device = self.connector._AsyncBACnetConnector__get_device_by_name(payload=payload)
        self.assertIsNone(device)
        fake_future.cancel.assert_called_once()

    async def test_update_when_device_has_no_mapping(self):
        payload = {"device": self.device.name, "data": {"binaryValue2": True}}
        await self.connector._AsyncBACnetConnector__devices.remove(self.device)
        self.device = self.create_fake_device(
            'attribute_updates/on_attribute_updates_bacnet_config_empty_section.json'
        )
        await self.connector._AsyncBACnetConnector__devices.add(self.device)

        with patch.object(self.connector, "_AsyncBACnetConnector__create_task") as create_task_mock, \
                self.assertLogs("Bacnet test", level="ERROR") as logcap:
            self.connector.on_attributes_update(payload)

        create_task_mock.assert_not_called()
        self.assertTrue(any("No attribute mapping found for device" in m for m in logcap.output))

    async def test_update_incorrect_object_id_mixed(self):
        payload = {"device": self.device.name, "data": {"binaryValue2": True, "binaryInput1": 56}}
        await self.connector._AsyncBACnetConnector__devices.remove(self.device)
        self.device = self.create_fake_device(
            "attribute_updates/on_attribute_updates_bacnet_config_incorrect_object_id.json"
        )
        await self.connector._AsyncBACnetConnector__devices.add(self.device)

        with patch.object(self.connector, "_AsyncBACnetConnector__create_task") as ct_mock, \
                self.assertLogs("Bacnet test", level="ERROR") as logcap:
            self.connector.on_attributes_update(content=payload)
        self.assertEqual(ct_mock.call_count, 1)
        func, args, kwargs = ct_mock.call_args.args
        addr, obj_id, prop_id, value = args

        self.assertEqual(str(addr), self.device.details.address)
        self.assertEqual(obj_id, ObjectIdentifier("binaryValue:2"))
        self.assertEqual(prop_id, "presentValue")
        self.assertTrue(value)

        self.assertIn("priority", kwargs)
        self.assertIn("result", kwargs)
        self.assertTrue(any("Such number of object id is not supported" in m for m in logcap.output))

    async def test_update_multiple_correct_attribute(self):
        payload = {"device": self.device.name, "data": {"binaryValue2": True, "binaryInput1": 56}}
        await self.connector._AsyncBACnetConnector__devices.remove(self.device)
        self.device = self.create_fake_device(
            "attribute_updates/on_attribute_updates_bacnet_connector_multiple_updates.json"
        )
        await self.connector._AsyncBACnetConnector__devices.add(self.device)

        with patch.object(self.connector, "_AsyncBACnetConnector__create_task") as ct_mock:
            self.connector.on_attributes_update(content=payload)

        self.assertEqual(ct_mock.call_count, 2)
        seen = []
        for c in ct_mock.call_args_list:
            _, args, kwargs = c.args
            addr, obj_id, prop_id, value = args
            seen.append((
                str(addr), obj_id, prop_id, value,
                "priority" in kwargs, "result" in kwargs
            ))

        expected = {
            (self.device.details.address, ObjectIdentifier("binaryValue:2"), "presentValue", True, True, True),
            (self.device.details.address, ObjectIdentifier("binaryInput:1"), "presentValue", 56, True, True),
        }

        self.assertEqual(set(seen), expected)

    async def test_update_attribute_update_with_incorrect_object_id_datatype(self):
        payload = {"device": self.device.name, "data": {"binaryValue2": True}}
        await self.connector._AsyncBACnetConnector__devices.remove(self.device)
        self.device = self.create_fake_device(
            "attribute_updates/on_attribute_updates_bacnet_config_invalid_data_type.json"
        )
        await self.connector._AsyncBACnetConnector__devices.add(self.device)

        with patch.object(self.connector, "_AsyncBACnetConnector__create_task") as ct_mock, \
                self.assertLogs("Bacnet test", level="ERROR") as logcap:
            self.connector.on_attributes_update(content=payload)

        ct_mock.assert_not_called()
        self.assertTrue(any("for key" in m for m in logcap.output))

    async def test_update_continues_when_first_task_raises(self):
        payload = {"device": self.device.name, "data": {"binaryValue2": True, "binaryInput1": 56}}
        await self.connector._AsyncBACnetConnector__devices.remove(self.device)
        self.device = self.create_fake_device(
            "attribute_updates/on_attribute_updates_bacnet_connector_multiple_updates.json"
        )
        await self.connector._AsyncBACnetConnector__devices.add(self.device)

        with patch.object(self.connector, "_AsyncBACnetConnector__create_task") as ct_mock, \
                self.assertLogs("Bacnet test", level="ERROR") as logcap:
            ct_mock.side_effect = [Exception(""), None]
            self.connector.on_attributes_update(payload)
        self.assertEqual(ct_mock.call_count, 2)
        
