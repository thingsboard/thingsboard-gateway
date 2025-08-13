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

    async def asyncTearDown(self):
        await super().asyncTearDown()

    async def test_retrieve_device_by_name_success(self):
        payload = {"device": self.DEVICE_NAME, "data": {"binaryValue2": True}}

        fake_task = MagicMock()
        fake_task.done.return_value = True
        fake_task.result.return_value = self.device  # success

        with patch.object(self.connector.loop, "create_task", return_value=fake_task) as ct_mock:
            device = self.connector._AsyncBACnetConnector__get_device_by_name(payload=payload)

        ct_mock.assert_called_once()
        self.assertIsInstance(device, Device)
        self.assertIs(device, self.device)
        self.assertEqual(device.name, self.DEVICE_NAME)

    async def test_get_device_by_name_missing_device_key(self):
        payload = {"data": {"binaryValue2": True}}
        fake_task = MagicMock()
        fake_task.done.return_value = True
        fake_task.result.return_value = None

        with patch.object(self.connector.loop, "create_task", return_value=fake_task), \
                self.assertLogs("Bacnet test", level="ERROR") as logcap:
            device = self.connector._AsyncBACnetConnector__get_device_by_name(payload=payload)

        self.assertIsNone(device)
        self.assertTrue(any("does not contain a device name" in m for m in logcap.output))

    async def test_retrieve_device_on_non_existent_device(self):
        payload = {"device": "non-existent-device", "data": {"binaryValue2": True}}

        fake_task = MagicMock()
        fake_task.done.return_value = True
        fake_task.result.return_value = None

        with patch.object(self.connector.loop, "create_task", return_value=fake_task) as ct_mock:
            device = self.connector._AsyncBACnetConnector__get_device_by_name(payload=payload)

        ct_mock.assert_called_once()
        self.assertIsNone(device)

    async def test_get_device_by_name_timeout(self):
        payload = {"device": self.DEVICE_NAME, "data": {"binaryValue2": True}}
        fake_task = MagicMock()
        fake_task.done.return_value = False

        with patch.object(self.connector.loop, "create_task", return_value=fake_task) as ct_mock, \
                patch.object(self.connector, "_AsyncBACnetConnector__wait_task_with_timeout",
                             return_value=(False, None)) as waiter_mock, \
                self.assertLogs("Bacnet test", level="DEBUG") as logcap:
            device = self.connector._AsyncBACnetConnector__get_device_by_name(payload=payload)

        ct_mock.assert_called_once()
        waiter_mock.assert_called_once()
        self.assertIsNone(device)
        self.assertTrue(any("look up task failed on timeout" in m for m in logcap.output))

    async def test_update_attribute_update_with_device_not_found(self):
        self.connector._AsyncBACnetConnector__get_device_by_name = MagicMock()
        self.connector._AsyncBACnetConnector__get_device_by_name.return_value = None
        payload = {'device': 'test emulator device22', 'data': {'binaryValue2': True}}
        with patch.object(self.connector, "_AsyncBACnetConnector__create_task") as create_task_mock, \
                self.assertLogs("Bacnet test", level="ERROR") as logcap:
            self.connector.on_attributes_update(payload)
        create_task_mock.assert_not_called()

    async def test_update_when_device_has_no_mapping(self):
        payload = {"device": self.device.name, "data": {"binaryValue2": True}}
        await self.connector._AsyncBACnetConnector__devices.remove(self.device)
        self.device = self.create_fake_device(
            'attribute_updates/on_attribute_updates_bacnet_config_empty_section.json'
        )
        await self.connector._AsyncBACnetConnector__devices.add(self.device)
        self.connector._AsyncBACnetConnector__get_device_by_name = MagicMock()
        self.connector._AsyncBACnetConnector__get_device_by_name.return_value = self.device

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
        self.connector._AsyncBACnetConnector__get_device_by_name = MagicMock()
        self.connector._AsyncBACnetConnector__get_device_by_name.return_value = self.device

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
        self.assertTrue(any("Such number of object id is not supported" in m for m in logcap.output))

    async def test_update_multiple_correct_attribute(self):
        payload = {"device": self.device.name, "data": {"binaryValue2": True, "binaryInput1": 56}}
        await self.connector._AsyncBACnetConnector__devices.remove(self.device)
        self.device = self.create_fake_device(
            "attribute_updates/on_attribute_updates_bacnet_connector_multiple_updates.json"
        )
        await self.connector._AsyncBACnetConnector__devices.add(self.device)
        self.connector._AsyncBACnetConnector__get_device_by_name = MagicMock()
        self.connector._AsyncBACnetConnector__get_device_by_name.return_value = self.device

        with patch.object(self.connector, "_AsyncBACnetConnector__create_task") as ct_mock:
            self.connector.on_attributes_update(content=payload)

        self.assertEqual(ct_mock.call_count, 2)

    async def test_update_attribute_update_with_incorrect_object_id_datatype(self):
        payload = {"device": self.device.name, "data": {"binaryValue2": True}}
        await self.connector._AsyncBACnetConnector__devices.remove(self.device)
        self.device = self.create_fake_device(
            "attribute_updates/on_attribute_updates_bacnet_config_invalid_data_type.json"
        )
        await self.connector._AsyncBACnetConnector__devices.add(self.device)
        self.connector._AsyncBACnetConnector__get_device_by_name = MagicMock()
        self.connector._AsyncBACnetConnector__get_device_by_name.return_value = self.device

        with patch.object(self.connector, "_AsyncBACnetConnector__create_task") as ct_mock, \
                self.assertLogs("Bacnet test", level="ERROR") as logcap:
            self.connector.on_attributes_update(content=payload)

        ct_mock.assert_not_called()
        self.assertTrue(any("for key" in m for m in logcap.output))

    async def test_update_continues_when_first_task_failed(self):
        payload = {"device": self.device.name, "data": {"binaryValue2": True, "binaryInput1": 56}}
        await self.connector._AsyncBACnetConnector__devices.remove(self.device)
        self.device = self.create_fake_device(
            "attribute_updates/on_attribute_updates_bacnet_connector_multiple_updates.json"
        )
        await self.connector._AsyncBACnetConnector__devices.add(self.device)
        self.connector._AsyncBACnetConnector__get_device_by_name = MagicMock()
        self.connector._AsyncBACnetConnector__get_device_by_name.return_value = self.device

        with patch.object(self.connector, "_AsyncBACnetConnector__create_task") as ct_mock, \
                self.assertLogs("Bacnet test", level="ERROR") as logcap:
            ct_mock.side_effect = [Exception(""), None]
            self.connector.on_attributes_update(payload)
        self.assertEqual(ct_mock.call_count, 2)
