from unittest.mock import patch, MagicMock, AsyncMock

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
            attribute_update_config_path='attribute_updates/on_attribute_updates_bacnet_config_empty_section.json')
        await self.connector._AsyncBACnetConnector__devices.add(self.device)

        with patch.object(self.connector, "_AsyncBACnetConnector__create_task") as create_task_mock:
            self.connector.on_attributes_update(payload)

        create_task_mock.assert_not_called()
