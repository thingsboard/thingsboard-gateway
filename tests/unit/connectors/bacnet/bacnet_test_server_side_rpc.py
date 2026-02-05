#     Copyright 2026. ThingsBoard
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
from unittest.mock import patch, MagicMock, AsyncMock
from asyncio import Future

from bacpypes3.primitivedata import ObjectIdentifier

from tests.unit.connectors.bacnet.bacnet_base_test import BacnetBaseTestCase


class BacnetReservedRpcTestCase(BacnetBaseTestCase):

    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.connector._AsyncBACnetConnector__application = AsyncMock()
        if not hasattr(self.connector, "_AsyncBACnetConnector__gateway"):
            self.connector._AsyncBACnetConnector__gateway = MagicMock()
        self._loop_thread = Thread(target=self.connector.loop.run_forever, daemon=True)
        self._loop_thread.start()
        await self.connector._AsyncBACnetConnector__devices.remove(self.device)
        self.device = self.create_fake_device('server_side_rpc/bacnet_server_side_rpc.json')
        await self.connector._AsyncBACnetConnector__devices.add(self.device)
        self.connector._AsyncBACnetConnector__get_device_by_name = MagicMock()

    async def asyncTearDown(self):
        self.connector.loop.call_soon_threadsafe(self.connector.loop.stop)
        self._loop_thread.join(timeout=1)
        await super().asyncTearDown()

    async def test_set_reserved_rpc_with_correct_params(self):
        await self.connector._AsyncBACnetConnector__devices.remove(self.device)
        self.device = self.create_fake_device('server_side_rpc/bacnet_server_side_rpc.json')
        await self.connector._AsyncBACnetConnector__devices.add(self.device)
        self.connector._AsyncBACnetConnector__get_device_by_name.return_value = self.device
        content = {
            'device': self.device.name,
            'data': {
                'id': 24,
                'method': 'set',
                'params': 'objectType=binaryInput;objectId=1;propertyId=presentValue;value=56;',
            },
            'id': 24
        }

        done = Future()
        done.set_result({"response": {"value": "56"}})

        with patch.object(self.connector, "_AsyncBACnetConnector__create_task", return_value=done) as ct_mock:
            self.connector.server_side_rpc_handler(content=content)

        ct_mock.assert_called_once()
        func, args, kwargs = ct_mock.call_args.args
        addr, obj_id, prop_id = args[:3]
        self.assertEqual(str(addr), self.device.details.address)
        self.assertEqual(prop_id, "presentValue")
        self.assertEqual(kwargs.get("value"), "56")

        self.connector._AsyncBACnetConnector__gateway.send_rpc_reply.assert_called_once()
        _, _, k = self.connector._AsyncBACnetConnector__gateway.send_rpc_reply.mock_calls[-1]
        self.assertEqual(k["device"], self.device.name)
        self.assertIn("value", k["content"]["result"])

    async def test_get_reserved_rpc_with_correct_params(self):
        content = {
            'device': self.device.name,
            'data': {
                'id': 86,
                'method': 'get',
                'params': 'objectType=binaryInput;objectId=1;propertyId=presentValue;',
            },
            'id': 86
        }

        done = Future()
        done.set_result({"response": {"value": "56"}})
        self.connector._AsyncBACnetConnector__get_device_by_name.return_value = self.device

        with patch.object(self.connector, "_AsyncBACnetConnector__create_task", return_value=done) as ct_mock:
            self.connector.server_side_rpc_handler(content=content)

        ct_mock.assert_called_once()
        func, args, kwargs = ct_mock.call_args.args
        addr, obj_id, prop_id = args[:3]
        self.assertEqual(str(addr), self.device.details.address)
        self.assertEqual(str(obj_id), str(ObjectIdentifier(("binaryInput", 1))))
        self.assertEqual(prop_id, "presentValue")

        self.connector._AsyncBACnetConnector__gateway.send_rpc_reply.assert_called_once()
        _, _, k = self.connector._AsyncBACnetConnector__gateway.send_rpc_reply.mock_calls[-1]
        self.assertEqual(k["device"], self.device.name)
        self.assertIn("value", k["content"]["result"])

    async def test_get_reseved_with_incorrect_schema_params(self):
        content = {
            'device': self.device.name,
            'data': {
                'id': 89,
                'method': 'get',
                'params': 'objectType= binaryInput;objectId=1;propertyId=presentValue;',
            },
            'id': 89
        }
        self.connector._AsyncBACnetConnector__get_device_by_name.return_value = self.device

        with patch.object(self.connector, "_AsyncBACnetConnector__create_task") as ct_mock:
            self.connector.server_side_rpc_handler(content=content)

        ct_mock.assert_not_called()
        self.assertTrue(self.connector._AsyncBACnetConnector__gateway.send_rpc_reply.called)
        _, _, k = self.connector._AsyncBACnetConnector__gateway.send_rpc_reply.mock_calls[-1]
        self.assertEqual(k["device"], self.device.name)
        self.assertEqual(k["req_id"], 89)
        self.assertIn("error", k["content"]["result"])
        self.assertIn("objectType=<objectType>;objectId=<objectId>;propertyId=<propertyId>;",
                      k["content"]["result"]["error"])

    async def test_set_reserved_with_incorrect_schema_params(self):
        content = {'device': self.device.name, 'data': {
            'id': 118,
            'method': 'set',
            'params': 'objectType=binaryInput ;objectId=1;propertyId=presentValue;value=69;', },
                   'id': 118
                   }
        self.connector._AsyncBACnetConnector__get_device_by_name.return_value = self.device
        with patch.object(self.connector, "_AsyncBACnetConnector__create_task") as ct_mock:
            self.connector.server_side_rpc_handler(content=content)

        ct_mock.assert_not_called()
        self.assertTrue(self.connector._AsyncBACnetConnector__gateway.send_rpc_reply.called)
        _, _, k = self.connector._AsyncBACnetConnector__gateway.send_rpc_reply.mock_calls[-1]
        self.assertEqual(k["device"], self.device.name)
        self.assertEqual(k["req_id"], 118)
        self.assertIn("error", k["content"]["result"])
        self.assertIn(
            "objectType=<objectType>;objectId=<objectId>;propertyId=<propertyId>;priority=<priority>;value=<value>;",
            k["content"]["result"]["error"])

    async def test_set_reserved_rpc_with_invalid_object_type(self):
        content = {'device': self.device.name, 'data': {
            'id': 118,
            'method': 'set',
            'params': 'objectType=binaryInputs;objectId=1;propertyId=presentValue;value=69;', },
                   'id': 118
                   }
        self.connector._AsyncBACnetConnector__get_device_by_name.return_value = self.device
        with patch.object(self.connector, "_AsyncBACnetConnector__create_task") as ct_mock:
            self.connector.server_side_rpc_handler(content=content)
        ct_mock.assert_not_called()
        self.assertTrue(self.connector._AsyncBACnetConnector__gateway.send_rpc_reply.called)
        _, _, k = self.connector._AsyncBACnetConnector__gateway.send_rpc_reply.mock_calls[-1]
        self.assertEqual(k["device"], self.device.name)
        self.assertEqual(k["req_id"], 118)
        self.assertIn("error", k["content"]["result"])
        self.assertEqual(
            "The objectType must be from '['analogInput', 'analogOutput', 'analogValue', 'binaryInput', 'binaryOutput', 'binaryValue'], but got'binaryInputs",
            k["content"]["result"]["error"])

    async def test_set_reserved_rpc_with_incorrect_object_id(self):
        content = {
            'device': self.device.name,
            'data': {
                'id': 119,
                'method': 'set',
                'params': 'objectType=binaryInput;objectId=25;propertyId=presentValue;value=69;',
            },
            'id': 119
        }
        self.connector._AsyncBACnetConnector__get_device_by_name.return_value = self.device
        with patch.object(self.connector, "_AsyncBACnetConnector__create_task") as ct_mock:
            self.connector.server_side_rpc_handler(content=content)

        ct_mock.assert_called_once()
        _, args, kwargs = ct_mock.call_args.args
        addr, obj_id, prop_id = args[:3]
        self.assertEqual(str(addr), self.device.details.address)
        self.assertEqual(prop_id, "presentValue")
        self.assertEqual(kwargs.get("value"), "69")

        self.assertTrue(self.connector._AsyncBACnetConnector__gateway.send_rpc_reply.called)
        _, _, k = self.connector._AsyncBACnetConnector__gateway.send_rpc_reply.mock_calls[-1]
        self.assertEqual(k["device"], self.device.name)

    async def test_blank_method(self):
        content = {'device': self.device.name, 'data': {'id': 122, 'method': "set", 'params': None}, 'id': 122}
        self.connector._AsyncBACnetConnector__get_device_by_name.return_value = self.device
        with patch.object(self.connector, "_AsyncBACnetConnector__create_task") as ct_mock:
            self.connector.server_side_rpc_handler(content=content)

        ct_mock.assert_not_called()
        self.assertTrue(self.connector._AsyncBACnetConnector__gateway.send_rpc_reply.called)
        _, _, k = self.connector._AsyncBACnetConnector__gateway.send_rpc_reply.mock_calls[-1]
        self.assertEqual(k["device"], self.device.name)
        self.assertEqual(k["req_id"], 122)
        self.assertIn("No params section found in RPC request", str(k.get("content", {})))

    async def test_method_without_params(self):
        content = {'device': self.device.name, 'data': {'id': 125, 'method': 'set', 'params': None}, 'id': 125}
        self.connector._AsyncBACnetConnector__get_device_by_name.return_value = self.device
        with patch.object(self.connector, "_AsyncBACnetConnector__create_task") as ct_mock:
            self.connector.server_side_rpc_handler(content=content)

        ct_mock.assert_not_called()
        self.assertTrue(self.connector._AsyncBACnetConnector__gateway.send_rpc_reply.called)
        _, _, k = self.connector._AsyncBACnetConnector__gateway.send_rpc_reply.mock_calls[-1]
        self.assertEqual(k["device"], self.device.name)
        self.assertIn("No params section found in RPC request", str(k.get("content", {})))


class BacnetDeviceRpcTest(BacnetBaseTestCase):

    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.connector._AsyncBACnetConnector__application = AsyncMock()
        if not hasattr(self.connector, "_AsyncBACnetConnector__gateway"):
            self.connector._AsyncBACnetConnector__gateway = MagicMock()
        self._loop_thread = Thread(target=self.connector.loop.run_forever, daemon=True)
        self._loop_thread.start()
        await self.connector._AsyncBACnetConnector__devices.remove(self.device)
        self.device = self.create_fake_device('server_side_rpc/bacnet_server_side_rpc.json')
        await self.connector._AsyncBACnetConnector__devices.add(self.device)
        self.connector._AsyncBACnetConnector__get_device_by_name = MagicMock()

    async def asyncTearDown(self):
        self.connector.loop.call_soon_threadsafe(self.connector.loop.stop)
        self._loop_thread.join(timeout=1)
        await super().asyncTearDown()

    async def test_execute_write_property_method(self):
        payload = {'device': 'test emulator device', 'data': {'id': 5, 'method': 'SetState', 'params': 50}, 'id': 5}
        self.connector._AsyncBACnetConnector__get_device_by_name.return_value = self.device

        done = Future()
        done.set_result({"response": {"value": "50"}})

        with patch.object(self.connector, "_AsyncBACnetConnector__create_task", return_value=done) as ct_mock:
            self.connector.server_side_rpc_handler(content=payload)

        ct_mock.assert_called_once()
        func, args, kwargs = ct_mock.call_args.args
        addr, obj_id, prop_id = args[:3]
        self.assertEqual(str(addr), self.device.details.address)
        self.assertEqual(str(obj_id), str(ObjectIdentifier(("binaryValue", 2))))
        self.assertEqual(prop_id, "presentValue")
        self.assertEqual(kwargs.get("value"), 50)

        self.connector._AsyncBACnetConnector__gateway.send_rpc_reply.assert_called_once()
        _, _, k = self.connector._AsyncBACnetConnector__gateway.send_rpc_reply.mock_calls[-1]
        self.assertEqual(k["device"], self.device.name)
        self.assertIn("value", k["content"]["result"])

    async def test_execute_read_property_method(self):
        payload = {'device': 'test emulator device', 'data': {'id': 6, 'method': 'GetState', 'params': None}, 'id': 6}
        await self.connector._AsyncBACnetConnector__devices.remove(self.device)
        self.device = self.create_fake_device('server_side_rpc/bacnet_server_side_rpc_multiple_valid_methods.json')
        await self.connector._AsyncBACnetConnector__devices.add(self.device)
        self.connector._AsyncBACnetConnector__get_device_by_name.return_value = self.device

        done = Future()
        done.set_result({"response": {"value": "50"}})

        with patch.object(self.connector, "_AsyncBACnetConnector__create_task", return_value=done) as ct_mock:
            self.connector.server_side_rpc_handler(content=payload)

        ct_mock.assert_called_once()
        func, args, kwargs = ct_mock.call_args.args
        addr, obj_id, prop_id = args[:3]
        self.assertEqual(str(addr), self.device.details.address)
        self.assertEqual(str(obj_id), str(ObjectIdentifier(("binaryInput", 1))))
        self.assertEqual(prop_id, "presentValue")

        self.connector._AsyncBACnetConnector__gateway.send_rpc_reply.assert_called_once()
        _, _, k = self.connector._AsyncBACnetConnector__gateway.send_rpc_reply.mock_calls[-1]
        self.assertEqual(k["device"], self.device.name)
        self.assertIn("value", k["content"]["result"])

    async def test_execute_read_property_method_incorrect_request_type(self):
        payload = {'device': 'test emulator device', 'data': {'id': 18, 'method': 'GetState', 'params': None}, 'id': 18}
        await self.connector._AsyncBACnetConnector__devices.remove(self.device)
        self.device = self.create_fake_device(
            'server_side_rpc/bacnet_server_side_rpc_incorrect_read_request_type.json')
        await self.connector._AsyncBACnetConnector__devices.add(self.device)
        self.connector._AsyncBACnetConnector__get_device_by_name.return_value = self.device
        with patch.object(self.connector, "_AsyncBACnetConnector__create_task") as ct_mock:
            self.connector.server_side_rpc_handler(content=payload)
        ct_mock.assert_not_called()
        self.assertTrue(self.connector._AsyncBACnetConnector__gateway.send_rpc_reply.called)
        _, _, k = self.connector._AsyncBACnetConnector__gateway.send_rpc_reply.mock_calls[-1]
        self.assertEqual(k["device"], self.device.name)
        self.assertEqual(k["req_id"], 18)
        self.assertIn("error", k["content"]["result"])
        self.assertEqual(
            "Invalid requestType: 'readPropertys'. Expected 'writeProperty' or "
            "'readProperty'."
            ,
            k["content"]["result"]["error"])

    async def test_execute_device_rpc_with_no_specified_method(self):
        payload = {'device': 'test emulator device', 'data': {'id': 20, 'method': 'SetStates', 'params': 200}, 'id': 20}
        self.connector._AsyncBACnetConnector__get_device_by_name.return_value = self.device
        with patch.object(self.connector, "_AsyncBACnetConnector__create_task") as ct_mock:
            self.connector.server_side_rpc_handler(content=payload)
        ct_mock.assert_not_called()
        self.assertTrue(self.connector._AsyncBACnetConnector__gateway.send_rpc_reply.called)
        _, _, k = self.connector._AsyncBACnetConnector__gateway.send_rpc_reply.mock_calls[-1]
        self.assertEqual(k["device"], self.device.name)
        self.assertEqual(k["req_id"], 20)
        self.assertIn("error", k["content"]["result"])
        self.assertEqual(
            'Neither of configured device rpc methods match with SetStates'
            ,
            k["content"]["result"]["error"])

    async def test_execute_device_rpc_with_incorrect_config(self):
        payload = {'device': 'test emulator device', 'data': {'id': 24, 'method': 'SetState', 'params': 79}, 'id': 24}
        await self.connector._AsyncBACnetConnector__devices.remove(self.device)
        self.device = self.create_fake_device(
            'server_side_rpc/bacnet_server_side_rpc_incorrect_config.json')
        await self.connector._AsyncBACnetConnector__devices.add(self.device)
        self.connector._AsyncBACnetConnector__get_device_by_name.return_value = self.device
        with patch.object(self.connector, "_AsyncBACnetConnector__create_task") as ct_mock:
            self.connector.server_side_rpc_handler(content=payload)
        ct_mock.assert_not_called()
        self.assertTrue(self.connector._AsyncBACnetConnector__gateway.send_rpc_reply.called)
        _, _, k = self.connector._AsyncBACnetConnector__gateway.send_rpc_reply.mock_calls[-1]
        self.assertEqual(k["device"], self.device.name)
        self.assertEqual(k["req_id"], 24)
        self.assertIn("error", k["content"]["result"])
        self.assertEqual("Invalid objectType: 'binaryValuessss'. Expected one of ['analogInput', "
                         "'analogOutput', 'analogValue', 'binaryInput', 'binaryOutput', "
                         "'binaryValue']."
                         ,
                         k["content"]["result"]["error"])

    async def test_execute_write_property_method_with_no_arguments(self):
        payload = {'device': 'test emulator device', 'data': {'id': 27, 'method': 'set', 'params': None}, 'id': 27}
        self.connector._AsyncBACnetConnector__get_device_by_name.return_value = self.device
        with patch.object(self.connector, "_AsyncBACnetConnector__create_task") as ct_mock:
            self.connector.server_side_rpc_handler(content=payload)
        ct_mock.assert_not_called()
        self.assertTrue(self.connector._AsyncBACnetConnector__gateway.send_rpc_reply.called)
        _, _, k = self.connector._AsyncBACnetConnector__gateway.send_rpc_reply.mock_calls[-1]
        self.assertEqual(k["device"], self.device.name)
        self.assertEqual(k["req_id"], 27)
        self.assertIn("error", k["content"]["result"])
        self.assertEqual(
            'No params section found in RPC request'
            ,
            k["content"]["result"]["error"])

    async def test_execute_read_property_method_with_arguments(self):
        payload = {'device': 'test emulator device', 'data': {'id': 29, 'method': 'GetState', 'params': 90}, 'id': 29}
        await self.connector._AsyncBACnetConnector__devices.remove(self.device)
        self.device = self.create_fake_device(
            'server_side_rpc/bacnet_server_side_rpc_multiple_valid_methods.json')
        await self.connector._AsyncBACnetConnector__devices.add(self.device)

        done = Future()
        done.set_result({"response": {"value": "56"}})
        self.connector._AsyncBACnetConnector__get_device_by_name.return_value = self.device

        with patch.object(self.connector, "_AsyncBACnetConnector__create_task", return_value=done) as ct_mock:
            self.connector.server_side_rpc_handler(content=payload)

        ct_mock.assert_called_once()
        func, args, kwargs = ct_mock.call_args.args
        addr, obj_id, prop_id = args[:3]
        self.assertEqual(str(addr), self.device.details.address)
        self.assertEqual(str(obj_id), str(ObjectIdentifier(("binaryInput", 1))))
        self.assertEqual(prop_id, "presentValue")

        self.connector._AsyncBACnetConnector__gateway.send_rpc_reply.assert_called_once()
        _, _, k = self.connector._AsyncBACnetConnector__gateway.send_rpc_reply.mock_calls[-1]
        self.assertEqual(k["device"], self.device.name)
        self.assertIn("value", k["content"]["result"])
