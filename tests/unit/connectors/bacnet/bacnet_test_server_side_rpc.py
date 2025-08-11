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
from unittest.mock import patch, MagicMock, AsyncMock

from bacpypes3.primitivedata import ObjectIdentifier

from tests.unit.connectors.bacnet.bacnet_base_test import BacnetBaseTestCase


class BacnetOnAttributeUpdatesTestCase(BacnetBaseTestCase):

    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.connector._AsyncBACnetConnector__application = AsyncMock()
        if not hasattr(self.connector, "_AsyncBACnetConnector__gateway"):
            self.connector._AsyncBACnetConnector__gateway = MagicMock()
        self._loop_thread = Thread(target=self.connector.loop.run_forever, daemon=True)
        self._loop_thread.start()

    async def asyncTearDown(self):
        self.connector.loop.call_soon_threadsafe(self.connector.loop.stop)
        self._loop_thread.join(timeout=1)
        await super().asyncTearDown()

    async def test_set_reserved_rpc_with_correct_params(self):
        await self.connector._AsyncBACnetConnector__devices.remove(self.device)
        self.device = self.create_fake_device('server_side_rpc/bacnet_server_side_rpc.json')
        await self.connector._AsyncBACnetConnector__devices.add(self.device)

        content = {'device': self.device.name, 'data': {'id': 24, 'method': 'set',
                                                        'params': 'objectType=binaryInput;objectId=1;propertyId=presentValue;value=true;', },
                   'id': 24}

        def _fill_result(*args, **kwargs):
            result = {}
            result['response'] = {"value": "true"}

        with patch.object(self.connector, "_AsyncBACnetConnector__create_task", side_effect=_fill_result) as ct_mock:
            self.connector.server_side_rpc_handler(content=content)

        ct_mock.assert_called_once()
        func, args, kwargs = ct_mock.call_args.args
        addr, obj_id, prop_id = args[:3]
        self.assertEqual(str(addr), self.device.details.address)
        self.assertEqual(prop_id, "presentValue")
        self.assertEqual(kwargs.get("value"), "true")
        self.assertTrue(self.connector._AsyncBACnetConnector__gateway.send_rpc_reply.called)
        _, _, k = self.connector._AsyncBACnetConnector__gateway.send_rpc_reply.mock_calls[-1]
        self.assertEqual(k["device"], self.device.name)

    async def test_get_reserved_rpc_with_correct_params(self):
        await self.connector._AsyncBACnetConnector__devices.remove(self.device)
        self.device = self.create_fake_device('server_side_rpc/bacnet_server_side_rpc.json')
        await self.connector._AsyncBACnetConnector__devices.add(self.device)

        content = {
            'device': self.device.name,
            'data': {
                'id': 86,
                'method': 'get',
                'params': 'objectType=binaryInput;objectId=1;propertyId=presentValue;',
            },
            'id': 86
        }

        with patch.object(self.connector, "_AsyncBACnetConnector__create_task") as ct_mock:
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
        self.assertIn("result", k["content"])

    async def test_get_reseved_with_incorrect_schema_params(self):
        await self.connector._AsyncBACnetConnector__devices.remove(self.device)
        self.device = self.create_fake_device('server_side_rpc/bacnet_server_side_rpc.json')
        await self.connector._AsyncBACnetConnector__devices.add(self.device)

        content = {
            'device': self.device.name,
            'data': {
                'id': 89,
                'method': 'get',
                'params': 'objectType= binaryInput;objectId=1;propertyId=presentValue;',
            },
            'id': 89
        }

        with patch.object(self.connector, "_AsyncBACnetConnector__create_task") as ct_mock:
            self.connector.server_side_rpc_handler(content=content)

        ct_mock.assert_not_called()
        self.assertTrue(self.connector._AsyncBACnetConnector__gateway.send_rpc_reply.called)
        _, _, k = self.connector._AsyncBACnetConnector__gateway.send_rpc_reply.mock_calls[-1]
        self.assertEqual(k["device"], self.device.name)
        self.assertEqual(k["req_id"], 89)
        self.assertFalse(k.get("success_sent", True))
        self.assertIn("error", k["content"]["result"])
        self.assertIn("objectType=<objectType>;objectId=<objectId>;propertyId=<propertyId>;",
                      k["content"]["result"]["error"])

    async def test_set_reserved_with_incorrect_schema_params(self):
        await self.connector._AsyncBACnetConnector__devices.remove(self.device)
        self.device = self.create_fake_device('server_side_rpc/bacnet_server_side_rpc.json')
        await self.connector._AsyncBACnetConnector__devices.add(self.device)

        content = {'device': self.device.name, 'data': {
            'id': 118,
            'method': 'set',
            'params': 'objectType=binaryInput ;objectId=1;propertyId=presentValue;value=69;', },
                   'id': 118
                   }

        with patch.object(self.connector, "_AsyncBACnetConnector__create_task") as ct_mock:
            self.connector.server_side_rpc_handler(content=content)

        ct_mock.assert_not_called()
        self.assertTrue(self.connector._AsyncBACnetConnector__gateway.send_rpc_reply.called)
        _, _, k = self.connector._AsyncBACnetConnector__gateway.send_rpc_reply.mock_calls[-1]
        self.assertEqual(k["device"], self.device.name)
        self.assertEqual(k["req_id"], 118)
        self.assertFalse(k.get("success_sent", True))
        self.assertIn("error", k["content"]["result"])
        self.assertIn(
            "objectType=<objectType>;objectId=<objectId>;propertyId=<propertyId>;priority=<priority>;value=<value>;",
            k["content"]["result"]["error"])

    async def test_set_reserved_rpc_with_incorrect_object_id(self):
        await self.connector._AsyncBACnetConnector__devices.remove(self.device)
        self.device = self.create_fake_device('server_side_rpc/bacnet_server_side_rpc.json')
        await self.connector._AsyncBACnetConnector__devices.add(self.device)

        content = {
            'device': self.device.name,
            'data': {
                'id': 119,
                'method': 'set',
                'params': 'objectType=binaryInput;objectId=25;propertyId=presentValue;value=69;',
            },
            'id': 119
        }

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
        self.assertIn("result", k["content"])

    async def test_blank_method(self):
        await self.connector._AsyncBACnetConnector__devices.remove(self.device)
        self.device = self.create_fake_device('server_side_rpc/bacnet_server_side_rpc.json')
        await self.connector._AsyncBACnetConnector__devices.add(self.device)

        content = {'device': self.device.name, 'data': {'id': 122, 'method': "''", 'params': None}, 'id': 122}

        with patch.object(self.connector, "_AsyncBACnetConnector__create_task") as ct_mock:
            self.connector.server_side_rpc_handler(content=content)

        ct_mock.assert_not_called()
        self.assertTrue(self.connector._AsyncBACnetConnector__gateway.send_rpc_reply.called)
        _, _, k = self.connector._AsyncBACnetConnector__gateway.send_rpc_reply.mock_calls[-1]
        self.assertEqual(k["device"], self.device.name)
        self.assertEqual(k["req_id"], 122)
        self.assertFalse(k.get("success_sent", True))
        self.assertIn("RPC method not found", str(k.get("content", {})))

    async def test_method_without_params(self):
        await self.connector._AsyncBACnetConnector__devices.remove(self.device)
        self.device = self.create_fake_device('server_side_rpc/bacnet_server_side_rpc.json')
        await self.connector._AsyncBACnetConnector__devices.add(self.device)

        content = {'device': self.device.name, 'data': {'id': 125, 'method': 'set', 'params': None}, 'id': 125}

        with patch.object(self.connector, "_AsyncBACnetConnector__create_task") as ct_mock:
            self.connector.server_side_rpc_handler(content=content)

        ct_mock.assert_not_called()
        self.assertTrue(self.connector._AsyncBACnetConnector__gateway.send_rpc_reply.called)
        _, _, k = self.connector._AsyncBACnetConnector__gateway.send_rpc_reply.mock_calls[-1]
        self.assertEqual(k["device"], self.device.name)
        self.assertFalse(k.get("success_sent", True))
        self.assertIn("No params section found in RPC request", str(k.get("content", {})))
