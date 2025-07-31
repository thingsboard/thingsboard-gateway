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


from asyncio import Future
from os import path
from unittest.mock import MagicMock, patch
from asyncua import Node
from asyncua.ua import NodeId

from tests.unit.connectors.opcua.opcua_base_test import OpcUABaseTest


class OpcUAAttributeUpdatesTest(OpcUABaseTest):

    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.fake_device = self.create_fake_device('attribute_updates/opcua_config_attribute_update_full_path.json')
        self.connector._OpcUaConnector__device_nodes.append(self.fake_device)

    async def test_correctly_return_node_on_full_path(self):
        fake_session = MagicMock()
        expected_id = NodeId(13, 2)
        payload = {"device": self.fake_device.name, "data": {"Frequency": 5}}

        done_future = Future()
        done_future.set_result(
            [[{"node": Node(fake_session, expected_id), "path": "2:Frequency"}]]
        )
        with patch.object(
                self.connector._OpcUaConnector__loop, "create_task", return_value=done_future
        ) as create_task_mock:
            node_id, value, timeout = self.connector._OpcUaConnector__resolve_node_id(
                payload, self.fake_device
            )
        create_task_mock.assert_called_once()
        self.assertEqual(node_id, expected_id)
        self.assertEqual(value, 5)

    async def test_correctly_return_node_on_identifier(self):
        expected_id = NodeId(13, 2)
        payload = {"device": self.fake_device.name, "data": {"Frequency": 5}}
        self.fake_device = self.create_fake_device(
            path.join(self.CONFIG_PATH, 'attribute_updates/opcua_config_attribute_update_identifier.json'))
        node_id, value, timeout = self.connector._OpcUaConnector__resolve_node_id(
            payload, self.fake_device
        )
        self.assertEqual(node_id, expected_id)
        self.assertEqual(value, 5)

    async def test_returns_none_on_incorrect_attribute_update_key(self):
        expected_id = None
        payload = {"device": self.fake_device.name, "data": {"Frequencyy": 5}}
        node_id, value = self.connector._OpcUaConnector__resolve_node_id(
            payload, self.fake_device
        )
        self.assertEqual(node_id, expected_id)
        self.assertEqual(value, None)

    async def test_update_non_listed_attribute(self):
        expected_id = None
        payload = {"device": self.fake_device.name, "data": {"Power": 5}}
        node_id, value = self.connector._OpcUaConnector__resolve_node_id(
            payload, self.fake_device
        )
        self.assertEqual(node_id, expected_id)
        self.assertEqual(value, None)

    async def test_attribute_is_written(self):
        node_id, value = NodeId(13, 2), 10
        done = Future()
        done.set_result({"value": value})

        with patch.object(
                self.connector._OpcUaConnector__loop, "create_task", return_value=done
        ) as ct_mock:
            result = self.connector._OpcUaConnector__write_node_value(node_id, value, timeout=5)

        ct_mock.assert_called_once()
        self.assertTrue(result)
        self.assertEqual(done.result(), {"value": 10})

    async def test_write_returns_error_dict(self):
        node_id, value = None, 10
        done = Future()
        done.set_result({'error': "'NoneType' object has no attribute 'write_value'"})

        with patch.object(
                self.connector._OpcUaConnector__loop, "create_task", return_value=done,
        ):
            result = self.connector._OpcUaConnector__write_node_value(node_id, value, timeout=5)

        self.assertFalse(result)

    async def test_write_fails_when_create_task_raises(self):
        node_id, value = NodeId(99, 2), 7
        done = Future()
        done.set_result({'error': 'Failed to send request to OPC UA server'})

        with patch.object(
                self.connector._OpcUaConnector__loop, "create_task",
        ) as ct_mock, patch("time.sleep", return_value=None):
            result = self.connector._OpcUaConnector__write_node_value(node_id, value, timeout=5)

        ct_mock.assert_called_once()
        self.assertFalse(result)
