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
from asyncio import Future
from asyncio import get_event_loop
from os import path
from unittest import IsolatedAsyncioTestCase
from unittest.mock import MagicMock, patch
from asyncua import Node
from asyncua.ua import NodeId, UaError
from simplejson import load

from thingsboard_gateway.connectors.opcua.device import Device
from thingsboard_gateway.connectors.opcua.opcua_connector import OpcUaConnector


class OpcUAAttributeUpdatesTest(IsolatedAsyncioTestCase):
    CONFIG_PATH = path.join(path.dirname(path.abspath(__file__)), 'data')

    async def asyncSetUp(self):
        self.connector: OpcUaConnector = OpcUaConnector.__new__(OpcUaConnector)
        self.connector._OpcUaConnector__log = logging.getLogger('Opc test')
        self.connector._OpcUaConnector__loop = get_event_loop()
        self.connector._OpcUaConnector__device_nodes = []
        self.fake_device = self.create_fake_device('attribute_updates/opcua_config_attribute_update_full_path.json')
        self.connector._OpcUaConnector__device_nodes.append(self.fake_device)

    async def asyncTearDown(self):
        log = logging.getLogger('Opc test')
        for handler in list(log.handlers):
            log.removeHandler(handler)
        self.fake_device = None
        self.connector = None
        await super().asyncTearDown()

    @staticmethod
    def create_fake_nodes():
        fake_session = MagicMock()
        device_node = Node(fake_session, NodeId(12, 2))
        child_nodes = [
            {"key": "Power", "node": Node(fake_session, NodeId(14, 2)), "section": "attributes",
             "timestampLocation": "gateway"},
            {"key": "Frequency", "node": Node(fake_session, NodeId(13, 2)), "section": "attributes",
             "timestampLocation": "gateway"},
            {"key": "Humidity", "node": Node(fake_session, NodeId(16, 2)), "section": "timeseries",
             "timestampLocation": "gateway"},
            {"key": "Temperature", "node": Node(fake_session, NodeId(15, 2)), "section": "timeseries",
             "timestampLocation": "gateway"},
        ]
        return device_node, child_nodes

    def create_fake_device(self, attribute_update_config_path):
        device_node, child_nodes = self.create_fake_nodes()
        device = Device(
            logger=self.connector._OpcUaConnector__log,
            path=['0:Objects', '2:MyObject'],
            device_node=device_node,
            name="OPCUA New Advanced Device",
            device_profile="default",
            config=self.convert_json(path.join(self.CONFIG_PATH, attribute_update_config_path)),
            converter=None,
            converter_for_sub=None,
        )
        for child_node in child_nodes:
            device.nodes.append(child_node)
        return device

    @staticmethod
    def convert_json(config_path):
        with open(config_path, 'r') as config_file:
            config = load(config_file)
        return config

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
            node_id, value = self.connector._OpcUaConnector__resolve_node_id(
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
        node_id, value = self.connector._OpcUaConnector__resolve_node_id(
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
            result = self.connector._OpcUaConnector__write_node_value(node_id, value)

        ct_mock.assert_called_once()
        self.assertTrue(result)
        self.assertEqual(done.result(), {"value": 10})

    async def test_write_returns_error_dict(self):
        node_id, value = None, 10
        done = Future()
        done.set_result({'error': "'NoneType' object has no attribute 'write_value'"})

        with patch.object(
                self.connector._OpcUaConnector__loop, "create_task", return_value=done,
                side_effect=AttributeError("'NoneType' object has no attribute 'write_value'")
        ):
            result = self.connector._OpcUaConnector__write_node_value(node_id, value)

        self.assertFalse(result)

    async def test_write_fails_when_create_task_raises(self):
        node_id, value = NodeId(99, 2), 7
        done = Future()
        done.set_result({'error': 'Failed to send request to OPC UA server'})

        with patch.object(
                self.connector._OpcUaConnector__loop, "create_task",
                side_effect=UaError('Failed to send request to OPC UA server')
        ) as ct_mock, patch("time.sleep", return_value=None):
            result = self.connector._OpcUaConnector__write_node_value(node_id, value)

        ct_mock.assert_called_once()
        self.assertFalse(result)
