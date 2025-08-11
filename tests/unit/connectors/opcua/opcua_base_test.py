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
from asyncio import Future, get_event_loop
from contextlib import contextmanager
from os import path
from unittest import IsolatedAsyncioTestCase
from unittest.mock import MagicMock, patch

from asyncua import Node
from asyncua.ua import NodeId
from simplejson import load

from thingsboard_gateway.connectors.opcua.device import Device
from thingsboard_gateway.connectors.opcua.opcua_connector import OpcUaConnector


class OpcUABaseTest(IsolatedAsyncioTestCase):
    CONFIG_PATH = path.join(path.dirname(path.abspath(__file__)), 'data')
    DEVICE_NAME = "OPCUA New Advanced Device"
    FREQ_NODEID = NodeId(13, 2)  # used in some asserts

    async def asyncSetUp(self):
        self.connector: OpcUaConnector = OpcUaConnector.__new__(OpcUaConnector)
        self.connector._OpcUaConnector__log = logging.getLogger('Opc test')
        self.connector._OpcUaConnector__loop = get_event_loop()
        self.connector._OpcUaConnector__device_nodes = []
        if not hasattr(self.connector, "_OpcUaConnector__gateway"):
            self.connector._OpcUaConnector__gateway = MagicMock()

    async def asyncTearDown(self):
        log = logging.getLogger('Opc test')
        for handler in list(log.handlers):
            log.removeHandler(handler)
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
            name=self.DEVICE_NAME,
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

    @staticmethod
    def make_done_future(value):
        f = Future()
        f.set_result(value)
        return f

    def make_connector_payload(self, rpc_id: str, method: str, args: list, inner_method: str | None = None):
        return {
            'id': rpc_id,
            'method': method,
            'params': {
                'arguments': [{'type': 'integer', 'value': v} for v in args],
                'connectorId': '8bd78640-1888-4a6c-b43e-98003cda158a',
                'method': inner_method if inner_method is not None else method.replace('opcua_', '')
            }
        }

    def make_reserved_payload(self, rpc_id: int, kind: str, params: str, device: str | None = None):
        return {
            'data': {'id': rpc_id, 'method': kind, 'params': params},
            'device': device or self.DEVICE_NAME,
            'id': rpc_id
        }

    @contextmanager
    def patch_task_and_sleep(self, done_future: Future, patch_sleep: bool = True):

        with patch.object(self.connector._OpcUaConnector__loop, "create_task",
                          return_value=done_future) as create_task_mock:
            if patch_sleep:
                with patch("thingsboard_gateway.connectors.opcua.opcua_connector.sleep", return_value=None):
                    yield create_task_mock
            else:
                yield create_task_mock

    def call_connector_with_result(self, rpc_request, result, *, patch_sleep=True):
        done_future = self.make_done_future(result)
        with self.patch_task_and_sleep(done_future, patch_sleep=patch_sleep) as create_task_mock:
            results = self.connector._OpcUaConnector__process_connector_rpc_request(rpc_request=rpc_request)
        return done_future, create_task_mock, results

    def call_device_with_result(self, rpc_request, result, *, patch_sleep=True):
        done_future = self.make_done_future(result)
        with self.patch_task_and_sleep(done_future, patch_sleep=patch_sleep) as create_task_mock:
            results = self.connector._OpcUaConnector__process_device_rpc_request(rpc_request=rpc_request)
        return done_future, create_task_mock, results

    def call_reserved_with_result(self, rpc_request, result, *, patch_sleep=True):
        done_future = self.make_done_future(result)
        with self.patch_task_and_sleep(done_future, patch_sleep=patch_sleep) as create_task_mock:
            results = self.connector._OpcUaConnector__process_reserved_rpc_request(rpc_request=rpc_request)
        return done_future, create_task_mock, results

    def assert_gateway_reply(self, rpc_request, result):
        self.connector._OpcUaConnector__gateway.send_rpc_reply.assert_called_once_with(
            rpc_request.device_name, rpc_request.id, {"result": result}
        )
