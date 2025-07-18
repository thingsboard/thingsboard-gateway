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

from os import path
from unittest.mock import MagicMock

from simplejson import load
import logging
from asyncua import Node
from asyncio import get_event_loop
from unittest import mock, IsolatedAsyncioTestCase
from asyncua.ua import NodeId
from thingsboard_gateway.connectors.opcua.device import Device
from thingsboard_gateway.connectors.opcua.opcua_connector import OpcUaConnector
from thingsboard_gateway.tb_utility.tb_utility import TBUtility

ATTRIBUTE_REQUEST_CONTENT = {'data': {'Frequency': 5}, 'device': 'OPCUA New Advanced Device'}


class OpcUAAttributeUpdatesTest(IsolatedAsyncioTestCase):
    CONFIG_PATH = path.join(path.dirname(path.dirname(path.abspath(__file__))),
                            'connectors' + path.sep + 'opcua' + path.sep + 'data' + path.sep)

    async def asyncSetUp(self):
        self.connector: OpcUaConnector = OpcUaConnector.__new__(OpcUaConnector)
        self.connector._OpcUaConnector__log = logging.getLogger('Opc test')
        self.connector._OpcUaConnector__loop = get_event_loop()
        self.connector._OpcUaConnector__device_nodes = []
        self.fake_device = self.create_fake_device()
        self.connector._OpcUaConnector__device_nodes.append(self.fake_device)

    @staticmethod
    def create_fake_nodes():
        fake_session = MagicMock()
        device_node = Node(fake_session, NodeId(2, 12))
        child_nodes = [
            {"key": "Power", "node": Node(fake_session, NodeId(2, 14)), "section": "attributes",
             "timestampLocation": "gateway"},
            {"key": "Frequency", "node": Node(fake_session, NodeId(2, 13)), "section": "attributes",
             "timestampLocation": "gateway"},
            {"key": "Humidity", "node": Node(fake_session, NodeId(2, 16)), "section": "timeseries",
             "timestampLocation": "gateway"},
            {"key": "Temperature", "node": Node(fake_session, NodeId(2, 15)), "section": "timeseries",
             "timestampLocation": "gateway"},
        ]
        return device_node, child_nodes

    def create_fake_device(self):
        device_node, child_nodes = self.create_fake_nodes()
        device = Device(
            logger=self.connector._OpcUaConnector__log,
            path=["Root", "Objects", "MyObject"],
            device_node=device_node,
            name="OPCUA New Advanced Device",
            device_profile="default",
            config=self.convert_json(self.CONFIG_PATH + 'attribute_updates/opcua-device-config.json'),
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

    def test_correctly_return_identifier_on_full_path(self):
        pass


