import logging
from asyncio import get_event_loop
from os import path
from unittest import IsolatedAsyncioTestCase
from unittest.mock import MagicMock, AsyncMock
from asyncua import Node
from asyncua.ua import NodeId
from simplejson import load

from thingsboard_gateway.connectors.opcua.device import Device
from thingsboard_gateway.connectors.opcua.opcua_connector import OpcUaConnector


class OpcUABaseTest(IsolatedAsyncioTestCase):
    CONFIG_PATH = path.join(path.dirname(path.abspath(__file__)), 'data')

    async def asyncSetUp(self):
        self.connector: OpcUaConnector = OpcUaConnector.__new__(OpcUaConnector)
        self.connector._OpcUaConnector__log = logging.getLogger('Opc test')
        self.connector._OpcUaConnector__loop = get_event_loop()
        self.connector._OpcUaConnector__device_nodes = []

    async def asyncTearDown(self):
        log = logging.getLogger('Opc test')
        for handler in list(log.handlers):
            log.removeHandler(handler)
        # self.fake_device = None
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
