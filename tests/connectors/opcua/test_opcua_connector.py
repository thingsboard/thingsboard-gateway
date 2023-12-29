import logging
import unittest
from unittest import mock

from opcua import Node
from opcua.ua import FourByteNodeId

from thingsboard_gateway.connectors.opcua.opcua_connector import OpcUaConnector

logging.basicConfig(level=logging.ERROR,
                    format='%(asctime)s - %(levelname)s - %(module)s - %(lineno)d - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')
log = logging.getLogger("root")

EMPTY_CONFIG = {'server': {'mapping': [], 'url': 'opc.tcp://localhost:4840'}}

class TestOpcUaConnector(unittest.TestCase):
    def test_check_path_browsepath_partial(self):
        connector = OpcUaConnector(None, EMPTY_CONFIG, None)
        with mock.patch.object(OpcUaConnector, 'get_node_path', return_value='Root\\.Objects\\.Machine'):
            result = connector._check_path('Cycle\\.Cycle\\Status', Node(nodeid=FourByteNodeId("ns=2;i=1"), server=None))
            self.assertEqual(result, 'Root\\\\.Objects\\\\.Machine\\\\.Cycle\\\\.Cycle\\\\Status')

    def test_check_path_browsepath_partial_with_path_seperator(self):
        connector = OpcUaConnector(None, EMPTY_CONFIG, None)
        with mock.patch.object(OpcUaConnector, 'get_node_path', return_value='Root\\.Objects\\.Machine'):
            result = connector._check_path('\\.Cycle\\.Cycle\\Status', Node(nodeid=FourByteNodeId("ns=2;i=1"), server=None))
            self.assertEqual(result, 'Root\\\\.Objects\\\\.Machine\\\\.Cycle\\\\.Cycle\\\\Status')

    def test_check_path_browsepath_full(self):
        connector = OpcUaConnector(None, EMPTY_CONFIG, None)
        with mock.patch.object(OpcUaConnector, 'get_node_path', return_value='Root\\.Objects\\.Machine'):
            result = connector._check_path('Root\\.Objects\\.Machine\\.Cycle\\.Cycle\\Status', Node(nodeid=FourByteNodeId("ns=2;i=1"), server=None))
            self.assertEqual(result, 'Root\\\\.Objects\\\\.Machine\\\\.Cycle\\\\.Cycle\\\\Status')
