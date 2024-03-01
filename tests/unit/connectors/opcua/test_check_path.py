# from unittest import mock
#
# try:
#     from opcua import Node
#     from opcua.ua import FourByteNodeId
# except ImportError:
#     from thingsboard_gateway.tb_utility.tb_utility import TBUtility
#     TBUtility.install_package("opcua")
#     from opcua import Node
#     from opcua.ua import FourByteNodeId
#
# from tests.unit.BaseUnitTest import BaseUnitTest
# from thingsboard_gateway.connectors.opcua.opcua_connector import OpcUaConnector
#
# EMPTY_CONFIG = {'server': {'mapping': [], 'url': 'opc.tcp://localhost:4840'}}
#
# class TestOpcUaCheckPath(BaseUnitTest):
#     def test_check_path_browsepath_partial(self):
#         connector = OpcUaConnector(None, EMPTY_CONFIG, None)
#         with mock.patch.object(OpcUaConnector, 'get_node_path', return_value='Root\\.Objects\\.Machine'):
#             result = connector._check_path('Cycle\\.Cycle\\Status', Node(nodeid=FourByteNodeId("ns=2;i=1"), server=None))
#             self.assertEqual(result, 'Root\\\\.Objects\\\\.Machine\\\\.Cycle\\\\.Cycle\\\\Status')
#
#     def test_check_path_browsepath_partial_with_path_seperator(self):
#         connector = OpcUaConnector(None, EMPTY_CONFIG, None)
#         with mock.patch.object(OpcUaConnector, 'get_node_path', return_value='Root\\.Objects\\.Machine'):
#             result = connector._check_path('\\.Cycle\\.Cycle\\Status', Node(nodeid=FourByteNodeId("ns=2;i=1"), server=None))
#             self.assertEqual(result, 'Root\\\\.Objects\\\\.Machine\\\\.Cycle\\\\.Cycle\\\\Status')
#
#     def test_check_path_browsepath_full(self):
#         connector = OpcUaConnector(None, EMPTY_CONFIG, None)
#         with mock.patch.object(OpcUaConnector, 'get_node_path', return_value='Root\\.Objects\\.Machine'):
#             result = connector._check_path('Root\\.Objects\\.Machine\\.Cycle\\.Cycle\\Status', Node(nodeid=FourByteNodeId("ns=2;i=1"), server=None))
#             self.assertEqual(result, 'Root\\\\.Objects\\\\.Machine\\\\.Cycle\\\\.Cycle\\\\Status')
