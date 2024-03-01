# import asyncio
# import logging
# import unittest
# from threading import Thread
# from unittest.mock import Mock
#
# try:
#     from asyncua import Node
# except AttributeError:
#     # Ignoring the issue with the import of Node from asyncua
#     # TODO - investigate the issue with the import of Node from asyncua and Cryptography in the tests
#     pass
# except (ImportError, ModuleNotFoundError):
#     from thingsboard_gateway.tb_utility.tb_utility import TBUtility
#     TBUtility.install_package("asyncua")
#     from asyncua import Node
#
# from tests.integration.connectors.opcua.helpers import wait_until, read_config, await_until, send_to_storage_to_timeseries, \
#     list_intersect_ordered
# from tests.integration.connectors.opcua.opcua_test_server import OpcUaTestServer
# from thingsboard_gateway.connectors.opcua.opcua_connector import OpcUaConnector
# from thingsboard_gateway.connectors.opcua_asyncio.opcua_connector import OpcUaConnectorAsyncIO
# from thingsboard_gateway.gateway.tb_gateway_service import TBGatewayService
#
# logging.basicConfig(level=logging.INFO,
#                     format='%(asctime)s - %(levelname)s - %(name)s - %(lineno)d - %(message)s',
#                     datefmt='%Y-%m-%d %H:%M:%S')
# log = logging.getLogger("TEST")
#
# # Disable some OPCUA logging
# logger = logging.getLogger("asyncua.server.uaprocessor")
# logger.setLevel(logging.WARNING)
# logger = logging.getLogger("asyncua.server.subscription_service")
# logger.setLevel(logging.WARNING)
# logger = logging.getLogger("asyncua.server.address_space")
# logger.setLevel(logging.WARNING)
# logger = logging.getLogger("asyncua.common")
# logger.setLevel(logging.WARNING)
# logger = logging.getLogger("opcua.client.ua_client")
# logger.setLevel(logging.WARNING)
# logger = logging.getLogger("opcua.uaprotocol")
# logger.setLevel(logging.WARNING)
# logger = logging.getLogger("asyncua.client.ua_client.UaClient")
# logger.setLevel(logging.WARNING)
# logger = logging.getLogger("asyncua.server.internal_session")
# logger.setLevel(logging.WARNING)
#
#
# class BaseOpcUaIntegration(unittest.IsolatedAsyncioTestCase):
#
#     server = None
#     _server_thread = None
#
#     def setup_server(self):
#         # Overload this method
#         pass
#
#     def setUp(self):
#         self.server = None
#         self.setup_server()
#         assert self.server is not None
#         self._server_thread = Thread(name='OpcUa server thread', target=lambda: asyncio.run(self.server.run()), daemon=True)
#         self._server_thread.start()
#         assert wait_until(lambda: hasattr(self.server, 'is_ready') and self.server.is_ready == True, 5)
#
#     def tearDown(self):
#         self.server.stop()
#         del self._server_thread
#         assert wait_until(lambda: hasattr(self.server, 'is_stopped') and self.server.is_stopped == True, 10)
#
#
# class OpcUaServerForTestConnection(OpcUaTestServer):
#     async def create_nodes(self):
#         async def create_machine_nodes(parent: Node, sn, browse_name='IntState', node_id=None):
#             machine = await parent.add_object(self._idx, 'Machine' + str(sn))
#             serial_number = await machine.add_variable(self._idx, 'SerialNumber', f"{sn}")
#             if not node_id is None:
#                 int_state = await machine.add_variable(node_id, browse_name, -1)
#             else:
#                 int_state = await machine.add_variable(self._idx, browse_name, -1)
#             return machine
#
#         self.machine1 = await create_machine_nodes(self._server.nodes.objects, 1)
#         self.machine2 = await create_machine_nodes(self._server.nodes.objects, 2)
#         self.collection = await self._server.nodes.objects.add_object(self._idx, 'Collection')
#         self.machine3 = await create_machine_nodes(self.collection, 3, f"{self.idx}:Int\\State", f"ns={self.idx};s=Int\\State")
#
#     async def update_nodes(self, iteration: int):
#         async def update_machine(machine_node: Node, browse_name='IntState'):
#             int_state = await machine_node.get_child(f'{self.idx}:{browse_name}')
#             await int_state.write_value(iteration)
#
#         await update_machine(self.machine1)
#         await update_machine(self.machine2)
#         await update_machine(self.machine3, 'Int\\State')
#         await asyncio.sleep(1)
#
#
# class TestOpcUaIntegration(BaseOpcUaIntegration):
#
#     def setup_server(self):
#         self.server = OpcUaServerForTestConnection()
#
#     async def impl_test_connection(self, connector, connector_type):
#         config = read_config('opcua_test_connection.json')
#         gateway = Mock(spec=TBGatewayService)
#         connector = connector(gateway, config, connector_type)
#         connector.open()
#
#         self.assertTrue(await await_until(lambda: hasattr(connector, 'is_connected') and connector.is_connected, 10))
#
#         await asyncio.sleep(5)
#
#         self.assertTrue(gateway.send_to_storage.call_count > 10)
#         log.debug(gateway.send_to_storage.call_args_list)
#
#         async def test_serial_number(expected_sn):
#             serial_number = send_to_storage_to_timeseries(gateway.send_to_storage.call_args_list, f'Machine {expected_sn}', 'attributes', 'SerialNumber')
#             self.assertTrue(len(serial_number) > 0 and all([sn[1] == f"{expected_sn}" for sn in serial_number]), f"Serial number did not expected sn: {expected_sn}, data: {serial_number}")
#
#         await test_serial_number(1)
#         await test_serial_number(2)
#         await test_serial_number(3)
#
#         async def test_subscribed(sn):
#             ts_int_state = send_to_storage_to_timeseries(gateway.send_to_storage.call_args_list, f'Machine {sn}', 'telemetry', 'IntState')
#             # FIXME: Both connectors should function the same! (at least for the basics)
#             if connector.get_type() == "opcua_asyncio":
#                 self.assertTrue(len(ts_int_state) >= 4 and list_intersect_ordered([-1, -1, 0, 1, 2], [v for k,v in ts_int_state]), f"Subscription did not deliver data for Machine {sn}, data: {ts_int_state}")
#             elif connector.get_type() == "opcua":
#                 self.assertTrue(len(ts_int_state) >= 4 and list_intersect_ordered(["-1", "0", "1", "2", "3"], [v for k,v in ts_int_state]), f"Subscription did not deliver data for Machine {sn}, data: {ts_int_state}")
#             else:
#                 raise NotImplemented(f"Unknown connector type: {connector.get_type()}")
#
#         await test_subscribed(1)
#         await test_subscribed(2)
#         await test_subscribed(3)
#
#     @unittest.skip("Skip, server doesn't stop after test in case of batch running")
#     async def test_connection(self):
#         await self.impl_test_connection(OpcUaConnector, 'opcua')
#
#     @unittest.skip("Skip, server doesn't stop after test in case of batch running")
#     async def test_connection_asyncio(self):
#         await self.impl_test_connection(OpcUaConnectorAsyncIO, 'opcua_asyncio')
