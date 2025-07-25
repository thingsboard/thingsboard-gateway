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
from unittest.mock import MagicMock, patch, AsyncMock
from asyncua import Node
from asyncua.ua import NodeId, UaError
from asyncua.ua.uaerrors import BadNoMatch
from more_itertools.more import side_effect

from tests.unit.connectors.opcua.opcua_base_test import OpcUABaseTest
from thingsboard_gateway.connectors.opcua.entities.rpc_request import OpcUaRpcRequest, OpcUaRpcType


class TestOpcUaConnectorServerSideRpc(OpcUABaseTest):

    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.fake_device = self.create_fake_device('rpc/opcua_config_empty_section_rpc.json')
        self.connector._OpcUaConnector__device_nodes.append(self.fake_device)

    async def test_execute_connector_rpc(self):
        result = {'result': 15}
        payload = {'id': '0', 'method': 'opcua_multiply',
                   'params': {'arguments': [{'type': 'integer', 'value': 5}, {'type': 'integer', 'value': 6}],
                              'connectorId': '8bd78640-1888-4a6c-b43e-98003cda158a', 'method': 'multiply'}}
        done_future = Future()
        done_future.set_result(result)
        rpc_request = OpcUaRpcRequest(payload)
        self.assertEqual(rpc_request.rpc_type, OpcUaRpcType.CONNECTOR)
        self.assertEqual(rpc_request.arguments, [5, 6])
        self.assertEqual(rpc_request.rpc_method, 'multiply')
        with patch.object(self.connector._OpcUaConnector__loop, "create_task",
                          return_value=done_future) as create_task_mock:
            results = self.connector._OpcUaConnector__process_connector_rpc_request(rpc_request=rpc_request)

        create_task_mock.assert_called_once()
        self.assertIsInstance(results, list)
        self.assertEqual(results, [{'result': 15}])

    async def test_execute_connector_rpc_with_unsupported_amount_of_arguments(self):
        result = {'error': 'An unexpected error occurred.(BadUnexpectedError)'}
        payload = {'id': '11', 'method': 'opcua_multiply', 'params': {
            'arguments': [{'type': 'integer', 'value': 3}, {'type': 'integer', 'value': 5},
                          {'type': 'integer', 'value': 6}], 'connectorId': '8bd78640-1888-4a6c-b43e-98003cda158a',
            'method': 'multiply'}}
        done_future = Future()
        done_future.set_result(result)
        rpc_request = OpcUaRpcRequest(payload)
        self.assertEqual(rpc_request.rpc_type, OpcUaRpcType.CONNECTOR)
        self.assertEqual(rpc_request.arguments, [3, 5, 6])
        self.assertEqual(rpc_request.rpc_method, 'multiply')

        with patch.object(self.connector._OpcUaConnector__loop, "create_task",
                          return_value=done_future) as create_task_mock:
            results = self.connector._OpcUaConnector__process_connector_rpc_request(rpc_request=rpc_request)
        create_task_mock.assert_called_once()
        self.assertIsInstance(results, list)
        self.assertEqual(results, [{'error': 'An unexpected error occurred.(BadUnexpectedError)'}])

    async def test_execute_connector_rpc_with_unknown_method(self):
        result = {'error': 'The requested operation has no match to return.(BadNoMatch)'}
        payload = {'id': '18', 'method': 'opcua_abba',
                   'params': {'arguments': [{'type': 'integer', 'value': 3}, {'type': 'integer', 'value': 5}],
                              'connectorId': '8bd78640-1888-4a6c-b43e-98003cda158a',
                              'method': 'opcua_abba'}}
        done_future = Future()
        done_future.set_result(result)
        rpc_request = OpcUaRpcRequest(payload)
        self.assertEqual(rpc_request.rpc_type, OpcUaRpcType.CONNECTOR)
        self.assertEqual(rpc_request.arguments, [3, 5])
        self.assertEqual(rpc_request.rpc_method, 'abba')
        with patch.object(self.connector._OpcUaConnector__loop, "create_task",
                          return_value=done_future) as create_task_mock:
            results = self.connector._OpcUaConnector__process_connector_rpc_request(rpc_request=rpc_request)

        create_task_mock.assert_called_once()
        self.assertIsInstance(results, list)
        self.assertEqual(results, [result])


class TestOpcUaDeviceServerSideRpc(OpcUABaseTest):

    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.fake_device = self.create_fake_device('rpc/opcua_config_rpc_with_no_values.json')
        self.connector._OpcUaConnector__device_nodes.append(self.fake_device)
        self.connector._OpcUaConnector__gateway = MagicMock()

    async def test_execute_device_rpc(self):
        payload = {'data': {'id': 49, 'method': 'multiply', 'params': [2, 5]}, 'device': 'OPCUA New Advanced Device',
                   'id': 49}
        result = {"result": {"result": 10}}
        done_future = Future()
        done_future.set_result(result)
        rpc_request = OpcUaRpcRequest(payload)
        self.assertEqual(rpc_request.rpc_type, OpcUaRpcType.DEVICE)
        self.assertEqual(rpc_request.rpc_method, "multiply")
        self.assertIsNone(rpc_request.arguments)
        with patch.object(self.connector._OpcUaConnector__loop, "create_task",
                          return_value=done_future) as create_task_mock:
            results = self.connector._OpcUaConnector__process_device_rpc_request(rpc_request=rpc_request)
            device = self.connector._OpcUaConnector__get_device_by_name(rpc_request.device_name)

        self.assertEqual(rpc_request.arguments, [2, 5])
        self.assertIs(device, self.fake_device)
        create_task_mock.assert_called_once()
        self.assertEqual(results, result)
        self.connector._OpcUaConnector__gateway.send_rpc_reply.assert_called_once_with(
            rpc_request.device_name,
            rpc_request.id,
            {"result": result}
        )

    async def test_execute_device_rpc_with_no_arguments(self):
        payload = {'data': {'id': 16, 'method': 'multiply', 'params': None}, 'device': 'OPCUA New Advanced Device',
                   'id': 16}
        result = {"result": {"error": "multiply - No arguments provided"}}
        rpc_request = OpcUaRpcRequest(payload)
        self.assertEqual(rpc_request.rpc_type, OpcUaRpcType.DEVICE)
        self.assertEqual(rpc_request.rpc_method, "multiply")
        self.assertIsNone(rpc_request.arguments)
        results = self.connector._OpcUaConnector__process_device_rpc_request(rpc_request=rpc_request)
        self.assertIsNone(results)
        self.connector._OpcUaConnector__gateway.send_rpc_reply.assert_called_once_with(
            rpc_request.device_name,
            rpc_request.id,
            result
        )

    async def test_execute_device_rpc_with_unknown_method(self):
        payload = {'data': {'id': 26, 'method': 'frfrffr', 'params': [5, 6]}, 'device': 'OPCUA New Advanced Device',
                   'id': 26}
        result = {'result': {'error': 'frfrffr - No configuration provided for method'}}
        rpc_request = OpcUaRpcRequest(payload)
        self.assertEqual(rpc_request.rpc_type, OpcUaRpcType.DEVICE)
        self.assertEqual(rpc_request.rpc_method, "frfrffr")
        self.assertIsNone(rpc_request.arguments)
        results = self.connector._OpcUaConnector__process_device_rpc_request(rpc_request=rpc_request)
        self.assertIsNone(results)
        self.connector._OpcUaConnector__gateway.send_rpc_reply.assert_called_once_with(
            rpc_request.device_name,
            rpc_request.id,
            result
        )

    async def test_execute_device_rpc_with_specified_arguments(self):
        payload = {'data': {'id': 28, 'method': 'multiply', 'params': None}, 'device': 'OPCUA New Advanced Device', 'id': 28}
        result = {"result":{"result":30}}
        self.fake_device = self.create_fake_device('rpc/opcua_config_rpc_with_values.json')
        


