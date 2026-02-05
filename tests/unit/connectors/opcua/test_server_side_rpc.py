#     Copyright 2026. ThingsBoard
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

from tests.unit.connectors.opcua.opcua_base_test import OpcUABaseTest
from asyncio import Future
from unittest.mock import patch


class TestOpcUaConnectorServerSideRpc(OpcUABaseTest):

    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.fake_device = self.create_fake_device('rpc/opcua_config_empty_section_rpc.json')
        self.connector._OpcUaConnector__device_nodes.append(self.fake_device)

    async def test_connector_rpc_cases(self):
        cases = [
            ("execute_connector_rpc",
             '0', 'opcua_multiply', [5, 6], {'result': 15}, 'multiply', None),

            ("execute_connector_rpc_with_unsupported_amount_of_arguments",
             '11', 'opcua_multiply', [3, 5, 6],
             {'error': 'An unexpected error occurred.(BadUnexpectedError)'}, 'multiply', None),

            ("execute_connector_rpc_with_unknown_method",
             '18', 'opcua_abba', [3, 5],
             {'error': 'The requested operation has no match to return.(BadNoMatch)'},
             'abba', 'opcua_abba'),
        ]

        for name, rpc_id, top_method, args, result, expected_method, inner_method in cases:
            with self.subTest(name=name):
                payload = self.make_connector_payload(rpc_id, top_method, args, inner_method=inner_method)
                rpc_request = OpcUaRpcRequest(payload)
                self.assertEqual(rpc_request.rpc_type, OpcUaRpcType.CONNECTOR)
                self.assertEqual(rpc_request.arguments, args)
                self.assertEqual(rpc_request.rpc_method, expected_method)

                done_future, create_task_mock, results = self.call_connector_with_result(rpc_request, result)

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
        payload = {'data': {'id': 49, 'method': 'multiply', 'params': [2, 5]}, 'device': self.DEVICE_NAME, 'id': 49}
        result = {"result": {"result": 10}}
        rpc_request = OpcUaRpcRequest(payload)

        self.assertEqual(rpc_request.rpc_type, OpcUaRpcType.DEVICE)
        self.assertEqual(rpc_request.rpc_method, "multiply")
        self.assertEqual(rpc_request.arguments, [2, 5])

        done_future, create_task_mock, results = self.call_device_with_result(rpc_request, result)

        device = self.connector._OpcUaConnector__get_device_by_name(rpc_request.device_name)
        self.assertIs(device, self.fake_device)
        create_task_mock.assert_called_once()
        self.assertEqual(results, result)
        self.assert_gateway_reply(rpc_request, result)

    async def test_execute_device_rpc_with_unsupported_amount_of_arguments(self):
        payload = {'data': {'id': 50, 'method': 'multiply', 'params': [2, 5, 6]}, 'device': self.DEVICE_NAME, 'id': 50}
        result = {'result': 'Expected 2 arguments, but got 3'}
        rpc_request = OpcUaRpcRequest(payload)

        self.assertEqual(rpc_request.rpc_type, OpcUaRpcType.DEVICE)
        self.assertEqual(rpc_request.rpc_method, "multiply")
        self.assertEqual(rpc_request.arguments, [2, 5, 6])
        results = self.connector._OpcUaConnector__process_device_rpc_request(rpc_request=rpc_request)
        self.assertIsNone(results)
        self.connector._OpcUaConnector__gateway.send_rpc_reply.assert_called_once_with(
            device=rpc_request.device_name,
            req_id=rpc_request.id,
            content=result
        )

    async def test_execute_device_rpc_with_no_arguments_and_defined_argument_section(self):
        payload = {'data': {'id': 50, 'method': 'multiply', 'params': None}, 'device': self.DEVICE_NAME, 'id': 50}
        result = {'result': 'Expected 2 arguments, but got 0'}
        rpc_request = OpcUaRpcRequest(payload)
        self.assertEqual(rpc_request.rpc_type, OpcUaRpcType.DEVICE)
        self.assertEqual(rpc_request.rpc_method, "multiply")
        self.assertIsNone(rpc_request.arguments)
        results = self.connector._OpcUaConnector__process_device_rpc_request(rpc_request=rpc_request)
        self.assertIsNone(results)
        self.connector._OpcUaConnector__gateway.send_rpc_reply.assert_called_once_with(
            device=rpc_request.device_name,
            req_id=rpc_request.id,
            content=result
        )

    async def test_execute_device_rpc_on_partly_configured_device_section(self):
        payload = {'data': {'id': 51, 'method': 'multiply', 'params': [5]}, 'device': self.DEVICE_NAME, 'id': 51}
        result = {"result": "You must either define values for arguments in config or along with rpc request"}
        self.fake_device = self.create_fake_device('rpc/opcua_config_rpc_partly_defined_arguments.json')
        self.connector._OpcUaConnector__device_nodes = [self.fake_device]
        rpc_request = OpcUaRpcRequest(payload)
        self.assertEqual(rpc_request.rpc_type, OpcUaRpcType.DEVICE)
        self.assertEqual(rpc_request.rpc_method, "multiply")
        self.assertEqual(rpc_request.arguments, [5])
        results = self.connector._OpcUaConnector__process_device_rpc_request(rpc_request=rpc_request)
        self.assertIsNone(results)
        self.connector._OpcUaConnector__gateway.send_rpc_reply.assert_called_once_with(
            device=rpc_request.device_name,
            req_id=rpc_request.id,
            content=result
        )

    async def test_succesfuly_execute_device_rpc_on_partly_configured_device_section_with_given_arguments(self):
        payload = {'data': {'id': 52, 'method': 'multiply', 'params': [5, 2]}, 'device': self.DEVICE_NAME, 'id': 52}
        result = {"result": {"result": 10}}
        self.fake_device = self.create_fake_device('rpc/opcua_config_rpc_partly_defined_arguments.json')
        self.connector._OpcUaConnector__device_nodes = [self.fake_device]
        rpc_request = OpcUaRpcRequest(payload)

        self.assertEqual(rpc_request.rpc_type, OpcUaRpcType.DEVICE)
        self.assertEqual(rpc_request.rpc_method, "multiply")
        done_future, create_task_mock, results = self.call_device_with_result(rpc_request, result)

        self.assertEqual(rpc_request.arguments, [5, 2])
        create_task_mock.assert_called_once()
        self.assertEqual(results, result)
        self.assert_gateway_reply(rpc_request, result)

    async def test_execute_device_rpc_with_incorrect_data_format(self):
        payload = {'data': {'id': 71, 'method': 'multiply', 'params': '5 76'}, 'device': self.DEVICE_NAME, 'id': 71}
        result = {"result": "The arguments must be specified in the square quotes []"}
        self.fake_device = self.create_fake_device('rpc/opcua_config_multiple_methods_section.json')
        self.connector._OpcUaConnector__device_nodes = [self.fake_device]
        rpc_request = OpcUaRpcRequest(payload)
        self.assertEqual(rpc_request.rpc_type, OpcUaRpcType.DEVICE)
        self.assertEqual(rpc_request.rpc_method, "multiply")
        self.assertEqual(rpc_request.arguments, '5 76')
        results = self.connector._OpcUaConnector__process_device_rpc_request(rpc_request=rpc_request)
        self.assertIsNone(results)
        self.connector._OpcUaConnector__gateway.send_rpc_reply.assert_called_once_with(
            device=rpc_request.device_name,
            req_id=rpc_request.id,
            content=result
        )


    async def test_execute_device_rpc_fails_on_timeout(self):
        payload = {'data': {'id': 67, 'method': 'multiply', 'params': [3, 8]},
                   'device': self.DEVICE_NAME, 'id': 67}
        result = {'error': f'Timeout rpc has been reached for {self.DEVICE_NAME}'}
        rpc_request = OpcUaRpcRequest(payload)

        self.assertEqual(rpc_request.rpc_type, OpcUaRpcType.DEVICE)
        self.assertEqual(rpc_request.rpc_method, "multiply")

        pending_future = Future()
        OPC_MOD = "thingsboard_gateway.connectors.opcua.opcua_connector"

        with patch.object(self.connector._OpcUaConnector__loop, "create_task", return_value=pending_future), \
                patch(f"{OPC_MOD}.sleep", return_value=None), \
                patch(f"{OPC_MOD}.monotonic", side_effect=[0.0, 999.0]):
            results = self.connector._OpcUaConnector__process_device_rpc_request(rpc_request=rpc_request)

        self.assertEqual(results, result)
        self.connector._OpcUaConnector__gateway.send_rpc_reply.assert_called_once_with(
            rpc_request.device_name, rpc_request.id, {"result": result}
        )

    async def test_execute_device_rpc_with_unknown_method(self):
        payload = {'data': {'id': 26, 'method': 'frfrffr', 'params': [5, 6]}, 'device': self.DEVICE_NAME, 'id': 26}
        result = {'error': 'Requested rpc method is not found in config'}
        rpc_request = OpcUaRpcRequest(payload)

        self.assertEqual(rpc_request.rpc_type, OpcUaRpcType.DEVICE)
        self.assertEqual(rpc_request.rpc_method, "frfrffr")
        self.assertEqual(rpc_request.arguments, [5, 6])

        results = self.connector._OpcUaConnector__process_device_rpc_request(rpc_request=rpc_request)

        self.assertIsNone(results)
        self.connector._OpcUaConnector__gateway.send_rpc_reply.assert_called_once_with(
            device=rpc_request.device_name,
            req_id=rpc_request.id,
            content={"result": result}
        )

    async def test_execute_device_rpc_with_specified_arguments(self):
        payload = {'data': {'id': 28, 'method': 'multiply', 'params': None}, 'device': self.DEVICE_NAME, 'id': 28}
        result = {"result": {"result": 8}}
        self.fake_device = self.create_fake_device('rpc/opcua_config_rpc_with_values.json')
        self.connector._OpcUaConnector__device_nodes = [self.fake_device]

        rpc_request = OpcUaRpcRequest(payload)

        self.assertEqual(rpc_request.rpc_type, OpcUaRpcType.DEVICE)
        self.assertEqual(rpc_request.rpc_method, "multiply")

        done_future, create_task_mock, results = self.call_device_with_result(rpc_request, result)

        self.assertEqual(rpc_request.arguments, [2, 4])
        create_task_mock.assert_called_once()
        self.assertEqual(results, result)
        self.assert_gateway_reply(rpc_request, result)

    async def test_execute_device_rpc_on_empty_device_section(self):
        payload = {'data': {'id': 29, 'method': 'multiply', 'params': [2, 5]}, 'device': self.DEVICE_NAME, 'id': 29}
        result = {"error": "Requested rpc method is not found in config"}
        self.fake_device = self.create_fake_device('rpc/opcua_config_empty_section_rpc.json')
        self.connector._OpcUaConnector__device_nodes = [self.fake_device]
        rpc_request = OpcUaRpcRequest(payload)
        self.assertEqual(rpc_request.rpc_type, OpcUaRpcType.DEVICE)
        self.assertEqual(rpc_request.rpc_method, "multiply")
        done_future, create_task_mock, results = self.call_device_with_result(rpc_request, result)
        self.connector._OpcUaConnector__gateway.send_rpc_reply.assert_called_once_with(
            device=rpc_request.device_name,
            req_id=rpc_request.id,
            content={"result": result}
        )

    async def test_correctly_execute_rpc_for_multiple_methods_in_config(self):
        payload = {'data': {'id': 67, 'method': 'multiply', 'params': None}, 'device': self.DEVICE_NAME, 'id': 67}
        result = {"result": {"result": 10}}
        self.fake_device = self.create_fake_device('rpc/opcua_config_multiple_methods_section.json')
        self.connector._OpcUaConnector__device_nodes = [self.fake_device]
        rpc_request = OpcUaRpcRequest(payload)
        self.assertEqual(rpc_request.rpc_type, OpcUaRpcType.DEVICE)
        self.assertEqual(rpc_request.rpc_method, "multiply")
        self.assertIsNone(rpc_request.arguments)
        done_future, create_task_mock, results = self.call_device_with_result(rpc_request, result)
        create_task_mock.assert_called_once()
        self.assertEqual(results, result)
        self.assert_gateway_reply(rpc_request, result)

    async def test_correctly_execute_rpc_for_multiple_methods_in_config_with_specified_arguments(self):
        payload = {'data': {'id': 68, 'method': 'multiply', 'params': [9, 8]}, 'device': self.DEVICE_NAME, 'id': 68}
        result = {"result": {"result": 72}}
        self.fake_device = self.create_fake_device('rpc/opcua_config_multiple_methods_section.json')
        self.connector._OpcUaConnector__device_nodes = [self.fake_device]
        rpc_request = OpcUaRpcRequest(payload)
        self.assertEqual(rpc_request.rpc_type, OpcUaRpcType.DEVICE)
        self.assertEqual(rpc_request.rpc_method, "multiply")
        self.assertEqual(rpc_request.arguments, [9, 8])
        done_future, create_task_mock, results = self.call_device_with_result(rpc_request, result)
        create_task_mock.assert_called_once()
        self.assertEqual(results, result)
        self.assert_gateway_reply(rpc_request, result)


class TestOpcUaReservedGetRpcRpcRequest(OpcUABaseTest):
    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.fake_device = self.create_fake_device('rpc/opcua_config_empty_section_rpc.json')
        self.connector._OpcUaConnector__device_nodes.append(self.fake_device)
        self.connector._OpcUaConnector__gateway = MagicMock()

    async def test_execute_get_device_rpc_with_node_identifier(self):
        payload = self.make_reserved_payload(48, "get", "ns=2;i=13")
        result = {"result": {"value": 6}}
        rpc_request = OpcUaRpcRequest(payload)

        self.assertEqual(rpc_request.rpc_type, OpcUaRpcType.RESERVED)
        self.assertEqual(rpc_request.rpc_method, "get")

        done_future, create_task_mock, results = self.call_reserved_with_result(rpc_request, result)

        self.assertEqual(rpc_request.params, 'ns=2;i=13')
        create_task_mock.assert_called_once()
        self.assertEqual(results, result)
        self.assert_gateway_reply(rpc_request, result)

    async def test_execute_get_device_rpc_with_invalid_node_identifier(self):
        payload = self.make_reserved_payload(57, "get", "ns=200;i=1300")
        result = {"result": {
            "error": "The node id refers to a node that does not exist in the server address space.(BadNodeIdUnknown)"}}
        rpc_request = OpcUaRpcRequest(payload)

        done_future, create_task_mock, results = self.call_reserved_with_result(rpc_request, result)

        self.assertEqual(rpc_request.params, 'ns=200;i=1300')
        create_task_mock.assert_called_once()
        self.assertEqual(results, result)
        self.assert_gateway_reply(rpc_request, result)

    async def test_execute_get_device_rpc_with_relative_path(self):
        payload = self.make_reserved_payload(64, "get", "Frequency")
        result = {"result": {"value": 6}}
        rpc_request = OpcUaRpcRequest(payload)

        done_future, create_task_mock, results = self.call_reserved_with_result(rpc_request, result)

        self.assertIsInstance(rpc_request.received_identifier, Node)
        self.assertEqual(rpc_request.received_identifier.nodeid, NodeId(13, 2))
        create_task_mock.assert_called_once()
        self.assertEqual(results, result)
        self.assert_gateway_reply(rpc_request, result)

    async def test_execute_get_device_rpc_with_incorrect_relative_path(self):
        payload = self.make_reserved_payload(65, "get", "Frequnecy")
        result = {"result": {
            "error": "argument to node must be a NodeId object or a string defining a nodeid found None of type <class 'NoneType'>"}}
        rpc_request = OpcUaRpcRequest(payload)

        done_future, create_task_mock, results = self.call_reserved_with_result(rpc_request, result)

        self.assertIsNone(rpc_request.received_identifier)
        self.assertEqual(results, result)
        self.assert_gateway_reply(rpc_request, result)

    async def test_execute_get_device_rpc_with_absolute_path(self):
        payload = self.make_reserved_payload(129, "get", r"Root\.Objects\.TempSensor\.Frequency")
        result = {"result": {"value": 6}}
        rpc_request = OpcUaRpcRequest(payload)

        done_future, create_task_mock, results = self.call_reserved_with_result(rpc_request, result)

        self.assertEqual(results, result)
        self.assert_gateway_reply(rpc_request, result)

    async def test_execute_get_device_rpc_with_incorrect_absolute_path(self):
        payload = self.make_reserved_payload(82, "get", r"Root\.Objects\.TempSensor\.Frequencfef")
        result = {"result": {
            "error": "argument to node must be a NodeId object or a string defining a nodeid found None of type <class 'NoneType'>"
        }}
        rpc_request = OpcUaRpcRequest(payload)
        done_future, create_task_mock, results = self.call_reserved_with_result(rpc_request, result)

        self.assertIsNone(rpc_request.received_identifier)
        self.assertEqual(results, result)
        self.assert_gateway_reply(rpc_request, result)



from unittest.mock import MagicMock

from asyncua import Node
from asyncua.ua import NodeId

from tests.unit.connectors.opcua.opcua_base_test import OpcUABaseTest
from thingsboard_gateway.connectors.opcua.entities.rpc_request import OpcUaRpcRequest, OpcUaRpcType


class TestOpcUaReservedServerSideRpc(OpcUABaseTest):

    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.fake_device = self.create_fake_device('rpc/opcua_config_empty_section_rpc.json')
        self.connector._OpcUaConnector__device_nodes.append(self.fake_device)
        self.connector._OpcUaConnector__gateway = MagicMock()

    async def test_execute_reserved_rpc_with_node_identifier(self):
        payload = self.make_reserved_payload(89, "set", "ns=2;i=13; 56;")
        result = {"result": {"value": "56"}}
        rpc_request = OpcUaRpcRequest(payload)

        self.assertEqual(rpc_request.rpc_type, OpcUaRpcType.RESERVED)
        self.assertEqual(rpc_request.rpc_method, 'set')
        self.assertEqual(rpc_request.arguments, '56')

        done_future, create_task_mock, results = self.call_reserved_with_result(rpc_request, result)

        self.assertEqual(rpc_request.params, 'ns=2;i=13')
        create_task_mock.assert_called_once()
        self.assertEqual(results, result)
        self.assert_gateway_reply(rpc_request, result)

    async def test_execute_reserved_rpc_with_invalid_node_identifier(self):
        payload = self.make_reserved_payload(91, "set", "ns=200;i=136; 91;")
        result = {"result": {"error": "Failed to send request to OPC UA server"}}
        rpc_request = OpcUaRpcRequest(payload)

        self.assertEqual(rpc_request.rpc_type, OpcUaRpcType.RESERVED)
        self.assertEqual(rpc_request.rpc_method, 'set')
        self.assertEqual(rpc_request.arguments, '91')

        done_future, create_task_mock, results = self.call_reserved_with_result(rpc_request, result)

        self.assertEqual(rpc_request.params, 'ns=200;i=136')
        create_task_mock.assert_called_once()
        self.assertEqual(results, result)
        self.assert_gateway_reply(rpc_request, result)

    async def test_execute_reserved_rpc_with_relative_path(self):
        payload = self.make_reserved_payload(97, "set", "Frequncy; 35;")
        result = {"result": {"value": "35"}}
        rpc_request = OpcUaRpcRequest(payload)

        self.assertEqual(rpc_request.rpc_type, OpcUaRpcType.RESERVED)
        self.assertEqual(rpc_request.rpc_method, 'set')
        self.assertEqual(rpc_request.arguments, '35')

        done_future, create_task_mock, results = self.call_reserved_with_result(rpc_request, result)

        self.assertIsNone(rpc_request.received_identifier)
        self.assertEqual(results, result)
        self.assert_gateway_reply(rpc_request, result)

    async def test_execute_reserved_rpc_with_unknown_relative_path(self):
        payload = self.make_reserved_payload(100, "set", "Frequenc4tr; 35;")
        result = {"result": {"error": "'NoneType' object has no attribute 'write_value'"}}
        rpc_request = OpcUaRpcRequest(payload)

        self.assertEqual(rpc_request.rpc_type, OpcUaRpcType.RESERVED)
        self.assertEqual(rpc_request.rpc_method, 'set')
        self.assertEqual(rpc_request.arguments, '35')
        self.assertEqual(rpc_request.params, 'Frequenc4tr')

        done_future, create_task_mock, results = self.call_reserved_with_result(rpc_request, result)

        self.assertIsNone(rpc_request.received_identifier)
        self.assertEqual(results, result)
        self.assert_gateway_reply(rpc_request, result)

    async def test_execute_reserved_rpc_with_full_path(self):
        payload = self.make_reserved_payload(105, "set", r"Root\\\.Objects\\\.TempSensor\\\.Frequency ; 10")
        result = {"result": {"value": "10"}}
        rpc_request = OpcUaRpcRequest(payload)

        self.assertEqual(rpc_request.rpc_type, OpcUaRpcType.RESERVED)
        self.assertEqual(rpc_request.rpc_method, 'set')
        self.assertEqual(rpc_request.arguments, '10')
        self.assertEqual(rpc_request.params, r"Root\\\.Objects\\\.TempSensor\\\.Frequency")

        done_future, create_task_mock, results = self.call_reserved_with_result(rpc_request, result)

        self.assertIsNone(rpc_request.received_identifier)
        self.assertEqual(results, result)
        self.assert_gateway_reply(rpc_request, result)

    async def test_execute_reserved_rpc_with_incorrect_full_path(self):
        payload = self.make_reserved_payload(106, "set", r"Root\\\.Objects\\\.TempSensor\\\.Frequenrr ; 10")
        result = {"result": {"error": "'NoneType' object has no attribute 'write_value'"}}
        rpc_request = OpcUaRpcRequest(payload)

        self.assertEqual(rpc_request.rpc_type, OpcUaRpcType.RESERVED)
        self.assertEqual(rpc_request.rpc_method, 'set')
        self.assertEqual(rpc_request.arguments, '10')
        self.assertEqual(rpc_request.params, r"Root\\\.Objects\\\.TempSensor\\\.Frequenrr")

        done_future, create_task_mock, results = self.call_reserved_with_result(rpc_request, result)

        self.assertIsNone(rpc_request.received_identifier)
        self.assertEqual(results, result)
        self.assert_gateway_reply(rpc_request, result)

    async def test_execute_reserved_rpc_with_different_delimiter(self):
        payload = self.make_reserved_payload(111, "set", "Frequency= 35   ;")
        result = {"result": {"value": "35"}}
        rpc_request = OpcUaRpcRequest(payload)

        self.assertEqual(rpc_request.rpc_type, OpcUaRpcType.RESERVED)
        self.assertEqual(rpc_request.rpc_method, 'set')
        self.assertEqual(rpc_request.arguments, '35')
        self.assertEqual(rpc_request.params, 'Frequency')

        done_future, create_task_mock, results = self.call_reserved_with_result(rpc_request, result)

        ident = rpc_request.received_identifier
        self.assertIsInstance(ident, Node)
        self.assertEqual(ident.nodeid, self.FREQ_NODEID)

        self.assertEqual(results, result)
        self.assert_gateway_reply(rpc_request, result)
