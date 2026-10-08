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

from asyncio import Future
from unittest.mock import patch

from asyncua import Node
from asyncua.ua import NodeId

from tests.unit.connectors.opcua.opcua_base_test import OpcUABaseTest
from thingsboard_gateway.gateway.entities.rpc_request import RPCType


class TestOpcUaConnectorServerSideRpc(OpcUABaseTest):

    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.fake_device = self.create_fake_device('rpc/opcua_config_empty_section_rpc.json')
        self.connector._OpcUaConnector__device_nodes.append(self.fake_device)

    async def test_connector_rpc_cases(self):
        cases = [
            ("execute_connector_rpc", '0', 'multiply', [5, 6], {'result': 15}),
            ("execute_connector_rpc_with_unsupported_amount_of_arguments", '11', 'multiply', [3, 5, 6],
             {'error': 'An unexpected error occurred.(BadUnexpectedError)'}),
            ("execute_connector_rpc_with_unknown_method", '18', 'abba', [3, 5],
             {'error': 'The requested operation has no match to return.(BadNoMatch)'}),
        ]

        for name, rpc_id, method_name, args, task_result in cases:
            with self.subTest(name=name):
                rpc_request = self.make_connector_rpc_request(rpc_id, method_name, args)

                self.assertEqual(rpc_request.rpc_type, RPCType.CONNECTOR)
                self.assertEqual(rpc_request.connector_type, 'opcua')
                self.assertEqual(rpc_request.method_name, method_name)

                done_future, create_task_mock, response = self.call_connector_with_result(rpc_request, task_result)

                create_task_mock.assert_called_once()
                expected_entry = {**task_result, 'device_name': self.fake_device.name}
                self.assertEqual(response.message, {'result': [expected_entry]})
                self.assertEqual(response.id, rpc_id)


class TestOpcUaDeviceServerSideRpc(OpcUABaseTest):

    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.fake_device = self.create_fake_device('rpc/opcua_config_rpc_with_no_values.json')
        self.connector._OpcUaConnector__device_nodes.append(self.fake_device)

    async def test_execute_device_rpc(self):
        rpc_request = self.make_device_rpc_request(49, 'multiply', [2, 5])
        task_result = {'result': 10}

        self.assertEqual(rpc_request.rpc_type, RPCType.DEVICE)
        self.assertEqual(rpc_request.method_name, "multiply")

        done_future, create_task_mock, response = self.call_device_with_result(rpc_request, task_result)

        device = self.connector._OpcUaConnector__get_device_by_name(rpc_request.device_name)
        self.assertIs(device, self.fake_device)
        create_task_mock.assert_called_once()
        self.assertEqual(rpc_request.arguments, [2, 5])
        self.assertEqual(response.message, {"result": task_result})
        self.assertEqual(response.device_name, self.DEVICE_NAME)
        self.assertEqual(response.id, 49)

    async def test_execute_device_rpc_with_unsupported_amount_of_arguments(self):
        rpc_request = self.make_device_rpc_request(50, 'multiply', [2, 5, 6])

        self.assertEqual(rpc_request.rpc_type, RPCType.DEVICE)
        self.assertEqual(rpc_request.method_name, "multiply")

        with patch.object(self.connector._OpcUaConnector__loop, "create_task") as create_task_mock:
            response = self.connector._OpcUaConnector__process_device_rpc_request(rpc_request=rpc_request)

        create_task_mock.assert_not_called()
        self.assertEqual(rpc_request.arguments, {"error": "Expected 2 arguments, but got 3"})
        self.assertEqual(response.message, {"error": "Expected 2 arguments, but got 3"})

    async def test_execute_device_rpc_with_no_arguments_and_defined_argument_section(self):
        rpc_request = self.make_device_rpc_request(50, 'multiply', None)

        self.assertEqual(rpc_request.rpc_type, RPCType.DEVICE)
        self.assertEqual(rpc_request.method_name, "multiply")

        with patch.object(self.connector._OpcUaConnector__loop, "create_task") as create_task_mock:
            response = self.connector._OpcUaConnector__process_device_rpc_request(rpc_request=rpc_request)

        create_task_mock.assert_not_called()
        self.assertEqual(response.message, {"error": "Expected 2 arguments, but got 0"})

    async def test_execute_device_rpc_on_partly_configured_device_section(self):
        self.fake_device = self.create_fake_device('rpc/opcua_config_rpc_partly_defined_arguments.json')
        self.connector._OpcUaConnector__device_nodes = [self.fake_device]
        rpc_request = self.make_device_rpc_request(51, 'multiply', [5])

        self.assertEqual(rpc_request.rpc_type, RPCType.DEVICE)
        self.assertEqual(rpc_request.method_name, "multiply")

        with patch.object(self.connector._OpcUaConnector__loop, "create_task") as create_task_mock:
            response = self.connector._OpcUaConnector__process_device_rpc_request(rpc_request=rpc_request)

        create_task_mock.assert_not_called()
        expected_msg = "You must either define values for arguments in config or along with rpc request"
        self.assertEqual(response.message, {"error": expected_msg})

    async def test_succesfuly_execute_device_rpc_on_partly_configured_device_section_with_given_arguments(self):
        self.fake_device = self.create_fake_device('rpc/opcua_config_rpc_partly_defined_arguments.json')
        self.connector._OpcUaConnector__device_nodes = [self.fake_device]
        rpc_request = self.make_device_rpc_request(52, 'multiply', [5, 2])
        task_result = {'result': 10}

        self.assertEqual(rpc_request.rpc_type, RPCType.DEVICE)
        self.assertEqual(rpc_request.method_name, "multiply")

        done_future, create_task_mock, response = self.call_device_with_result(rpc_request, task_result)

        self.assertEqual(rpc_request.arguments, [5, 2])
        create_task_mock.assert_called_once()
        self.assertEqual(response.message, {"result": task_result})

    async def test_execute_device_rpc_with_incorrect_data_format(self):
        self.fake_device = self.create_fake_device('rpc/opcua_config_multiple_methods_section.json')
        self.connector._OpcUaConnector__device_nodes = [self.fake_device]
        rpc_request = self.make_device_rpc_request(71, 'multiply', '5 76')

        self.assertEqual(rpc_request.rpc_type, RPCType.DEVICE)
        self.assertEqual(rpc_request.method_name, "multiply")

        with patch.object(self.connector._OpcUaConnector__loop, "create_task") as create_task_mock:
            response = self.connector._OpcUaConnector__process_device_rpc_request(rpc_request=rpc_request)

        create_task_mock.assert_not_called()
        self.assertEqual(response.message, {"error": "The arguments must be specified in the square quotes []"})

    async def test_execute_device_rpc_fails_on_timeout(self):
        rpc_request = self.make_device_rpc_request(67, 'multiply', [3, 8])

        self.assertEqual(rpc_request.rpc_type, RPCType.DEVICE)
        self.assertEqual(rpc_request.method_name, "multiply")

        pending_future = Future()
        OPC_MOD = "thingsboard_gateway.connectors.opcua.opcua_connector"

        with patch.object(self.connector._OpcUaConnector__loop, "create_task", return_value=pending_future), \
                patch(f"{OPC_MOD}.sleep", return_value=None), \
                patch(f"{OPC_MOD}.monotonic", side_effect=[0.0, 999.0]):
            response = self.connector._OpcUaConnector__process_device_rpc_request(rpc_request=rpc_request)

        expected_msg = f"Failed to process rpc request for {self.DEVICE_NAME}, timeout has been reached"
        self.assertEqual(response.message, {"error": expected_msg})

    async def test_execute_device_rpc_with_unknown_method(self):
        rpc_request = self.make_device_rpc_request(26, 'frfrffr', [5, 6])

        self.assertEqual(rpc_request.rpc_type, RPCType.DEVICE)
        self.assertEqual(rpc_request.method_name, "frfrffr")

        with patch.object(self.connector._OpcUaConnector__loop, "create_task") as create_task_mock:
            response = self.connector._OpcUaConnector__process_device_rpc_request(rpc_request=rpc_request)

        create_task_mock.assert_not_called()
        self.assertEqual(response.message, {"error": "Requested rpc method is not found in config"})

    async def test_execute_device_rpc_with_specified_arguments(self):
        self.fake_device = self.create_fake_device('rpc/opcua_config_rpc_with_values.json')
        self.connector._OpcUaConnector__device_nodes = [self.fake_device]
        rpc_request = self.make_device_rpc_request(28, 'multiply', None)
        task_result = {'result': 8}

        self.assertEqual(rpc_request.rpc_type, RPCType.DEVICE)
        self.assertEqual(rpc_request.method_name, "multiply")

        done_future, create_task_mock, response = self.call_device_with_result(rpc_request, task_result)

        self.assertEqual(rpc_request.arguments, [2, 4])
        create_task_mock.assert_called_once()
        self.assertEqual(response.message, {"result": task_result})

    async def test_execute_device_rpc_on_empty_device_section(self):
        self.fake_device = self.create_fake_device('rpc/opcua_config_empty_section_rpc.json')
        self.connector._OpcUaConnector__device_nodes = [self.fake_device]
        rpc_request = self.make_device_rpc_request(29, 'multiply', [2, 5])

        self.assertEqual(rpc_request.rpc_type, RPCType.DEVICE)
        self.assertEqual(rpc_request.method_name, "multiply")

        with patch.object(self.connector._OpcUaConnector__loop, "create_task") as create_task_mock:
            response = self.connector._OpcUaConnector__process_device_rpc_request(rpc_request=rpc_request)

        create_task_mock.assert_not_called()
        self.assertEqual(response.message, {"error": "Requested rpc method is not found in config"})

    async def test_correctly_execute_rpc_for_multiple_methods_in_config(self):
        self.fake_device = self.create_fake_device('rpc/opcua_config_multiple_methods_section.json')
        self.connector._OpcUaConnector__device_nodes = [self.fake_device]
        rpc_request = self.make_device_rpc_request(67, 'multiply', None)
        task_result = {'result': 10}

        self.assertEqual(rpc_request.rpc_type, RPCType.DEVICE)
        self.assertEqual(rpc_request.method_name, "multiply")

        done_future, create_task_mock, response = self.call_device_with_result(rpc_request, task_result)

        self.assertEqual(rpc_request.arguments, [2, 5])
        create_task_mock.assert_called_once()
        self.assertEqual(response.message, {"result": task_result})

    async def test_correctly_execute_rpc_for_multiple_methods_in_config_with_specified_arguments(self):
        self.fake_device = self.create_fake_device('rpc/opcua_config_multiple_methods_section.json')
        self.connector._OpcUaConnector__device_nodes = [self.fake_device]
        rpc_request = self.make_device_rpc_request(68, 'multiply', [9, 8])
        task_result = {'result': 72}

        self.assertEqual(rpc_request.rpc_type, RPCType.DEVICE)
        self.assertEqual(rpc_request.method_name, "multiply")

        done_future, create_task_mock, response = self.call_device_with_result(rpc_request, task_result)

        self.assertEqual(rpc_request.arguments, [9, 8])
        create_task_mock.assert_called_once()
        self.assertEqual(response.message, {"result": task_result})

    async def test_execute_device_rpc_get_relay_without_configured_or_request_arguments(self):
        # Regression test: a method with no "arguments" configured and no params in the
        # request used to leave rpc_request.arguments as None, which crashed `*arguments`
        # unpacking in __call_method with "Value after * must be an iterable, not NoneType".
        self.fake_device = self.create_fake_device('rpc/opcua_config_rpc_no_arguments_defined.json')
        self.connector._OpcUaConnector__device_nodes = [self.fake_device]
        rpc_request = self.make_device_rpc_request(72, 'get_relay', None)
        task_result = {'result': True}

        self.assertEqual(rpc_request.rpc_type, RPCType.DEVICE)
        self.assertEqual(rpc_request.method_name, "get_relay")

        done_future, create_task_mock, response = self.call_device_with_result(rpc_request, task_result)

        self.assertEqual(rpc_request.arguments, [])
        create_task_mock.assert_called_once()
        self.assertEqual(response.message, {"result": task_result})

    async def test_execute_device_rpc_set_relay_with_scalar_request_argument(self):
        # Regression test: a method with no "arguments" configured but a single scalar value
        # in params (e.g. `set_relay true`) used to pass that scalar straight through, which
        # crashed `*arguments` unpacking in __call_method with "...not bool".
        self.fake_device = self.create_fake_device('rpc/opcua_config_rpc_no_arguments_defined.json')
        self.connector._OpcUaConnector__device_nodes = [self.fake_device]
        rpc_request = self.make_device_rpc_request(73, 'set_relay', True)
        task_result = {'result': None}

        self.assertEqual(rpc_request.rpc_type, RPCType.DEVICE)
        self.assertEqual(rpc_request.method_name, "set_relay")

        done_future, create_task_mock, response = self.call_device_with_result(rpc_request, task_result)

        self.assertEqual(rpc_request.arguments, [True])
        create_task_mock.assert_called_once()
        self.assertEqual(response.message, {"result": task_result})


class TestOpcUaReservedGetRpcRpcRequest(OpcUABaseTest):
    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.fake_device = self.create_fake_device('rpc/opcua_config_empty_section_rpc.json')
        self.connector._OpcUaConnector__device_nodes.append(self.fake_device)

    async def test_execute_get_device_rpc_with_node_identifier(self):
        rpc_request = self.make_reserved_rpc_request(48, "get", "ns=2;i=13")
        task_result = {"value": 6}

        self.assertEqual(rpc_request.rpc_type, RPCType.RESERVED)
        self.assertEqual(rpc_request.method_name, "get")

        done_future, create_task_mock, response = self.call_reserved_with_result(rpc_request, task_result)

        self.assertEqual(rpc_request.params, 'ns=2;i=13')
        create_task_mock.assert_called_once()
        self.assertEqual(response.message, {"result": task_result})

    async def test_execute_get_device_rpc_with_invalid_node_identifier(self):
        rpc_request = self.make_reserved_rpc_request(57, "get", "ns=200;i=1300")
        task_result = {"error": "The node id refers to a node that does not exist in the server "
                                "address space.(BadNodeIdUnknown)"}

        done_future, create_task_mock, response = self.call_reserved_with_result(rpc_request, task_result)

        self.assertEqual(rpc_request.params, 'ns=200;i=1300')
        create_task_mock.assert_called_once()
        self.assertEqual(response.message, {"result": task_result})

    async def test_execute_get_device_rpc_with_relative_path(self):
        rpc_request = self.make_reserved_rpc_request(64, "get", "Frequency")
        task_result = {"value": 6}

        done_future, create_task_mock, response = self.call_reserved_with_result(rpc_request, task_result)

        self.assertIsInstance(rpc_request.received_identifier, Node)
        self.assertEqual(rpc_request.received_identifier.nodeid, NodeId(13, 2))
        create_task_mock.assert_called_once()
        self.assertEqual(response.message, {"result": task_result})

    async def test_execute_get_device_rpc_with_incorrect_relative_path(self):
        rpc_request = self.make_reserved_rpc_request(65, "get", "Frequnecy")
        task_result = {"error": "argument to node must be a NodeId object or a string defining a "
                                "nodeid found None of type <class 'NoneType'>"}

        done_future, create_task_mock, response = self.call_reserved_with_result(rpc_request, task_result)

        self.assertIsNone(rpc_request.received_identifier)
        self.assertEqual(response.message, {"result": task_result})

    async def test_execute_get_device_rpc_with_absolute_path(self):
        rpc_request = self.make_reserved_rpc_request(129, "get", r"Root\.Objects\.TempSensor\.Frequency")
        task_result = {"value": 6}

        done_future, create_task_mock, response = self.call_reserved_with_result(rpc_request, task_result)

        self.assertEqual(response.message, {"result": task_result})

    async def test_execute_get_device_rpc_with_incorrect_absolute_path(self):
        rpc_request = self.make_reserved_rpc_request(82, "get", r"Root\.Objects\.TempSensor\.Frequencfef")
        task_result = {"error": "argument to node must be a NodeId object or a string defining a "
                                "nodeid found None of type <class 'NoneType'>"}
        done_future, create_task_mock, response = self.call_reserved_with_result(rpc_request, task_result)

        self.assertIsNone(rpc_request.received_identifier)
        self.assertEqual(response.message, {"result": task_result})


class TestOpcUaReservedServerSideRpc(OpcUABaseTest):

    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.fake_device = self.create_fake_device('rpc/opcua_config_empty_section_rpc.json')
        self.connector._OpcUaConnector__device_nodes.append(self.fake_device)

    async def test_execute_reserved_rpc_with_node_identifier(self):
        rpc_request = self.make_reserved_rpc_request(89, "set", "ns=2;i=13; 56;")
        task_result = {"value": "56"}

        self.assertEqual(rpc_request.rpc_type, RPCType.RESERVED)
        self.assertEqual(rpc_request.method_name, 'set')

        done_future, create_task_mock, response = self.call_reserved_with_result(rpc_request, task_result)

        self.assertEqual(rpc_request.arguments, '56')
        self.assertEqual(rpc_request.params, 'ns=2;i=13')
        create_task_mock.assert_called_once()
        self.assertEqual(response.message, {"result": task_result})

    async def test_execute_reserved_rpc_with_invalid_node_identifier(self):
        rpc_request = self.make_reserved_rpc_request(91, "set", "ns=200;i=136; 91;")
        task_result = {"error": "Failed to send request to OPC UA server"}

        self.assertEqual(rpc_request.rpc_type, RPCType.RESERVED)
        self.assertEqual(rpc_request.method_name, 'set')

        done_future, create_task_mock, response = self.call_reserved_with_result(rpc_request, task_result)

        self.assertEqual(rpc_request.arguments, '91')
        self.assertEqual(rpc_request.params, 'ns=200;i=136')
        create_task_mock.assert_called_once()
        self.assertEqual(response.message, {"result": task_result})

    async def test_execute_reserved_rpc_with_relative_path(self):
        rpc_request = self.make_reserved_rpc_request(97, "set", "Frequncy; 35;")
        task_result = {"value": "35"}

        self.assertEqual(rpc_request.rpc_type, RPCType.RESERVED)
        self.assertEqual(rpc_request.method_name, 'set')

        done_future, create_task_mock, response = self.call_reserved_with_result(rpc_request, task_result)

        self.assertEqual(rpc_request.arguments, '35')
        self.assertIsNone(rpc_request.received_identifier)
        self.assertEqual(response.message, {"result": task_result})

    async def test_execute_reserved_rpc_with_unknown_relative_path(self):
        rpc_request = self.make_reserved_rpc_request(100, "set", "Frequenc4tr; 35;")
        task_result = {"error": "'NoneType' object has no attribute 'write_value'"}

        self.assertEqual(rpc_request.rpc_type, RPCType.RESERVED)
        self.assertEqual(rpc_request.method_name, 'set')

        done_future, create_task_mock, response = self.call_reserved_with_result(rpc_request, task_result)

        self.assertEqual(rpc_request.arguments, '35')
        self.assertEqual(rpc_request.params, 'Frequenc4tr')
        self.assertIsNone(rpc_request.received_identifier)
        self.assertEqual(response.message, {"result": task_result})

    async def test_execute_reserved_rpc_with_full_path(self):
        rpc_request = self.make_reserved_rpc_request(105, "set", r"Root\\\.Objects\\\.TempSensor\\\.Frequency ; 10")
        task_result = {"value": "10"}

        self.assertEqual(rpc_request.rpc_type, RPCType.RESERVED)
        self.assertEqual(rpc_request.method_name, 'set')

        done_future, create_task_mock, response = self.call_reserved_with_result(rpc_request, task_result)

        self.assertEqual(rpc_request.arguments, '10')
        self.assertEqual(rpc_request.params, r"Root\\\.Objects\\\.TempSensor\\\.Frequency")
        self.assertIsNone(rpc_request.received_identifier)
        self.assertEqual(response.message, {"result": task_result})

    async def test_execute_reserved_rpc_with_incorrect_full_path(self):
        rpc_request = self.make_reserved_rpc_request(106, "set", r"Root\\\.Objects\\\.TempSensor\\\.Frequenrr ; 10")
        task_result = {"error": "'NoneType' object has no attribute 'write_value'"}

        self.assertEqual(rpc_request.rpc_type, RPCType.RESERVED)
        self.assertEqual(rpc_request.method_name, 'set')

        done_future, create_task_mock, response = self.call_reserved_with_result(rpc_request, task_result)

        self.assertEqual(rpc_request.arguments, '10')
        self.assertEqual(rpc_request.params, r"Root\\\.Objects\\\.TempSensor\\\.Frequenrr")
        self.assertIsNone(rpc_request.received_identifier)
        self.assertEqual(response.message, {"result": task_result})

    async def test_execute_reserved_rpc_with_different_delimiter(self):
        rpc_request = self.make_reserved_rpc_request(111, "set", "Frequency= 35   ;")
        task_result = {"value": "35"}

        self.assertEqual(rpc_request.rpc_type, RPCType.RESERVED)
        self.assertEqual(rpc_request.method_name, 'set')

        done_future, create_task_mock, response = self.call_reserved_with_result(rpc_request, task_result)

        self.assertEqual(rpc_request.arguments, '35')
        self.assertEqual(rpc_request.params, 'Frequency')

        ident = rpc_request.received_identifier
        self.assertIsInstance(ident, Node)
        self.assertEqual(ident.nodeid, self.FREQ_NODEID)

        self.assertEqual(response.message, {"result": task_result})
