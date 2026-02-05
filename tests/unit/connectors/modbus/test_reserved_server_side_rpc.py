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

from unittest.mock import MagicMock, patch
from tests.unit.connectors.modbus.modbus_base_test import ServerSideRPCModbusSetUp
from thingsboard_gateway.connectors.modbus.entities.rpc_request import RPCRequest

LOGNAME = "Modbus test"


class TestReservedModbusRPC(ServerSideRPCModbusSetUp):

    async def test_get_reserved_modbus_rpc_success(self):
        content = {'data': {'id': 96, 'method': 'get', 'params': 'type=16int;functionCode=3;objectsCount=1;address=2;'},
                   'device': 'Demo Device', 'id': 96}
        rpc_request = RPCRequest(content=content)
        self.assertEqual(
            rpc_request.params,
            {'type': '16int', 'functionCode': 3, 'objectsCount': 1, 'address': 2}
        )

        fake_task = MagicMock()
        fake_task.done.return_value = True
        expected_result = {"value": 78}

        with patch.object(self.connector, "_AsyncModbusConnector__create_task", return_value=fake_task) as ct_mock, \
                patch.object(self.connector, "_AsyncModbusConnector__wait_task_with_timeout",
                             return_value=(True, expected_result)) as wait_mock:
            out = self.connector._AsyncModbusConnector__process_reserved_rpc_request(rpc_request)

        ct_mock.assert_called_once()
        func, args, kwargs = ct_mock.call_args.args
        device, passed_params, passed_rpc = args
        self.assertIs(device, self.slave)
        self.assertEqual(passed_params, rpc_request.params)
        self.assertIs(passed_rpc, rpc_request)
        self.assertEqual(kwargs, {})

        wait_mock.assert_called_once_with(task=fake_task, timeout=rpc_request.timeout, poll_interval=0.2)
        self.assertEqual(out, expected_result)
        self.assertEqual(out, expected_result)
        self.gateway.send_rpc_reply.assert_called_once_with(
            self.slave.device_name, 96, {"result": expected_result}
        )

    async def test_get_reserved_modbus_rpc_incorrect_request_schema(self):
        content = {
            'data': {'id': 97, 'method': 'get',
                     'params': 'type=16int;functionCode=333;objectsCount=1;address=2;'},
            'device': self.slave.device_name, 'id': 97
        }
        with self.assertRaises(ValueError) as e:
            RPCRequest(content=content)
        self.assertEqual(str(e.exception),
                         'The requested RPC either does not match with the schema get type=<type>;functionCode=<functionCode>;objectsCount=<objectsCount>;address=<address>; or incorrect value/values provided')

    async def test_get_reserved_modbus_rpc_no_device(self):
        content = {
            'data': {
                'id': 98,
                'method': 'get',
                'params': 'type=16int;functionCode=3;objectsCount=1;address=2;'
            },
            'device': 'Ghost Device',
            'id': 98
        }
        rpc_request = RPCRequest(content=content)

        with patch.object(self.connector, "get_name", return_value="AsyncModbusConnector(TEST)") as name_mock, \
                patch.object(self.connector, "_AsyncModbusConnector__create_task") as ct_mock, \
                self.assertLogs(LOGNAME, level="ERROR") as logcap:
            self.connector._AsyncModbusConnector__process_reserved_rpc_request(rpc_request)
        name_mock.assert_called_once_with()
        self.gateway.send_rpc_reply.assert_called_once()
        _, kwargs = self.gateway.send_rpc_reply.call_args
        assert kwargs == {
            "device": "Ghost Device",
            "req_id": 98,
            "content": {"result": {"error": "Device not found"}}
        }
        assert any(
            "Device Ghost Device not found in connector AsyncModbusConnector(TEST)" in m
            for m in logcap.output
        )
        ct_mock.assert_not_called()

    async def test_get_reserved_modbus_rpc_fails_on_timeout(self):
        content = {
            'data': {'id': 96, 'method': 'get',
                     'params': 'type=16int;functionCode=3;objectsCount=1;address=2;'},
            'device': self.slave.device_name, 'id': 96
        }
        rpc_request = RPCRequest(content=content)

        fake_task = MagicMock()

        with patch.object(self.connector, "_AsyncModbusConnector__create_task", return_value=fake_task), \
                patch.object(self.connector, "_AsyncModbusConnector__wait_task_with_timeout",
                             return_value=(False, None)):
            out = self.connector._AsyncModbusConnector__process_reserved_rpc_request(rpc_request)

        self.assertEqual(out, {"error": f"Timeout rpc has been reached for {self.slave.device_name}"})
        self.gateway.send_rpc_reply.assert_called_once_with(
            self.slave.device_name, 96, {"result": out}
        )

    async def test_set_reserved_modbus_rpc_success(self):
        content = {
            'data': {'id': 99, 'method': 'set',
                     'params': 'type=16int;functionCode=6;objectsCount=1;address=2;value=77;'},
            'device': self.slave.device_name, 'id': 99
        }
        rpc_request = RPCRequest(content=content)

        self.assertEqual(
            rpc_request.params,
            {'type': '16int', 'functionCode': 6, 'objectsCount': 1, 'address': 2}
        )
        self.assertEqual(rpc_request.value, {'data': {'params': '77'}})

        fake_task = MagicMock()
        expected_result = {"value": 77}

        with patch.object(self.connector, "_AsyncModbusConnector__create_task", return_value=fake_task), \
                patch.object(self.connector, "_AsyncModbusConnector__wait_task_with_timeout",
                             return_value=(True, expected_result)):
            out = self.connector._AsyncModbusConnector__process_reserved_rpc_request(rpc_request)

        self.assertEqual(out, expected_result)
        self.gateway.send_rpc_reply.assert_called_once_with(
            self.slave.device_name, 99, {"result": expected_result}
        )

    async def test_set_reserved_modbus_rpc_incorrect_request_schema(self):
        content = {
            'data': {'id': 100, 'method': 'set',
                     'params': 'type=16int;functionCode=444;objectsCount=1;address=2;value=77;'},
            'device': self.slave.device_name, 'id': 100
        }
        with self.assertRaises(ValueError):
            RPCRequest(content=content)

    async def test_set_reserved_modbus_rpc_no_device(self):
        content = {
            'data': {
                'id': 101,
                'method': 'set',
                'params': 'type=16int;functionCode=6;objectsCount=1;address=2;value=77;'
            },
            'device': 'Ghost Device',
            'id': 101
        }
        rpc_request = RPCRequest(content=content)

        with patch.object(self.connector, "get_name", return_value="AsyncModbusConnector(TEST)") as name_mock, \
                patch.object(self.connector, "_AsyncModbusConnector__create_task") as ct_mock, \
                self.assertLogs(LOGNAME, level="ERROR") as logcap:
            self.connector._AsyncModbusConnector__process_reserved_rpc_request(rpc_request)

        name_mock.assert_called_once_with()

        self.gateway.send_rpc_reply.assert_called_once()
        _, kwargs = self.gateway.send_rpc_reply.call_args
        assert kwargs == {
            "device": "Ghost Device",
            "req_id": 101,
            "content": {"result": {"error": "Device not found"}}
        }

        assert any(
            "Device Ghost Device not found in connector AsyncModbusConnector(TEST)" in m
            for m in logcap.output
        )

        ct_mock.assert_not_called()

    async def test_set_reserved_modbus_rpc_fails_on_timeout(self):
        content = {
            'data': {'id': 102, 'method': 'set',
                     'params': 'type=16int;functionCode=6;objectsCount=1;address=2;value=77;'},
            'device': self.slave.device_name, 'id': 102
        }
        rpc_request = RPCRequest(content=content)

        fake_task = MagicMock()

        with patch.object(self.connector, "_AsyncModbusConnector__create_task", return_value=fake_task), \
                patch.object(self.connector, "_AsyncModbusConnector__wait_task_with_timeout",
                             return_value=(False, None)):
            out = self.connector._AsyncModbusConnector__process_reserved_rpc_request(rpc_request)

        self.assertEqual(out, {"error": f"Timeout rpc has been reached for {self.slave.device_name}"})
        self.gateway.send_rpc_reply.assert_called_once_with(
            self.slave.device_name, 102, {"result": out}
        )

    async def test_set_reserved_modbus_rpc_invalid_data_type(self):
        content = {
            'data': {'id': 103, 'method': 'set',
                     'params': 'type=16int;functionCode=6;objectsCount=1;address=2;value=string;'},
            'device': self.slave.device_name, 'id': 103
        }
        rpc_request = RPCRequest(content=content)

        fake_task = MagicMock()
        err_payload = {"error": "invalid literal for int() with base 10: 'string'"}

        with patch.object(self.connector, "_AsyncModbusConnector__create_task", return_value=fake_task), \
                patch.object(self.connector, "_AsyncModbusConnector__wait_task_with_timeout",
                             return_value=(True, err_payload)):
            out = self.connector._AsyncModbusConnector__process_reserved_rpc_request(rpc_request)

        self.assertEqual(out, err_payload)
        self.gateway.send_rpc_reply.assert_called_once_with(
            self.slave.device_name, 103, {"result": err_payload}
        )

