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
from thingsboard_gateway.connectors.modbus.constants import GET_RPC_EXPECTED_SCHEMA, SET_RPC_EXPECTED_SCHEMA
from thingsboard_gateway.gateway.entities.rpc_request import create_rpc_request_from_dict

LOGNAME = "Modbus test"


class TestReservedModbusRPC(ServerSideRPCModbusSetUp):

    async def test_get_reserved_modbus_rpc_success(self):
        content = {'data': {'id': 96, 'method': 'get', 'params': 'type=16int;functionCode=3;objectsCount=1;address=2;'},
                   'device': 'Demo Device', 'id': 96}
        rpc_request = create_rpc_request_from_dict(content)

        fake_task = MagicMock()
        fake_task.done.return_value = True
        expected_result = {"value": 78}

        with patch.object(self.connector, "_AsyncModbusConnector__create_task", return_value=fake_task) as ct_mock, \
                patch.object(self.connector, "_AsyncModbusConnector__wait_task_with_timeout",
                             return_value=(True, expected_result)) as wait_mock:
            response = self.connector._AsyncModbusConnector__process_reserved_rpc_request(rpc_request)

        ct_mock.assert_called_once()
        func, args, kwargs = ct_mock.call_args.args
        device, passed_params, passed_rpc = args
        self.assertIs(device, self.slave)
        self.assertEqual(passed_params, {'type': '16int', 'functionCode': 3, 'objectsCount': 1, 'address': 2})
        self.assertIs(passed_rpc, rpc_request)
        self.assertEqual(kwargs, {})

        wait_mock.assert_called_once_with(task=fake_task, timeout=rpc_request.timeout, poll_interval=0.2)
        self.assertEqual(response.device_name, self.slave.device_name)
        self.assertEqual(response.id, 96)
        self.assertEqual(response.message, {"result": expected_result})

    async def test_get_reserved_modbus_rpc_incorrect_request_schema(self):
        content = {
            'data': {'id': 97, 'method': 'get',
                     'params': 'type=16int;functionCode=333;objectsCount=1;address=2;'},
            'device': self.slave.device_name, 'id': 97
        }
        rpc_request = create_rpc_request_from_dict(content)

        with patch.object(self.connector, "_AsyncModbusConnector__create_task") as ct_mock:
            response = self.connector._AsyncModbusConnector__process_reserved_rpc_request(rpc_request)

        ct_mock.assert_not_called()
        expected_msg = (f'The requested RPC either does not match with the schema {GET_RPC_EXPECTED_SCHEMA} '
                       'or incorrect value/values provided')
        self.assertEqual(response.message, {"error": expected_msg})

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
        rpc_request = create_rpc_request_from_dict(content)

        with patch.object(self.connector, "get_name", return_value="AsyncModbusConnector(TEST)") as name_mock, \
                patch.object(self.connector, "_AsyncModbusConnector__create_task") as ct_mock, \
                self.assertLogs(LOGNAME, level="ERROR") as logcap:
            response = self.connector._AsyncModbusConnector__process_reserved_rpc_request(rpc_request)
        name_mock.assert_called_once_with()
        self.assertEqual(response.device_name, "Ghost Device")
        self.assertEqual(
            response.message,
            {"error": "Device Ghost Device not found in connector AsyncModbusConnector(TEST)"}
        )
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
        rpc_request = create_rpc_request_from_dict(content)

        fake_task = MagicMock()

        with patch.object(self.connector, "_AsyncModbusConnector__create_task", return_value=fake_task), \
                patch.object(self.connector, "_AsyncModbusConnector__wait_task_with_timeout",
                             return_value=(False, None)):
            response = self.connector._AsyncModbusConnector__process_reserved_rpc_request(rpc_request)

        expected_msg = f'Failed to process reserved rpc request for {self.slave.device_name}, timeout has been reached'
        self.assertEqual(response.message, {"error": expected_msg})

    async def test_set_reserved_modbus_rpc_success(self):
        content = {
            'data': {'id': 99, 'method': 'set',
                     'params': 'type=16int;functionCode=6;objectsCount=1;address=2;value=77;'},
            'device': self.slave.device_name, 'id': 99
        }
        rpc_request = create_rpc_request_from_dict(content)

        fake_task = MagicMock()
        expected_result = {"value": 77}

        with patch.object(self.connector, "_AsyncModbusConnector__create_task", return_value=fake_task) as ct_mock, \
                patch.object(self.connector, "_AsyncModbusConnector__wait_task_with_timeout",
                             return_value=(True, expected_result)):
            response = self.connector._AsyncModbusConnector__process_reserved_rpc_request(rpc_request)

        func, args, kwargs = ct_mock.call_args.args
        device, passed_params, passed_rpc = args
        self.assertEqual(passed_params,
                         {'type': '16int', 'functionCode': 6, 'objectsCount': 1, 'address': 2, 'value': '77'})

        self.assertEqual(response.message, {"result": expected_result})

    async def test_set_reserved_modbus_rpc_incorrect_request_schema(self):
        content = {
            'data': {'id': 100, 'method': 'set',
                     'params': 'type=16int;functionCode=444;objectsCount=1;address=2;value=77;'},
            'device': self.slave.device_name, 'id': 100
        }
        rpc_request = create_rpc_request_from_dict(content)

        with patch.object(self.connector, "_AsyncModbusConnector__create_task") as ct_mock:
            response = self.connector._AsyncModbusConnector__process_reserved_rpc_request(rpc_request)

        ct_mock.assert_not_called()
        expected_msg = (f'The requested RPC either does not match with the schema {SET_RPC_EXPECTED_SCHEMA} '
                       'or incorrect value/values provided')
        self.assertEqual(response.message, {"error": expected_msg})

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
        rpc_request = create_rpc_request_from_dict(content)

        with patch.object(self.connector, "get_name", return_value="AsyncModbusConnector(TEST)") as name_mock, \
                patch.object(self.connector, "_AsyncModbusConnector__create_task") as ct_mock, \
                self.assertLogs(LOGNAME, level="ERROR") as logcap:
            response = self.connector._AsyncModbusConnector__process_reserved_rpc_request(rpc_request)

        name_mock.assert_called_once_with()

        self.assertEqual(response.device_name, "Ghost Device")
        self.assertEqual(
            response.message,
            {"error": "Device Ghost Device not found in connector AsyncModbusConnector(TEST)"}
        )

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
        rpc_request = create_rpc_request_from_dict(content)

        fake_task = MagicMock()

        with patch.object(self.connector, "_AsyncModbusConnector__create_task", return_value=fake_task), \
                patch.object(self.connector, "_AsyncModbusConnector__wait_task_with_timeout",
                             return_value=(False, None)):
            response = self.connector._AsyncModbusConnector__process_reserved_rpc_request(rpc_request)

        expected_msg = f'Failed to process reserved rpc request for {self.slave.device_name}, timeout has been reached'
        self.assertEqual(response.message, {"error": expected_msg})

    async def test_set_reserved_modbus_rpc_invalid_data_type(self):
        content = {
            'data': {'id': 103, 'method': 'set',
                     'params': 'type=16int;functionCode=6;objectsCount=1;address=2;value=string;'},
            'device': self.slave.device_name, 'id': 103
        }
        rpc_request = create_rpc_request_from_dict(content)

        fake_task = MagicMock()
        err_payload = {"error": "invalid literal for int() with base 10: 'string'"}

        with patch.object(self.connector, "_AsyncModbusConnector__create_task", return_value=fake_task), \
                patch.object(self.connector, "_AsyncModbusConnector__wait_task_with_timeout",
                             return_value=(True, err_payload)):
            response = self.connector._AsyncModbusConnector__process_reserved_rpc_request(rpc_request)

        self.assertEqual(response.message, {"result": err_payload})
