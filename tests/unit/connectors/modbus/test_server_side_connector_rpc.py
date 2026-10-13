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
from thingsboard_gateway.gateway.entities.rpc_request import create_rpc_request_from_dict

LOGNAME = "Modbus test"


class TestModbusServerSideConnectorRpc(ServerSideRPCModbusSetUp):

    @staticmethod
    def make_connector_rpc_request(content):
        return create_rpc_request_from_dict(content, gateway_or_connector_req_id=content['id'])

    async def test_execute_read_connector_rpc_success(self):
        content = {'id': '225', 'method': 'modbus_get',
                   'params': {'address': 2, 'connectorId': 'd83f1c95-9946-48c6-8979-ad471faf523e',
                              'functionCode': 3,
                              'objectsCount': 1, 'type': '16int'}}
        rpc_request = self.make_connector_rpc_request(content)

        fake_task = MagicMock()
        fake_task.done.return_value = True
        success_payload = {"result": 78}

        with patch.object(self.connector, "_AsyncModbusConnector__create_task", return_value=fake_task) as ct_mock, \
                patch.object(self.connector, "_AsyncModbusConnector__wait_task_with_timeout",
                             return_value=(True, success_payload)) as wait_mock, \
                self.assertLogs(LOGNAME, level="DEBUG"):
            response = self.connector._AsyncModbusConnector__process_connector_rpc_request(rpc_request)

        ct_mock.assert_called_once()
        func, args, kwargs = ct_mock.call_args.args
        device, passed_params, passed_rpc = args
        self.assertIs(device, self.slave)
        self.assertEqual(passed_params, rpc_request.params)
        self.assertIs(passed_rpc, rpc_request)
        self.assertEqual(kwargs, {})
        wait_mock.assert_called_once_with(task=fake_task, timeout=rpc_request.timeout, poll_interval=0.2)

        self.assertEqual(response.message,
                         {"result": [{"result": success_payload, "device_name": self.slave.device_name}]})

    async def test_execute_read_connector_rpc_invalid_required_rpc_fields(self):
        content = {'id': '225', 'method': 'modbus_get',
                   'params': {'address': 2, 'connectorId': 'd83f1c95-9946-48c6-8979-ad471faf523e', 'functionCode': 333,
                              'objectsCount': 1, 'type': '16int'}}
        rpc_request = self.make_connector_rpc_request(content)

        fake_task = MagicMock()
        fake_task.done.return_value = True
        error_payload = {"error": "Unsupported function code in RPC request."}

        with patch.object(self.connector, "_AsyncModbusConnector__create_task", return_value=fake_task) as ct_mock, \
                patch.object(self.connector, "_AsyncModbusConnector__wait_task_with_timeout",
                             return_value=(True, error_payload)) as wait_mock:
            response = self.connector._AsyncModbusConnector__process_connector_rpc_request(rpc_request)

        ct_mock.assert_called_once()
        wait_mock.assert_called_once_with(task=fake_task, timeout=rpc_request.timeout, poll_interval=0.2)
        self.assertEqual(response.message,
                         {"result": [{"result": error_payload, "device_name": self.slave.device_name}]})

    async def test_execute_read_connector_rpc_fails_on_timeout(self):
        content = {'id': '225', 'method': 'modbus_get',
                   'params': {'address': 2, 'connectorId': 'd83f1c95-9946-48c6-8979-ad471faf523e',
                              'functionCode': 3,
                              'objectsCount': 1, 'type': '16int'}}
        rpc_request = self.make_connector_rpc_request(content)

        fake_task = MagicMock()
        fake_task.done.return_value = False

        with patch.object(self.connector, "_AsyncModbusConnector__create_task", return_value=fake_task) as ct_mock, \
                patch.object(self.connector, "_AsyncModbusConnector__wait_task_with_timeout",
                             return_value=(False, None)) as wait_mock, \
                self.assertLogs(LOGNAME, level="ERROR") as logcap:
            response = self.connector._AsyncModbusConnector__process_connector_rpc_request(rpc_request)

        ct_mock.assert_called_once()
        wait_mock.assert_called_once_with(task=fake_task, timeout=rpc_request.timeout, poll_interval=0.2)
        self.assertEqual(response.message,
                         {"result": [{"error": f"Timeout rpc has been reached for {self.slave.device_name}"}]})
        self.assertTrue(any("timeout has been reached" in m.lower() for m in logcap.output))

    async def test_execute_write_connector_rpc_success(self):
        content = {'id': '225', 'method': 'modbus_set',
                   'params': {'address': 2, 'connectorId': 'd83f1c95-9946-48c6-8979-ad471faf523e', 'functionCode': 6,
                              'objectsCount': 1, 'type': '16int', 'value': '78'}}
        rpc_request = self.make_connector_rpc_request(content)

        fake_task = MagicMock()
        fake_task.done.return_value = True
        success_payload = {"result": 78}

        with patch.object(self.connector, "_AsyncModbusConnector__create_task", return_value=fake_task) as ct_mock, \
                patch.object(self.connector, "_AsyncModbusConnector__wait_task_with_timeout",
                             return_value=(True, success_payload)) as wait_mock:
            response = self.connector._AsyncModbusConnector__process_connector_rpc_request(rpc_request)

        ct_mock.assert_called_once()
        wait_mock.assert_called_once_with(task=fake_task, timeout=rpc_request.timeout, poll_interval=0.2)
        self.assertEqual(response.message,
                         {"result": [{"result": success_payload, "device_name": self.slave.device_name}]})

    async def test_execute_write_connector_rpc_invalid_required_rpc_fields(self):
        content = {'id': '225', 'method': 'modbus_set',
                   'params': {'address': 2, 'connectorId': 'd83f1c95-9946-48c6-8979-ad471faf523e', 'functionCode': 333,
                              'objectsCount': 1, 'type': '16int', 'value': '78'}}
        rpc_request = self.make_connector_rpc_request(content)

        fake_task = MagicMock()
        fake_task.done.return_value = True
        error_payload = {"error": "Unsupported function code in RPC request."}

        with patch.object(self.connector, "_AsyncModbusConnector__create_task", return_value=fake_task) as ct_mock, \
                patch.object(self.connector, "_AsyncModbusConnector__wait_task_with_timeout",
                             return_value=(True, error_payload)) as wait_mock:
            response = self.connector._AsyncModbusConnector__process_connector_rpc_request(rpc_request)

        ct_mock.assert_called_once()
        wait_mock.assert_called_once_with(task=fake_task, timeout=rpc_request.timeout, poll_interval=0.2)
        self.assertEqual(response.message,
                         {"result": [{"result": error_payload, "device_name": self.slave.device_name}]})

    async def test_execute_write_connector_rpc_fails_on_timeout(self):
        content = {'id': '225', 'method': 'modbus_set',
                   'params': {'address': 2, 'connectorId': 'd83f1c95-9946-48c6-8979-ad471faf523e', 'functionCode': 6,
                              'objectsCount': 1, 'type': '16int', 'value': '78'}}
        rpc_request = self.make_connector_rpc_request(content)

        fake_task = MagicMock()
        fake_task.done.return_value = False

        with patch.object(self.connector, "_AsyncModbusConnector__create_task", return_value=fake_task) as ct_mock, \
                patch.object(self.connector, "_AsyncModbusConnector__wait_task_with_timeout",
                             return_value=(False, None)) as wait_mock, \
                self.assertLogs(LOGNAME, level="ERROR") as logcap:
            response = self.connector._AsyncModbusConnector__process_connector_rpc_request(rpc_request)

        ct_mock.assert_called_once()
        wait_mock.assert_called_once_with(task=fake_task, timeout=rpc_request.timeout, poll_interval=0.2)
        self.assertEqual(response.message,
                         {"result": [{"error": f"Timeout rpc has been reached for {self.slave.device_name}"}]})
        self.assertTrue(any("timeout has been reached" in m.lower() for m in logcap.output))

    async def test_execute_write_connector_rpc_non_matching_datatype(self):
        content = {'id': '225', 'method': 'modbus_set',
                   'params': {'address': 2, 'connectorId': 'd83f1c95-9946-48c6-8979-ad471faf523e', 'functionCode': 6,
                              'objectsCount': 1, 'type': '16int', 'value': 'rrrree'}}
        rpc_request = self.make_connector_rpc_request(content)

        fake_task = MagicMock()
        fake_task.done.return_value = True
        error_payload = {"error": "invalid literal for int() with base 10: 'rrrree'"}

        with patch.object(self.connector, "_AsyncModbusConnector__create_task", return_value=fake_task) as ct_mock, \
                patch.object(self.connector, "_AsyncModbusConnector__wait_task_with_timeout",
                             return_value=(True, error_payload)) as wait_mock:
            response = self.connector._AsyncModbusConnector__process_connector_rpc_request(rpc_request)

        ct_mock.assert_called_once()
        wait_mock.assert_called_once_with(task=fake_task, timeout=rpc_request.timeout, poll_interval=0.2)
        self.assertEqual(response.message,
                         {"result": [{"result": error_payload, "device_name": self.slave.device_name}]})
