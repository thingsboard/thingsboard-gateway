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

from unittest.mock import MagicMock, patch
from tests.unit.connectors.modbus.modbus_base_test import ServerSideRPCModbusSetUp
from thingsboard_gateway.connectors.modbus.entities.rpc_request import RPCRequest

LOGNAME = "Modbus test"


class TestDeviceServerSideRPC(ServerSideRPCModbusSetUp):

    async def test_read_device_rpc_success(self):
        content = {'data': {'id': 115, 'method': 'getValue', 'params': None},
                   'device': self.slave.device_name, 'id': 115}
        rpc_request = RPCRequest(content=content)

        fake_task = MagicMock()
        expected = {"value": 78}

        with patch.object(self.connector, "_AsyncModbusConnector__create_task", return_value=fake_task) as ct_mock, \
                patch.object(self.connector, "_AsyncModbusConnector__wait_task_with_timeout",
                             return_value=(True, expected)) as wait_mock:
            out = self.connector._AsyncModbusConnector__process_device_rpc_request(rpc_request)
        ct_mock.assert_called_once()
        func, args, kwargs = ct_mock.call_args.args
        device, dev_cfg, passed_rpc = args
        assert device is self.slave
        assert passed_rpc is rpc_request
        assert kwargs == {}
        wait_mock.assert_called_once_with(task=fake_task, timeout=rpc_request.timeout, poll_interval=0.2)
        assert out == expected
        self.gateway.send_rpc_reply.assert_called_once_with(
            self.slave.device_name, 115, {"result": expected}
        )

    async def test_read_device_rpc_no_device_found(self):
        content = {'data': {'id': 115, 'method': 'getValue', 'params': None},
                   'device': 'Ghost Device', 'id': 115}
        rpc_request = RPCRequest(content=content)

        with patch.object(self.connector, "get_name", return_value="AsyncModbusConnector(TEST)") as name_mock, \
                patch.object(self.connector, "_AsyncModbusConnector__create_task") as ct_mock, \
                self.assertLogs(LOGNAME, level="ERROR") as logcap:
            self.connector._AsyncModbusConnector__process_device_rpc_request(rpc_request)

        name_mock.assert_called_once_with()
        self.gateway.send_rpc_reply.assert_called_once()
        _, kwargs = self.gateway.send_rpc_reply.call_args
        assert kwargs == {"device": "Ghost Device"}
        assert any(
            "Device Ghost Device not found in connector AsyncModbusConnector(TEST)" in m
            for m in logcap.output
        )
        ct_mock.assert_not_called()

    async def test_read_device_rpc_no_method_specified_inside_config(self):
        content = {'data': {'id': 117, 'method': 'unknownmethod', 'params': None},
                   'device': self.slave.device_name, 'id': 117}
        rpc_request = RPCRequest(content=content)

        with patch.object(self.connector, "get_name", return_value="AsyncModbusConnector(TEST)") as name_mock, \
                patch.object(self.connector, "_AsyncModbusConnector__create_task") as ct_mock, \
                self.assertLogs(LOGNAME, level="ERROR") as logcap:
            out = self.connector._AsyncModbusConnector__process_device_rpc_request(rpc_request)

        name_mock.assert_called_once_with()
        ct_mock.assert_not_called()
        self.gateway.send_rpc_reply.assert_called_once_with(
            self.slave.device_name, 117, {"result": {"error": "Method not found for unknownmethod"}}
        )
        assert out is None
        assert any("method unknownmethod not found in config" in m.lower() for m in logcap.output)

    async def test_read_device_rpc_fails_on_timeout(self):
        content = {'data': {'id': 115, 'method': 'getValue', 'params': None},
                   'device': self.slave.device_name, 'id': 115}
        rpc_request = RPCRequest(content=content)

        fake_task = MagicMock()

        with patch.object(self.connector, "_AsyncModbusConnector__create_task", return_value=fake_task), \
                patch.object(self.connector, "_AsyncModbusConnector__wait_task_with_timeout",
                             return_value=(False, None)), \
                self.assertLogs(LOGNAME, level="ERROR") as logcap:
            out = self.connector._AsyncModbusConnector__process_device_rpc_request(rpc_request)

        expected = {"error": f"Timeout rpc has been reached for {self.slave.device_name}"}
        assert out == expected
        self.gateway.send_rpc_reply.assert_called_once_with(
            self.slave.device_name, 115, {"result": expected}
        )
        assert any("timeout has been reached" in m.lower() for m in logcap.output)

    async def test_write_device_rpc_success(self):
        content = {'data': {'id': 118, 'method': 'setValue', 'params': 56},
                   'device': self.slave.device_name, 'id': 118}
        rpc_request = RPCRequest(content=content)

        fake_task = MagicMock()
        expected = {"value": 56}

        with patch.object(self.connector, "_AsyncModbusConnector__create_task", return_value=fake_task), \
                patch.object(self.connector, "_AsyncModbusConnector__wait_task_with_timeout",
                             return_value=(True, expected)):
            out = self.connector._AsyncModbusConnector__process_device_rpc_request(rpc_request)

        assert out == expected
        self.gateway.send_rpc_reply.assert_called_once_with(
            self.slave.device_name, 118, {"result": expected}
        )

    async def test_write_device_rpc_no_device_found(self):
        content = {'data': {'id': 115, 'method': 'setValue', 'params': 56},
                   'device': 'Ghost Device', 'id': 115}
        rpc_request = RPCRequest(content=content)

        with patch.object(self.connector, "get_name", return_value="AsyncModbusConnector(TEST)") as name_mock, \
                patch.object(self.connector, "_AsyncModbusConnector__create_task") as ct_mock, \
                self.assertLogs(LOGNAME, level="ERROR") as logcap:
            self.connector._AsyncModbusConnector__process_device_rpc_request(rpc_request)

        name_mock.assert_called_once_with()
        self.gateway.send_rpc_reply.assert_called_once()
        _, kwargs = self.gateway.send_rpc_reply.call_args
        assert kwargs == {"device": "Ghost Device"}
        assert any(
            "Device Ghost Device not found in connector AsyncModbusConnector(TEST)" in m
            for m in logcap.output
        )
        ct_mock.assert_not_called()

    async def test_write_device_rpc_no_method_specified_inside_config(self):
        content = {'data': {'id': 120, 'method': 'unknownmethod', 'params': 76},
                   'device': self.slave.device_name, 'id': 120}
        rpc_request = RPCRequest(content=content)

        with patch.object(self.connector, "get_name", return_value="AsyncModbusConnector(TEST)") as name_mock, \
                patch.object(self.connector, "_AsyncModbusConnector__create_task") as ct_mock, \
                self.assertLogs(LOGNAME, level="ERROR") as logcap:
            out = self.connector._AsyncModbusConnector__process_device_rpc_request(rpc_request)

        name_mock.assert_called_once_with()
        ct_mock.assert_not_called()
        self.gateway.send_rpc_reply.assert_called_once_with(
            self.slave.device_name, 120, {"result": {"error": "Method not found for unknownmethod"}}
        )
        assert out is None
        assert any("method unknownmethod not found in config" in m.lower() for m in logcap.output)

    async def test_write_device_rpc_fails_on_timeout(self):
        content = {'data': {'id': 118, 'method': 'setValue', 'params': 56},
                   'device': self.slave.device_name, 'id': 118}
        rpc_request = RPCRequest(content=content)

        fake_task = MagicMock()

        with patch.object(self.connector, "_AsyncModbusConnector__create_task", return_value=fake_task), \
                patch.object(self.connector, "_AsyncModbusConnector__wait_task_with_timeout",
                             return_value=(False, None)), \
                self.assertLogs(LOGNAME, level="ERROR") as logcap:
            out = self.connector._AsyncModbusConnector__process_device_rpc_request(rpc_request)

        expected = {"error": f"Timeout rpc has been reached for {self.slave.device_name}"}
        assert out == expected
        self.gateway.send_rpc_reply.assert_called_once_with(
            self.slave.device_name, 118, {"result": expected}
        )
        assert any("timeout has been reached" in m.lower() for m in logcap.output)

    async def test_write_device_rpc_incorrect_input_datatype(self):
        content = {'data': {'id': 121, 'method': 'setValue', 'params': 'string'},
                   'device': self.slave.device_name, 'id': 121}
        rpc_request = RPCRequest(content=content)

        fake_task = MagicMock()
        err = {"error": "invalid literal for int() with base 10: 'string'"}

        with patch.object(self.connector, "_AsyncModbusConnector__create_task", return_value=fake_task), \
                patch.object(self.connector, "_AsyncModbusConnector__wait_task_with_timeout",
                             return_value=(True, err)):
            out = self.connector._AsyncModbusConnector__process_device_rpc_request(rpc_request)

        assert out == err
        self.gateway.send_rpc_reply.assert_called_once_with(
            self.slave.device_name, 121, {"result": err}
        )

