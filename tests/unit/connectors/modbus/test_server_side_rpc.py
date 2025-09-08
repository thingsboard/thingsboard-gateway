from unittest.mock import MagicMock, patch, AsyncMock
from tests.unit.connectors.modbus.modbus_base_test import ModbusBaseTestCase
from thingsboard_gateway.connectors.modbus.entities.rpc_request import RPCRequest

LOGNAME = "Modbus test"


class TestModbusServerSideConnectorRpc(ModbusBaseTestCase):
    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.slave = self.connector._AsyncModbusConnector__slaves[0]
        self.slave.connect = AsyncMock(return_value=True)
        self.slave.read = AsyncMock(return_value=78)
        self.slave.write = AsyncMock(return_value=78)
        self.slave.downlink_converter = MagicMock()
        self.slave.downlink_converter.convert.return_value = [78]
        self.slave.uplink_converter = MagicMock()

    async def asyncTearDown(self):
        self.slave = None
        await super().asyncTearDown()

    async def test_execute_read_connector_rpc_success(self):
        content = {'id': '225', 'method': 'modbus_get',
                   'params': {'address': 2, 'connectorId': 'd83f1c95-9946-48c6-8979-ad471faf523e',
                              'functionCode': 3,
                              'objectsCount': 1, 'type': '16int'}}
        rpc_request = RPCRequest(content=content)

        fake_task = MagicMock()
        fake_task.done.return_value = True
        success_payload = {"result": 78}

        with patch.object(self.connector, "_AsyncModbusConnector__create_task", return_value=fake_task) as ct_mock, \
                patch.object(self.connector, "_AsyncModbusConnector__wait_task_with_timeout",
                             return_value=(True, success_payload)) as wait_mock, \
                self.assertLogs(LOGNAME, level="DEBUG"):
            out = self.connector._AsyncModbusConnector__process_connector_rpc_request(rpc_request)

        ct_mock.assert_called_once()
        func, args, kwargs = ct_mock.call_args.args
        device, passed_params, passed_rpc = args
        self.assertIs(device, self.slave)
        self.assertEqual(passed_params, rpc_request.params)
        self.assertIs(passed_rpc, rpc_request)
        self.assertEqual(kwargs, {"with_response": True})
        wait_mock.assert_called_once_with(task=fake_task, timeout=rpc_request.timeout, poll_interval=0.2)

        self.assertEqual(out, [{"result": 78, "device_name": self.slave.device_name}])

    async def test_execute_read_connector_rpc_invalid_required_rpc_fields(self):
        content = {'id': '225', 'method': 'modbus_get',
                   'params': {'address': 2, 'connectorId': 'd83f1c95-9946-48c6-8979-ad471faf523e', 'functionCode': 333,
                              'objectsCount': 1, 'type': '16int'}}
        rpc_request = RPCRequest(content=content)

        fake_task = MagicMock()
        fake_task.done.return_value = True
        error_payload = {"error": "Unsupported function code in RPC request."}

        with patch.object(self.connector, "_AsyncModbusConnector__create_task", return_value=fake_task) as ct_mock, \
                patch.object(self.connector, "_AsyncModbusConnector__wait_task_with_timeout",
                             return_value=(True, error_payload)) as wait_mock:
            out = self.connector._AsyncModbusConnector__process_connector_rpc_request(rpc_request)

        ct_mock.assert_called_once()
        wait_mock.assert_called_once_with(task=fake_task, timeout=rpc_request.timeout, poll_interval=0.2)
        self.assertEqual(out, [{"error": "Unsupported function code in RPC request.",
                                "device_name": self.slave.device_name}])

    async def test_execute_read_connector_rpc_fails_on_timeout(self):
        content = {'id': '225', 'method': 'modbus_get',
                   'params': {'address': 2, 'connectorId': 'd83f1c95-9946-48c6-8979-ad471faf523e',
                              'functionCode': 3,
                              'objectsCount': 1, 'type': '16int'}}
        rpc_request = RPCRequest(content=content)

        fake_task = MagicMock()
        fake_task.done.return_value = False

        with patch.object(self.connector, "_AsyncModbusConnector__create_task", return_value=fake_task) as ct_mock, \
                patch.object(self.connector, "_AsyncModbusConnector__wait_task_with_timeout",
                             return_value=(False, None)) as wait_mock, \
                self.assertLogs(LOGNAME, level="ERROR") as logcap:
            out = self.connector._AsyncModbusConnector__process_connector_rpc_request(rpc_request)

        ct_mock.assert_called_once()
        wait_mock.assert_called_once_with(task=fake_task, timeout=rpc_request.timeout, poll_interval=0.2)
        self.assertEqual(out, [{"error": f"Timeout rpc has been reached for {self.slave.device_name}"}])
        self.assertTrue(any("timeout has been reached" in m.lower() for m in logcap.output))

    async def test_execute_write_connector_rpc_success(self):
        content = {'id': '225', 'method': 'modbus_set',
                   'params': {'address': 2, 'connectorId': 'd83f1c95-9946-48c6-8979-ad471faf523e', 'functionCode': 6,
                              'objectsCount': 1, 'type': '16int', 'value': '78'}}
        rpc_request = RPCRequest(content=content)

        fake_task = MagicMock()
        fake_task.done.return_value = True
        success_payload = {"result": 78}

        with patch.object(self.connector, "_AsyncModbusConnector__create_task", return_value=fake_task) as ct_mock, \
                patch.object(self.connector, "_AsyncModbusConnector__wait_task_with_timeout",
                             return_value=(True, success_payload)) as wait_mock:
            out = self.connector._AsyncModbusConnector__process_connector_rpc_request(rpc_request)

        ct_mock.assert_called_once()
        wait_mock.assert_called_once_with(task=fake_task, timeout=rpc_request.timeout, poll_interval=0.2)
        self.assertEqual(out, [{"result": 78, "device_name": self.slave.device_name}])

    async def test_execute_write_connector_rpc_invalid_required_rpc_fields(self):
        content = {'id': '225', 'method': 'modbus_set',
                   'params': {'address': 2, 'connectorId': 'd83f1c95-9946-48c6-8979-ad471faf523e', 'functionCode': 333,
                              'objectsCount': 1, 'type': '16int', 'value': '78'}}
        rpc_request = RPCRequest(content=content)

        fake_task = MagicMock()
        fake_task.done.return_value = True
        error_payload = {"error": "Unsupported function code in RPC request."}

        with patch.object(self.connector, "_AsyncModbusConnector__create_task", return_value=fake_task) as ct_mock, \
                patch.object(self.connector, "_AsyncModbusConnector__wait_task_with_timeout",
                             return_value=(True, error_payload)) as wait_mock:
            out = self.connector._AsyncModbusConnector__process_connector_rpc_request(rpc_request)

        ct_mock.assert_called_once()
        wait_mock.assert_called_once_with(task=fake_task, timeout=rpc_request.timeout, poll_interval=0.2)
        self.assertEqual(out, [{"error": "Unsupported function code in RPC request.",
                                "device_name": self.slave.device_name}])

    async def test_execute_write_connector_rpc_fails_on_timeout(self):
        content = {'id': '225', 'method': 'modbus_set',
                   'params': {'address': 2, 'connectorId': 'd83f1c95-9946-48c6-8979-ad471faf523e', 'functionCode': 6,
                              'objectsCount': 1, 'type': '16int', 'value': '78'}}
        rpc_request = RPCRequest(content=content)

        fake_task = MagicMock()
        fake_task.done.return_value = False

        with patch.object(self.connector, "_AsyncModbusConnector__create_task", return_value=fake_task) as ct_mock, \
                patch.object(self.connector, "_AsyncModbusConnector__wait_task_with_timeout",
                             return_value=(False, None)) as wait_mock, \
                self.assertLogs(LOGNAME, level="ERROR") as logcap:
            out = self.connector._AsyncModbusConnector__process_connector_rpc_request(rpc_request)

        ct_mock.assert_called_once()
        wait_mock.assert_called_once_with(task=fake_task, timeout=rpc_request.timeout, poll_interval=0.2)
        self.assertEqual(out, [{"error": f"Timeout rpc has been reached for {self.slave.device_name}"}])
        self.assertTrue(any("timeout has been reached" in m.lower() for m in logcap.output))

    async def test_execute_write_connector_rpc_non_matching_datatype(self):
        content = {'id': '225', 'method': 'modbus_set',
                   'params': {'address': 2, 'connectorId': 'd83f1c95-9946-48c6-8979-ad471faf523e', 'functionCode': 6,
                              'objectsCount': 1, 'type': '16int', 'value': 'rrrree'}}
        rpc_request = RPCRequest(content=content)

        fake_task = MagicMock()
        fake_task.done.return_value = True
        error_payload = {"error": "invalid literal for int() with base 10: 'rrrree'"}

        with patch.object(self.connector, "_AsyncModbusConnector__create_task", return_value=fake_task) as ct_mock, \
                patch.object(self.connector, "_AsyncModbusConnector__wait_task_with_timeout",
                             return_value=(True, error_payload)) as wait_mock:
            out = self.connector._AsyncModbusConnector__process_connector_rpc_request(rpc_request)

        ct_mock.assert_called_once()
        wait_mock.assert_called_once_with(task=fake_task, timeout=rpc_request.timeout, poll_interval=0.2)
        self.assertEqual(out, [{"error": "invalid literal for int() with base 10: 'rrrree'",
                                "device_name": self.slave.device_name}])
