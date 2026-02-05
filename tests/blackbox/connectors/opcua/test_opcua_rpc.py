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

from time import sleep
from tests.blackbox.connectors.opcua.test_base_opcua import BaseOpcuaTest


class OpcuaReadRpc(BaseOpcuaTest):

    def setUp(self):
        super(OpcuaReadRpc, self).setUp()
        sleep(self.GENERAL_TIMEOUT * 2)

    def test_full_path_reading_rpc(self):
        (config, _) = self.change_connector_configuration(
            self.CONFIG_PATH + 'configs/rpc_configs/opcua_rpc_absolute_path.json')
        sleep(self.GENERAL_TIMEOUT)
        telemetry_keys = [
            key['key']
            for conf_list in config[self.CONNECTOR_NAME]['configurationJson']['mapping']
            for key in conf_list['timeseries']
        ]
        expected_values = self.load_configuration(
            self.CONFIG_PATH + 'test_values/rpc/opcua_rpc_full_path_default_values.json'
        )
        request_payload = {'method': 'get', 'params': 'Root\\.Objects\\.TempSensor\\.Pressure'}
        result = self.client.handle_two_way_device_rpc_request(self.device.id,
                                                               {
                                                                   "method": request_payload['method'],
                                                                   "params": request_payload['params'],
                                                                   "timeout": 5000
                                                               })
        for telemetry_key in telemetry_keys:
            self.assertEqual(result, {'result': {'value': expected_values[telemetry_key]}},
                             f'Value is not equal for the full path and the following telemetry key: {telemetry_key}')

    def test_relative_path_reading_rpc(self):
        (config, _) = self.change_connector_configuration(
            self.CONFIG_PATH + 'configs/rpc_configs/opcua_rpc_relative_path.json')
        sleep(self.GENERAL_TIMEOUT)
        telemetry_keys = [
            key['key']
            for conf_list in config[self.CONNECTOR_NAME]['configurationJson']['mapping']
            for key in conf_list['timeseries']
        ]
        expected_values = self.load_configuration(
            self.CONFIG_PATH + 'test_values/rpc/opcua_rpc_relative_path_default_values.json'
        )
        request_payload = {'method': 'get', 'params': 'Pressure'}
        result = self.client.handle_two_way_device_rpc_request(self.device.id,
                                                               {
                                                                   "method": request_payload['method'],
                                                                   "params": request_payload['params'],
                                                                   "timeout": 5000
                                                               })
        for telemetry_key in telemetry_keys:
            self.assertEqual(result, {'result': {'value': expected_values[telemetry_key]}},
                             f'Value is not equal for the relative path and the following telemetry key: {telemetry_key}')

    def test_foreign_node_reading_rpc(self):
        (config, _) = self.change_connector_configuration(
            self.CONFIG_PATH + 'configs/rpc_configs/opcua_rpc_foreign_node.json')
        sleep(self.GENERAL_TIMEOUT)
        telemetry_keys = [
            key['key']
            for conf_list in config[self.CONNECTOR_NAME]['configurationJson']['mapping']
            for key in conf_list['timeseries']
        ]
        expected_values = self.load_configuration(
            self.CONFIG_PATH + 'test_values/rpc/opcua_rpc_foreign_path_default_values.json'
        )
        request_payload = {'method': 'get', 'params': 'Root\\.Objects\\.TempSensor_G\\.Humidity'}
        result = self.client.handle_two_way_device_rpc_request(self.device.id,
                                                               {
                                                                   "method": request_payload['method'],
                                                                   "params": request_payload['params'],
                                                                   "timeout": 5000
                                                               })
        for telemetry_key in telemetry_keys:
            self.assertEqual(result, {'result': {'value': expected_values[telemetry_key]}},
                             f'Value is not equal for the full path and the following telemetry key: {telemetry_key}')

    def test_identifier_reading_rpc(self):
        (config, _) = self.change_connector_configuration(
            self.CONFIG_PATH + 'configs/rpc_configs/opcua_rpc_identifier.json')
        sleep(self.GENERAL_TIMEOUT)
        telemetry_keys = [
            key['key']
            for conf_list in config[self.CONNECTOR_NAME]['configurationJson']['mapping']
            for key in conf_list['timeseries']
        ]
        expected_values = self.load_configuration(
            self.CONFIG_PATH + 'test_values/rpc/opcua_rpc_identifier_default_values.json'
        )
        request_payload = {'method': 'get', 'params': 'ns=3;i=9'}
        result = self.client.handle_two_way_device_rpc_request(self.device.id,
                                                               {
                                                                   "method": request_payload['method'],
                                                                   "params": request_payload['params'],
                                                                   "timeout": 5000
                                                               })
        for telemetry_key in telemetry_keys:
            self.assertEqual(result, {'result': {'value': expected_values[telemetry_key]}},
                             f'Value is not equal for the identifier and the following telemetry key: {telemetry_key}')

    def test_multipy_reading_rpc(self):
        (config, _) = self.change_connector_configuration(
            self.CONFIG_PATH + 'configs/rpc_configs/opcua_rpc_multiply.json')
        sleep(self.GENERAL_TIMEOUT)
        request_payload = {'method': 'multiply', 'params': [7, 8]}
        result = self.client.handle_two_way_device_rpc_request(self.device.id,
                                                               {
                                                                   "method": request_payload['method'],
                                                                   "params": request_payload['params'],
                                                                   "timeout": 5000
                                                               })
        self.assertEqual(result, {'result': {'result': 56}},
                         f'Value is not equal for the multiply method')


class OpcuaWriteRPC(BaseOpcuaTest):

    def setUp(self):
        super(OpcuaWriteRPC, self).setUp()
        sleep(self.GENERAL_TIMEOUT * 2)

    def test_full_path_writing_rpc(self):
        (config, _) = self.change_connector_configuration(
            self.CONFIG_PATH + 'configs/rpc_configs/opcua_rpc_absolute_path.json')
        sleep(self.GENERAL_TIMEOUT)
        telemetry_keys = [
            key['key']
            for conf_list in config[self.CONNECTOR_NAME]['configurationJson']['mapping']
            for key in conf_list['timeseries']
        ]
        expected_values = self.load_configuration(
            self.CONFIG_PATH + 'test_values/rpc/opcua_rpc_full_path_values.json'
        )
        request_payload = {'method': 'set', 'params': 'Root\\.Objects\\.TempSensor\\.Pressure; 90.25'}
        result = self.client.handle_two_way_device_rpc_request(self.device.id,
                                                               {
                                                                   "method": request_payload['method'],
                                                                   "params": request_payload['params'],
                                                                   "timeout": 5000
                                                               })
        for telemetry_key in telemetry_keys:
            self.assertEqual(result, {'result': {'value': expected_values[telemetry_key]}},
                             f'Value is not equal for the identifier and the following telemetry key: {telemetry_key}')

    def test_foreign_path_write_rpc(self):
        (config, _) = self.change_connector_configuration(
            self.CONFIG_PATH + 'configs/rpc_configs/opcua_rpc_foreign_node.json')
        sleep(self.GENERAL_TIMEOUT)
        telemetry_keys = [
            key['key']
            for conf_list in config[self.CONNECTOR_NAME]['configurationJson']['mapping']
            for key in conf_list['timeseries']
        ]
        expected_values = self.load_configuration(
            self.CONFIG_PATH + 'test_values/rpc/opcua_rpc_foreign_path_values.json'
        )
        request_payload = {'method': 'set', 'params': 'Root\\.Objects\\.TempSensor_G\\.Humidity; 40.5'}
        result = self.client.handle_two_way_device_rpc_request(self.device.id,
                                                               {
                                                                   "method": request_payload['method'],
                                                                   "params": request_payload['params'],
                                                                   "timeout": 5000
                                                               })
        for telemetry_key in telemetry_keys:
            self.assertEqual(result, {'result': {'value': expected_values[telemetry_key]}},
                             f'Value is not equal for the full path and the following telemetry key: {telemetry_key}')

    def test_relative_path_writing_rpc(self):
        (config, _) = self.change_connector_configuration(
            self.CONFIG_PATH + 'configs/rpc_configs/opcua_rpc_relative_path.json')
        sleep(self.GENERAL_TIMEOUT)
        telemetry_keys = [
            key['key']
            for conf_list in config[self.CONNECTOR_NAME]['configurationJson']['mapping']
            for key in conf_list['timeseries']
        ]
        expected_values = self.load_configuration(
            self.CONFIG_PATH + 'test_values/rpc/opcua_rpc_relative_path_values.json'
        )
        request_payload = {'method': 'set', 'params': 'Pressure; 90.25'}
        result = self.client.handle_two_way_device_rpc_request(self.device.id,
                                                               {
                                                                   "method": request_payload['method'],
                                                                   "params": request_payload['params'],
                                                                   "timeout": 5000
                                                               })
        for telemetry_key in telemetry_keys:
            self.assertEqual(result, {'result': {'value': expected_values[telemetry_key]}},
                             f'Value is not equal for the relative path and the following telemetry key: {telemetry_key}')

    def test_identifier_writing_rpc(self):
        (config, _) = self.change_connector_configuration(
            self.CONFIG_PATH + 'configs/rpc_configs/opcua_rpc_identifier.json')
        sleep(self.GENERAL_TIMEOUT)
        telemetry_keys = [
            key['key']
            for conf_list in config[self.CONNECTOR_NAME]['configurationJson']['mapping']
            for key in conf_list['timeseries']
        ]
        expected_values = self.load_configuration(
            self.CONFIG_PATH + 'test_values/rpc/opcua_rpc_identifier_values.json'
        )
        request_payload = {'method': 'set', 'params': 'ns=3;i=9; 456.34'}
        result = self.client.handle_two_way_device_rpc_request(self.device.id,
                                                               {
                                                                   "method": request_payload['method'],
                                                                   "params": request_payload['params'],
                                                                   "timeout": 5000
                                                               })
        for telemetry_key in telemetry_keys:
            self.assertEqual(result, {'result': {'value': expected_values[telemetry_key]}},
                             f'Value is not equal for the relative path and the following telemetry key: {telemetry_key}')

