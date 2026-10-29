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

from unittest.mock import MagicMock

from tests.unit.BaseUnitTest import BaseUnitTest
from thingsboard_gateway.connectors.xmpp.xmpp_connector import XMPPConnector
from thingsboard_gateway.connectors.xmpp.device import Device
from thingsboard_gateway.gateway.entities.rpc_request import create_rpc_request_from_dict

DEVICE_NAME = 'XMPP RPC Device'
DEVICE_JID = 'device@localhost'


class TestProcessRpcToDevice(BaseUnitTest):

    def setUp(self):
        super().setUp()
        self.connector = XMPPConnector.__new__(XMPPConnector)
        self.connector._XMPPConnector__log = MagicMock()
        self.connector._devices = {
            DEVICE_JID: Device(
                jid=DEVICE_JID,
                device_name_expression='',
                device_type_expression='',
                attributes=[],
                timeseries=[],
                attribute_updates=[],
                server_side_rpc=[
                    {'methodRPC': 'setRelay', 'valueExpression': '{"relay": ${params}}'},
                    {'methodRPC': 'setTemperature', 'valueExpression': '{"temperature": ${params}}'},
                    {'methodRPC': 'setState', 'valueExpression': '{"state": ${params}}'},
                ]
            )
        }
        self.connector._send_message = MagicMock()

    @staticmethod
    def make_device_rpc_request(rpc_id, method_name, params):
        content = {'data': {'id': rpc_id, 'method': method_name, 'params': params}, 'device': DEVICE_NAME}
        return create_rpc_request_from_dict(content)

    def test_boolean_param_is_resolved_and_sent_as_json_boolean(self):
        rpc_request = self.make_device_rpc_request(1, 'setRelay', True)

        response = self.connector._process_rpc_to_device(rpc_request, DEVICE_JID)

        self.connector._send_message.assert_called_once_with(DEVICE_JID, '{"relay": true}')
        self.assertEqual(response.message, {'result': 'success'})

    def test_numeric_param_is_resolved_and_sent_as_json_number(self):
        rpc_request = self.make_device_rpc_request(2, 'setTemperature', 25)

        response = self.connector._process_rpc_to_device(rpc_request, DEVICE_JID)

        self.connector._send_message.assert_called_once_with(DEVICE_JID, '{"temperature": 25}')
        self.assertEqual(response.message, {'result': 'success'})

    def test_string_param_is_resolved_and_sent_as_json_string(self):
        rpc_request = self.make_device_rpc_request(3, 'setState', 'active')

        response = self.connector._process_rpc_to_device(rpc_request, DEVICE_JID)

        self.connector._send_message.assert_called_once_with(DEVICE_JID, '{"state": "active"}')
        self.assertEqual(response.message, {'result': 'success'})

    def test_unknown_method_returns_error_without_sending_message(self):
        rpc_request = self.make_device_rpc_request(4, 'unknownMethod', True)

        response = self.connector._process_rpc_to_device(rpc_request, DEVICE_JID)

        self.connector._send_message.assert_not_called()
        self.assertEqual(response.message, {'error': 'Config for unknownMethod not found'})
