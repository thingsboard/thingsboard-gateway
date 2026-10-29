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

from json import dumps
from unittest.mock import MagicMock

from tests.unit.BaseUnitTest import BaseUnitTest
from thingsboard_gateway.connectors.xmpp.xmpp_connector import XMPPConnector
from thingsboard_gateway.gateway.entities.rpc_request import RPCType, create_rpc_request_from_dict

DEVICE_NAME = 'XMPP RPC Device'
DEVICE_JID = 'device@localhost'


class TestProcessReservedRpc(BaseUnitTest):

    def setUp(self):
        super().setUp()
        self.connector = XMPPConnector.__new__(XMPPConnector)
        self.connector._XMPPConnector__log = MagicMock()
        self.connector._available_device = {DEVICE_NAME: DEVICE_JID}
        self.connector._send_message = MagicMock()

    @staticmethod
    def make_reserved_rpc_request(rpc_id, method_name, params):
        content = {'data': {'id': rpc_id, 'method': method_name, 'params': params}, 'device': DEVICE_NAME}
        rpc_request = create_rpc_request_from_dict(content)
        assert rpc_request.rpc_type == RPCType.RESERVED
        return rpc_request

    def test_set_reserved_rpc_params_is_plain_string_not_dict(self):
        # rpc_request.params is a raw string like "relay=true;" in the new RPC contract,
        # not the old {'data': {'params': ...}} dict - this must not raise AttributeError.
        rpc_request = self.make_reserved_rpc_request(1, 'set', 'relay=true;')

        response = self.connector._process_reserved_rpc(rpc_request, DEVICE_JID)

        self.connector._send_message.assert_called_once_with(DEVICE_JID, dumps({'relay': 'true'}))
        self.assertEqual(response.message, {'result': 'success'})

    def test_multiple_params_are_all_parsed(self):
        rpc_request = self.make_reserved_rpc_request(2, 'set', 'relay=true;timeout=10;')

        response = self.connector._process_reserved_rpc(rpc_request, DEVICE_JID)

        self.connector._send_message.assert_called_once_with(DEVICE_JID, dumps({'relay': 'true', 'timeout': '10'}))
        self.assertEqual(response.message, {'result': 'success'})

    def test_get_reserved_rpc_with_params(self):
        rpc_request = self.make_reserved_rpc_request(3, 'get', 'relay=true;')

        response = self.connector._process_reserved_rpc(rpc_request, DEVICE_JID)

        self.connector._send_message.assert_called_once_with(DEVICE_JID, dumps({'relay': 'true'}))
        self.assertEqual(response.message, {'result': 'success'})

    def test_empty_params_returns_error_without_sending_message(self):
        rpc_request = self.make_reserved_rpc_request(4, 'set', None)

        response = self.connector._process_reserved_rpc(rpc_request, DEVICE_JID)

        self.connector._send_message.assert_not_called()
        self.assertEqual(response.message, {'error': 'RPC params are empty, expected format: set value=<value>;'})

    def test_malformed_param_without_equals_sign_is_skipped(self):
        rpc_request = self.make_reserved_rpc_request(5, 'set', 'relay;timeout=10;')

        response = self.connector._process_reserved_rpc(rpc_request, DEVICE_JID)

        self.connector._send_message.assert_called_once_with(DEVICE_JID, dumps({'timeout': '10'}))
        self.assertEqual(response.message, {'result': 'success'})

    def test_server_side_rpc_handler_routes_reserved_rpc_end_to_end(self):
        rpc_request = self.make_reserved_rpc_request(6, 'set', 'relay=true;')

        response = self.connector.server_side_rpc_handler(rpc_request)

        self.connector._send_message.assert_called_once_with(DEVICE_JID, dumps({'relay': 'true'}))
        self.assertEqual(response.message, {'result': 'success'})
        self.assertEqual(response.device_name, DEVICE_NAME)
