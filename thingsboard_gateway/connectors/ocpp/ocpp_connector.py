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

import asyncio
import base64
import re
import ssl
from concurrent.futures import TimeoutError as FutureTimeoutError
from dataclasses import asdict, is_dataclass
from queue import Queue
from threading import Thread
from random import choice
from string import ascii_lowercase
from time import sleep

from simplejson import dumps, loads

from thingsboard_gateway.connectors.connector import Connector
from thingsboard_gateway.connectors.ocpp.constants import (
    RESERVED_GET_RPC_SCHEMA,
    RESERVED_SET_RPC_SCHEMA,
    RESERVED_GET_RPC_PATTERN,
    RESERVED_SET_RPC_PATTERN
)
from thingsboard_gateway.gateway.constants import RPC_DEFAULT_TIMEOUT
from thingsboard_gateway.gateway.entities.converted_data import ConvertedData
from thingsboard_gateway.gateway.entities.rpc_request import RPCType
from thingsboard_gateway.gateway.entities.rpc_response import RPCResponse
from thingsboard_gateway.gateway.statistics.decorators import CollectAllReceivedBytesStatistics
from thingsboard_gateway.gateway.statistics.statistics_service import StatisticsService
from thingsboard_gateway.tb_utility.tb_utility import TBUtility
from thingsboard_gateway.tb_utility.tb_logger import init_logger

try:
    import ocpp
except ImportError:
    print('OCPP library not found - installing...')
    TBUtility.install_package("ocpp", "2.1.0")
    import ocpp

try:
    import websockets
except ImportError:
    print('websockets library not found - installing...')
    TBUtility.install_package("websockets")
    import websockets

from ocpp.exceptions import OCPPError
from ocpp.v21 import call
from thingsboard_gateway.connectors.ocpp.charge_point import ChargePoint


class NotAuthorized(Exception):
    """Charge Point not authorized"""


class OcppConnector(Connector, Thread):
    DATA_TO_CONVERT = Queue(-1)
    DATA_TO_SEND = Queue(-1)

    def __init__(self, gateway, config, connector_type):
        super().__init__()
        self._config = config
        self.__id = self._config.get('id')
        self._central_system_config = config['centralSystem']
        self._charge_points_config = config.get('chargePoints', [])
        self._connector_type = connector_type
        self.statistics = {'MessagesReceived': 0,
                           'MessagesSent': 0}
        self._gateway = gateway
        self.name = self._config.get("name", 'OCPP Connector ' + ''.join(choice(ascii_lowercase) for _ in range(5)))
        self._log = init_logger(self._gateway, self.name, self._config.get('logLevel', 'INFO'),
                                enable_remote_logging=self._config.get('enableRemoteLogging', False),
                                is_connector_logger=True)
        self._converter_log = init_logger(self._gateway, self.name + '_converter',
                                          self._config.get('logLevel', 'INFO'),
                                          enable_remote_logging=self._config.get('enableRemoteLogging', False),
                                          is_converter_logger=True, attr_name=self.name)

        self._default_converters = {'uplink': 'OcppUplinkConverter'}
        self._server = None
        self._connected_charge_points = []

        self._ssl_context = None
        try:
            if self._central_system_config['connection']['type'].lower() == 'tls':
                self._ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
                pem_file = self._central_system_config['connection']['cert']
                key_file = self._central_system_config['connection']['key']
                password = self._central_system_config['connection'].get('password')
                self._ssl_context.load_cert_chain(pem_file, key_file, password=password)
        except Exception as e:
            self._log.exception(e)
            self._log.warning('TLS connection not set!')
            self._ssl_context = None

        self._data_convert_thread = Thread(name='Convert Data Thread', daemon=True, target=self._process_data)
        self._data_send_thread = Thread(name='Send Data Thread', daemon=True, target=self._send_data)

        self.__loop = asyncio.new_event_loop()

        self.__connected = False
        self.__stopped = False
        self.daemon = True

    def open(self):
        self.__stopped = False
        self.start()
        self._log.info("Starting OCPP Connector")

    def get_type(self):
        return self._connector_type

    def run(self):
        self._data_convert_thread.start()
        self._data_send_thread.start()

        self.__loop.create_task(self.start_server())
        self.__loop.run_forever()

    async def start_server(self):
        host = self._central_system_config.get('host', '0.0.0.0')
        port = self._central_system_config.get('port', 9000)
        self._server = await websockets.serve(self.on_connect, host, port, subprotocols=['ocpp2.1'],
                                              ssl=self._ssl_context)
        self.__connected = True
        self._log.info('Central System is running on %s:%d', host, port)

        await self._server.wait_closed()

    def _auth(self, websocket):
        for sec in self._central_system_config['security']:
            if sec['type'].lower() == 'token' and websocket.request.headers['authorization'] in sec['tokens']:
                self._log.debug('Got Authorization: %s', websocket.request.headers['authorization'])
                return
            elif sec['type'].lower() == 'basic':
                for cred in sec['credentials']:
                    token = 'Basic {0}'.format(
                        base64.b64encode(bytes(cred['username'] + ':' + cred['password'], 'utf-8')).decode('ascii'))
                    if websocket.request.headers['authorization'] == token:
                        self._log.debug('Got Authorization: %s', websocket.request.headers['authorization'])
                        return

        raise NotAuthorized('Charge Point not authorized')

    async def on_connect(self, websocket):
        """ For every new charge point that connects, create a ChargePoint instance
        and start listening for messages.

        """
        path = websocket.request.path

        requested_protocols = None
        try:
            requested_protocols = websocket.request.headers[
                'Sec-WebSocket-Protocol']
        except KeyError:
            self._log.info("Client hasn't requested any Subprotocol. "
                           "Closing Connection")

        # Authorize Charge Point before accept connection
        if self._central_system_config.get('security'):
            try:
                self._auth(websocket)
            except NotAuthorized as e:
                self._log.error(e)
                return await websocket.close()

        if websocket.subprotocol:
            self._log.info("Protocols Matched: %s", websocket.subprotocol)
        else:
            # In the websockets lib if no subprotocols are supported by the
            # client and the server, it proceeds without a subprotocol,
            # so we have to manually close the connection.
            self._log.warning('Protocols Mismatched | Expected Subprotocols: %s,'
                              ' but client supports  %s | Closing connection',
                              websocket.protocol.available_subprotocols,
                              requested_protocols)
            return await websocket.close()

        cp_host = None
        cp_port = None
        try:
            (cp_host, cp_port) = websocket.remote_address
        except ValueError:
            pass

        charge_point_id = path.strip('/')

        # check if charge point can be connected
        (is_valid, cp_config) = await self._is_charge_point_valid(charge_point_id, host=cp_host, port=cp_port)
        if is_valid:
            uplink_converter_name = cp_config.get('extension', self._default_converters['uplink'])
            cp = ChargePoint(charge_point_id, websocket, {**cp_config, 'uplink_converter_name': uplink_converter_name},
                             OcppConnector._callback, self._converter_log)
            cp.authorized = True

            self._log.info('Connected Charge Point with id: %s', charge_point_id)
            self._connected_charge_points.append(cp)

            try:
                await cp.start()
            except websockets.ConnectionClosed:
                self._connected_charge_points.pop(self._connected_charge_points.index(cp))

    async def _is_charge_point_valid(self, charge_point_id, **kwargs):
        for cp_config in self._charge_points_config:
            if re.match(cp_config['idRegexpPattern'], charge_point_id):
                host = cp_config.get('host')
                port = cp_config.get('port')
                if host or port:
                    if not re.match(host, kwargs.get('host')) or not re.match(port, kwargs.get('port')):
                        return False, None

                return True, cp_config

        return False, None

    def close(self):
        self.__stopped = True
        self.__connected = False

        tasks = asyncio.all_tasks(self.__loop)
        for task in tasks:
            task.cancel()

        for socket in self._server.server.sockets:
            socket._sock.close()

        self.__loop.stop()

        self._log.info('%s has been stopped.', self.get_name())
        self._log.stop()

    def get_id(self):
        return self.__id

    def get_name(self):
        return self.name

    def is_connected(self):
        return self.__connected

    def is_stopped(self):
        return self.__stopped

    @classmethod
    def _callback(cls, data):
        cls.DATA_TO_CONVERT.put(data)

    def _process_data(self):
        while not self.__stopped:
            if not self.DATA_TO_CONVERT.empty():
                self.statistics['MessagesReceived'] += 1
                (converter, config, data) = self.DATA_TO_CONVERT.get()

                StatisticsService.count_connector_message(self.name, stat_parameter_name='connectorMsgsReceived')
                StatisticsService.count_connector_bytes(self.name, data,
                                                        stat_parameter_name='connectorBytesReceived')

                self._log.debug('Data from Charge Point: %s', data)
                converted_data: ConvertedData = converter.convert(config, data)
                if (converted_data and
                        (converted_data.attributes_datapoints_count > 0 or
                         converted_data.telemetry_datapoints_count > 0)):
                    self.DATA_TO_SEND.put(converted_data)

            sleep(.001)

    def _send_data(self):
        while not self.__stopped:
            if not self.DATA_TO_SEND.empty():
                converted_data = self.DATA_TO_SEND.get()
                self._gateway.send_to_storage(self.name, self.get_id(), converted_data)
                self.statistics['MessagesSent'] += 1
                self._log.info("Data to ThingsBoard: %s", converted_data)

            sleep(.001)

    def __run_coroutine(self, coroutine, timeout):
        future = asyncio.run_coroutine_threadsafe(coroutine, self.__loop)
        try:
            return future.result(timeout)
        except FutureTimeoutError:
            future.cancel()
            raise

    def __call_and_wait(self, charge_point, request, timeout=RPC_DEFAULT_TIMEOUT):
        try:
            return self.__run_coroutine(charge_point.call(request), timeout)
        except FutureTimeoutError:
            self._log.warning('Timeout (%ss) waiting for %s response from Charge Point %s',
                              timeout, request.__class__.__name__, charge_point.name)

    def _get_charge_point_by_name(self, name):
        for charge_point in self._connected_charge_points:
            if charge_point.name == name:
                return charge_point

    @CollectAllReceivedBytesStatistics(start_stat_type='allReceivedBytesFromTB')
    def on_attributes_update(self, content):
        self._log.debug('Got attribute update: %s', content)

        charge_point = self._get_charge_point_by_name(content['device'])
        if charge_point is None:
            self._log.error('Charge Point with name %s not found!', content['device'])
            return

        try:
            for attribute_update_config in charge_point.config.get('attributeUpdates', []):
                for (attr_key, attr_value) in content['data'].items():
                    if attr_key == attribute_update_config['attributeOnThingsBoard']:
                        data = attribute_update_config["valueExpression"] \
                            .replace("${attributeKey}", str(attr_key)) \
                            .replace("${attributeValue}", str(attr_value))
                        request = call.DataTransfer('1', data=data)
                        timeout = attribute_update_config.get('timeout', RPC_DEFAULT_TIMEOUT)
                        self._log.debug(self.__call_and_wait(charge_point, request, timeout))
        except Exception as e:
            self._log.exception(e)

    @CollectAllReceivedBytesStatistics(start_stat_type='allReceivedBytesFromTB')
    def server_side_rpc_handler(self, rpc_request) -> RPCResponse:
        self._log.debug('Got RPC: %s', rpc_request)

        response = RPCResponse(rpc_request.id, device=rpc_request.device_name)
        try:
            if rpc_request.rpc_type == RPCType.DEVICE:
                charge_point = self.__get_connected_charge_point(rpc_request.device_name)
                request, timeout = self.__build_device_rpc_request(charge_point, rpc_request.id,
                                                                   rpc_request.method_name, rpc_request.params)
            elif rpc_request.rpc_type == RPCType.RESERVED:
                charge_point = self.__get_connected_charge_point(rpc_request.device_name)
                request, timeout = self.__build_reserved_rpc_request(rpc_request.method_name, rpc_request.params)
            elif rpc_request.rpc_type == RPCType.CONNECTOR:
                raise ValueError('Connector RPC is not supported by OCPP connector')
            else:
                raise ValueError(f'Invalid RPC type request: {rpc_request}')

            timeout = min(float(timeout), float(rpc_request.timeout))
            response.set_message(self.__call(charge_point, request, timeout))
        except Exception as e:
            self._log.error('Failed to process RPC request %s: %s', rpc_request, e)
            response.set_error_msg(f"Failed to process '{rpc_request.method_name}' RPC request: {e}")

        return response

    def __get_connected_charge_point(self, name):
        charge_point = self._get_charge_point_by_name(name)
        if charge_point is None:
            raise ValueError(f'Charge Point {name} is not connected')

        return charge_point

    def __build_device_rpc_request(self, charge_point, rpc_id, method_name, params):
        rpc_config = next((rpc for rpc in charge_point.config.get('serverSideRpc', [])
                           if rpc.get('methodRPC') == method_name), None)
        if rpc_config is None:
            raise ValueError('Neither of configured device rpc methods match')

        value_expression = rpc_config.get('valueExpression')
        if not value_expression:
            raise ValueError("'valueExpression' is not configured for this RPC")

        body = {'id': rpc_id, 'method': method_name, 'params': params}
        data_to_send_tags = TBUtility.get_values(value_expression, body, 'params', get_tag=True)
        data_to_send_values = TBUtility.get_values(value_expression, body, 'params', expression_instead_none=True)

        data_to_send = value_expression
        for (tag, value) in zip(data_to_send_tags, data_to_send_values):
            data_to_send = data_to_send.replace('${' + tag + '}', dumps(value))

        request = self.__build_ocpp_request(rpc_config, data_to_send)
        return request, rpc_config.get('timeout', RPC_DEFAULT_TIMEOUT)

    def __build_reserved_rpc_request(self, method_name, params):
        if not params:
            raise ValueError("No 'params' found in reserved RPC request")

        is_set = method_name.lower() == 'set'
        pattern = RESERVED_SET_RPC_PATTERN if is_set else RESERVED_GET_RPC_PATTERN
        match = re.fullmatch(pattern, params)
        if match is None:
            expected_schema = RESERVED_SET_RPC_SCHEMA if is_set else RESERVED_GET_RPC_SCHEMA
            raise ValueError(f"The requested RPC does not match with the schema: {expected_schema}")

        groups = match.groupdict()
        component, variable = self.__build_component_variable(groups)
        if is_set:
            request = call.SetVariables(set_variable_data=[{
                'attribute_value': groups['value'],
                'component': component,
                'variable': variable,
            }])
        else:
            request = call.GetVariables(get_variable_data=[{
                'component': component,
                'variable': variable,
            }])

        return request, groups.get('timeout') or RPC_DEFAULT_TIMEOUT

    def __call(self, charge_point, request, timeout):
        action = request.__class__.__name__
        try:
            result = self.__run_coroutine(charge_point.call(request, suppress=False), timeout)
        except FutureTimeoutError:
            raise TimeoutError(f'Timeout ({timeout}s) waiting for {action} response from Charge Point '
                               f'{charge_point.name} (previous call to the Charge Point may still be in progress)')
        except OCPPError as e:
            raise ValueError(f'Charge Point {charge_point.name} returned an error on {action}: {e}')

        return asdict(result) if is_dataclass(result) else result

    def __build_ocpp_request(self, rpc, data_to_send):
        action = rpc.get('action')
        if not action:
            return call.DataTransfer('1', data=data_to_send)

        try:
            payload = loads(data_to_send)
        except ValueError as e:
            raise ValueError(f"Invalid JSON payload for action '{action}': {e}")

        if not isinstance(payload, dict):
            raise ValueError(f"Payload for action '{action}' must be a JSON object")

        return self.__create_ocpp_call(action, payload)

    @staticmethod
    def __create_ocpp_call(action, payload):
        call_cls = getattr(call, action, None)
        if not (isinstance(call_cls, type) and is_dataclass(call_cls)):
            raise ValueError(f"Unknown OCPP action '{action}'")

        try:
            return call_cls(**payload)
        except TypeError as e:
            raise ValueError(f"Invalid payload fields for OCPP action '{action}': {e}")

    @staticmethod
    def __build_component_variable(params):
        component = {'name': params.get('component')}
        if params.get('componentInstance'):
            component['instance'] = params['componentInstance']
        if params.get('evseId'):
            component['evse'] = {'id': int(params['evseId'])}

        variable = {'name': params.get('variable')}
        if params.get('variableInstance'):
            variable['instance'] = params['variableInstance']

        return component, variable

    def get_config(self):
        return {'CS': self._central_system_config, 'CP': self._charge_points_config}
