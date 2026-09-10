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
from queue import Queue
from threading import Thread
from random import choice
from string import ascii_lowercase
from time import sleep, monotonic

from simplejson import dumps

from thingsboard_gateway.connectors.connector import Connector
from thingsboard_gateway.gateway.constants import RPC_DEFAULT_TIMEOUT
from thingsboard_gateway.gateway.entities.converted_data import ConvertedData
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

    @staticmethod
    async def _send_request(cp, request):
        return await cp.call(request)

    @staticmethod
    def __wait_task_with_timeout(task, timeout, poll_interval=0.2):
        start_time = monotonic()
        while not task.done():
            sleep(poll_interval)
            if monotonic() - start_time >= timeout:
                task.cancel()
                return False, None

        return True, task.result()

    def __call_and_wait(self, charge_point, request, timeout=RPC_DEFAULT_TIMEOUT):
        task = self.__loop.create_task(self._send_request(charge_point, request))
        task_completed, result = self.__wait_task_with_timeout(task, timeout)

        if not task_completed:
            self._log.warning('Timeout (%ss) waiting for %s response from Charge Point %s',
                              timeout, request.__class__.__name__, charge_point.name)

        return result

    @CollectAllReceivedBytesStatistics(start_stat_type='allReceivedBytesFromTB')
    def on_attributes_update(self, content):
        self._log.debug('Got attribute update: %s', content)

        try:
            charge_point = tuple(filter(lambda cp: cp.name == content['device'], self._connected_charge_points))[0]
        except IndexError:
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
    def server_side_rpc_handler(self, content):
        self._log.debug('Got RPC: %s', content)

        try:
            charge_point = tuple(filter(lambda cp: cp.name == content['device'], self._connected_charge_points))[0]
        except IndexError:
            self._log.error('Charge Point with name %s not found!', content['device'])
            return

        # check if RPC method is reserved get/set
        try:
            self.__check_and_process_reserved_rpc(charge_point, content)
        except Exception as e:
            self._log.exception(e)

        try:
            for rpc in charge_point.config.get('serverSideRpc', []):
                if rpc['methodRPC'] == content['data']['method']:
                    self.__process_rpc(charge_point, content, rpc)
        except Exception as e:
            self._log.exception(e)

    def __process_rpc(self, charge_point, content, rpc):
        if not rpc.get('valueExpression'):
            return

        data_to_send_tags = TBUtility.get_values(rpc.get('valueExpression'), content['data'],
                                                 'params',
                                                 get_tag=True)
        data_to_send_values = TBUtility.get_values(rpc.get('valueExpression'), content['data'],
                                                   'params',
                                                   expression_instead_none=True)

        data_to_send = rpc.get('valueExpression')
        for (tag, value) in zip(data_to_send_tags, data_to_send_values):
            data_to_send = data_to_send.replace('${' + tag + '}', dumps(value))

        request = call.DataTransfer('1', data=data_to_send)
        timeout = rpc.get('timeout', RPC_DEFAULT_TIMEOUT)
        result = self.__call_and_wait(charge_point, request, timeout)

        if rpc.get('withResponse', True):
            self._gateway.send_rpc_reply(content["device"], content["data"]["id"], {'result': str(result)})

    @staticmethod
    def __parse_rpc_params(raw_params):
        if not isinstance(raw_params, str):
            return {}

        params = {}
        for param in raw_params.split(';'):
            try:
                (key, value) = param.split('=')
            except ValueError:
                continue

            if key and value:
                params[key] = value

        return params

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

    def __send_set_variables(self, charge_point, content, params):
        component, variable = self.__build_component_variable(params)
        request = call.SetVariables(set_variable_data=[{
            'attribute_value': params.get('value'),
            'component': component,
            'variable': variable,
        }])
        timeout = float(params.get('timeout', RPC_DEFAULT_TIMEOUT))
        result = self.__call_and_wait(charge_point, request, timeout)
        self._gateway.send_rpc_reply(content['device'], content['data']['id'], {'result': str(result)})

    def __send_get_variables(self, charge_point, content, params):
        component, variable = self.__build_component_variable(params)
        request = call.GetVariables(get_variable_data=[{
            'component': component,
            'variable': variable,
        }])
        timeout = float(params.get('timeout', RPC_DEFAULT_TIMEOUT))
        result = self.__call_and_wait(charge_point, request, timeout)
        self._gateway.send_rpc_reply(content['device'], content['data']['id'], {'result': str(result)})

    def __check_and_process_reserved_rpc(self, charge_point, content):
        method = content['data']['method']
        if method not in ('get', 'set'):
            return

        params = self.__parse_rpc_params(content['data'].get('params'))
        if not params.get('component') or not params.get('variable'):
            self._log.warning("Reserved RPC '%s' requires 'component' and 'variable' params "
                              "(e.g. 'component=OCPPCommCtrlr;variable=HeartbeatInterval;value=30')", method)
            return

        if method == 'set':
            self.__send_set_variables(charge_point, content, params)
        else:
            self.__send_get_variables(charge_point, content, params)

    def get_config(self):
        return {'CS': self._central_system_config, 'CP': self._charge_points_config}
