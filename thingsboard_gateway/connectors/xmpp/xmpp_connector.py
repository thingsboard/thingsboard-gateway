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
from json import dumps
from queue import Queue
from random import choice
from string import ascii_lowercase
from threading import Thread
from time import sleep
from packaging import version

from thingsboard_gateway.connectors.connector import Connector
from thingsboard_gateway.connectors.xmpp.device import Device
from thingsboard_gateway.gateway.entities.converted_data import ConvertedData
from thingsboard_gateway.gateway.statistics.decorators import CollectStatistics, CollectAllReceivedBytesStatistics
from thingsboard_gateway.tb_utility.tb_loader import TBModuleLoader
from thingsboard_gateway.tb_utility.tb_utility import TBUtility
from thingsboard_gateway.gateway.statistics.statistics_service import StatisticsService
from thingsboard_gateway.gateway.entities.rpc_request import RPCType
from thingsboard_gateway.gateway.entities.rpc_response import RPCResponse
from thingsboard_gateway.tb_utility.tb_logger import init_logger

installation_required = False
required_version = '1.8.6'
force_install = False

try:
    from slixmpp import __version__ as slixmpp_version

    if version.parse(slixmpp_version) != version.parse(required_version):
        installation_required = True
        force_install = True
except ImportError:
    installation_required = True

if installation_required:
    print("Slixmpp library not found - installing...")
    TBUtility.install_package("slixmpp", required_version, force_install=force_install)

from slixmpp import ClientXMPP  # noqa: E402
from slixmpp.exceptions import IqError, IqTimeout  # noqa: E402

DEFAULT_UPLINK_CONVERTER = 'XmppUplinkConverter'


class XMPPConnector(Connector, Thread):
    INCOMING_MESSAGES = Queue()
    DATA_TO_SEND = Queue()

    def __init__(self, gateway, config, connector_type):
        self.statistics = {'MessagesReceived': 0,
                           'MessagesSent': 0}

        super().__init__()

        self._connector_type = connector_type
        self.__gateway = gateway

        self.__config = config
        self.__id = self.__config.get('id')
        self._server_config = config['server']
        self._devices_config = config.get('devices', [])
        self.name = config.get("name", 'XMPP Connector ' + ''.join(choice(ascii_lowercase) for _ in range(5)))
        self.__log = init_logger(self.__gateway, self.name, self.__config.get('logLevel', 'INFO'),
                                 enable_remote_logging=self.__config.get('enableRemoteLogging', False),
                                 is_connector_logger=True)
        self.__converter_log = init_logger(self.__gateway, self.name + '_converter',
                                           self.__config.get('logLevel', 'INFO'),
                                           enable_remote_logging=self.__config.get('enableRemoteLogging', False),
                                           is_connector_logger=True, attr_name=self.name)

        self._devices = {}
        self._reformat_devices_config()

        # devices dict for RPC and attributes updates
        # {'deviceName': 'device_jid'}
        self._available_device = {}

        self.__stopped = False
        self._connected = False
        self.daemon = True

        self._xmpp = None

    def _reformat_devices_config(self):
        for config in self._devices_config:
            try:
                device_jid = config.get('jid')

                converter_name = config.pop('converter', DEFAULT_UPLINK_CONVERTER)
                converter = self._load_converter(converter_name)
                if not converter:
                    continue

                self._devices[device_jid] = Device(
                    jid=device_jid,
                    device_name_expression=config['deviceNameExpression'],
                    device_type_expression=config['deviceTypeExpression'],
                    attributes=config.get('attributes', []),
                    timeseries=config.get('timeseries', []),
                    attribute_updates=config.get('attributeUpdates', []),
                    server_side_rpc=config.get('serverSideRpc', [])
                )
                self._devices[device_jid].set_converter(converter(config, self.__converter_log))
            except KeyError as e:
                self.__log.error('Invalid configuration %s with key error %s', config, e)
                continue

    def _load_converter(self, converter_name):
        module = TBModuleLoader.import_module(self._connector_type, converter_name)

        if module:
            self.__log.debug('Converter %s for device %s - found!', converter_name, self.name)
            return module

        self.__log.error("Cannot find converter for %s device", self.name)
        return None

    def open(self):
        self.__stopped = False
        self.start()
        self.__log.info('Starting XMPP Connector')

    def run(self):
        process_messages_thread = Thread(target=self._process_messages,
                                         name='Validate incoming msgs thread', daemon=True)
        process_messages_thread.start()

        data_to_send_thread = Thread(target=self._send_data, name='Data to send thread', daemon=True)
        data_to_send_thread.start()

        self.create_client()

    def create_client(self):
        # creating event loop for slixmpp
        asyncio.set_event_loop(asyncio.new_event_loop())

        self.__log.info('Starting XMPP Client...')
        self._xmpp = ClientXMPP(jid=self._server_config['jid'], password=self._server_config['password'])

        self._xmpp.add_event_handler("session_start", self.session_start)
        self._xmpp.add_event_handler("message", self.message)
        self._xmpp['feature_mechanisms'].unencrypted_plain = self._server_config.get('unencrypted_plain_auth', True)

        for plugin in self._server_config.get('plugins', []):
            self._xmpp.register_plugin(plugin)

        self._xmpp.connect(address=(self._server_config['host'], self._server_config['port']),
                           use_ssl=self._server_config.get('use_ssl', False),
                           disable_starttls=self._server_config.get('disable_starttls', False),
                           force_starttls=self._server_config.get('force_starttls', True))
        self._xmpp.process(forever=True, timeout=self._server_config.get('timeout', 10000))

    def session_start(self, _):
        try:
            self._xmpp.send_presence()
            self._xmpp.get_roster()
        except IqError as err:
            self.__log.error('There was an error getting the roster')
            self.__log.error(err.iq['error']['condition'])
            self._xmpp.disconnect()
            self.close()
        except IqTimeout:
            self.__log.error('Server is taking too long to respond')
            self._xmpp.disconnect()
            self.close()

        self._connected = True

    @staticmethod
    def message(msg):
        # put incoming messages to the queue because of data missing probability
        XMPPConnector.INCOMING_MESSAGES.put(msg)

    def _process_messages(self):
        while not self.__stopped:
            if not XMPPConnector.INCOMING_MESSAGES.empty():
                msg = XMPPConnector.INCOMING_MESSAGES.get()
                self.__log.debug('Got message: %s', msg.values)

                try:
                    device_jid = msg.values['from']
                    device = self._devices.get(device_jid)
                    if device:
                        StatisticsService.count_connector_message(self.name,
                                                                  stat_parameter_name='connectorMsgsReceived')
                        StatisticsService.count_connector_bytes(self.name, msg.values['body'],
                                                                stat_parameter_name='connectorBytesReceived')

                        converted_data = device.converter.convert(device, msg.values['body'])

                        if converted_data:
                            XMPPConnector.DATA_TO_SEND.put(converted_data)

                            if not self._available_device.get(converted_data.device_name):
                                self._available_device[converted_data.device_name] = device_jid
                        else:
                            self.__log.error('Converted data is empty')
                    else:
                        self.__log.info('Device %s not found', device_jid)
                except KeyError as e:
                    self.__log.exception(e)

            sleep(.2)

    def _send_data(self):
        while not self.__stopped:
            if not XMPPConnector.DATA_TO_SEND.empty():
                data: ConvertedData = XMPPConnector.DATA_TO_SEND.get()
                if data.attributes_datapoints_count > 0 or data.telemetry_datapoints_count > 0:
                    self.statistics['MessagesReceived'] = self.statistics['MessagesReceived'] + 1
                    self.__gateway.send_to_storage(self.get_name(), self.get_id(), data)
                    self.statistics['MessagesSent'] = self.statistics['MessagesSent'] + 1
                    self.__log.info('Data to ThingsBoard %s', data)

            sleep(.2)

    def close(self):
        self.__stopped = True
        self._connected = False
        self.__log.info('%s has been stopped.', self.get_name())
        self.__log.stop()

    def get_id(self):
        return self.__id

    def get_name(self):
        return self.name

    def get_type(self):
        return self._connector_type

    def is_connected(self):
        return self._connected

    def is_stopped(self):
        return self.__stopped

    def get_config(self):
        return self.__config

    @CollectStatistics(start_stat_type='allBytesSentToDevices')
    def _send_message(self, jid, data):
        self._xmpp.send_message(mto=jid, mfrom=self._server_config['jid'], mbody=data,
                                mtype='chat')

    @CollectAllReceivedBytesStatistics(start_stat_type='allReceivedBytesFromTB')
    def on_attributes_update(self, content):
        self.__log.debug('Got attribute update: %s', content)

        try:
            device_jid = self._available_device.get(content['device'])
            if not device_jid:
                self.__log.error('Device not found')

            attr_updates = self._devices[device_jid].attribute_updates
            for key, value in content['data'].items():
                for attr_conf in attr_updates:
                    if attr_conf['attributeOnThingsBoard'] == key:
                        data_to_send = attr_conf['valueExpression'].replace('${attributeKey}', key).replace(
                            '${attributeValue}', value)
                        self._send_message(device_jid, data_to_send)
                        return
        except KeyError as e:
            self.__log.error('Key not found %s during processing attribute update', e)

    @CollectAllReceivedBytesStatistics(start_stat_type='allReceivedBytesFromTB')
    def server_side_rpc_handler(self, rpc_request) -> RPCResponse:
        self.__log.debug('Received RPC: %s', rpc_request)
        response = RPCResponse(rpc_request.id, device=rpc_request.device_name)

        try:
            device_jid = self._available_device.get(rpc_request.device_name)
            if not device_jid:
                err_msg = f'{rpc_request.device_name} device not found'
                self.__log.error(err_msg)
                response.set_error_msg(err_msg)
                return response

            if rpc_request.rpc_type == RPCType.CONNECTOR:
                err_msg = 'Not implemented'
                self.__log.error(err_msg)
                response.set_error_msg(err_msg)
                return response
            elif rpc_request.rpc_type == RPCType.RESERVED:
                return self._process_reserved_rpc(rpc_request, device_jid)
            elif rpc_request.rpc_type == RPCType.DEVICE:
                return self._process_rpc_to_device(rpc_request, device_jid)
            else:
                err_msg = f"Invalid RPC type request: {rpc_request}"
                self.__log.error(err_msg)
                response.set_error_msg(err_msg)
                return response
        except Exception as e:
            error_msg = f"Failed to process RPC with error: {e}"
            self.__log.error(error_msg)
            response.set_error_msg(error_msg)
            return response

    def _process_rpc_to_device(self, rpc_request, device_jid):
        response = RPCResponse(rpc_request.id, device=rpc_request.device_name)

        rpcs = self._devices[device_jid].server_side_rpc
        rpc_config = self._find_rpc_config_by_method_name(rpc_request.method_name, rpcs)
        if rpc_config is None:
            err_msg = f'Config for {rpc_request.method_name} not found'
            self.__log.error(err_msg)
            response.set_error_msg(err_msg)
            return response

        data_to_send_tags = TBUtility.get_values(rpc_config.get('valueExpression'),
                                                 {'params': rpc_request.params},
                                                 'params',
                                                 get_tag=True)
        data_to_send_values = TBUtility.get_values(rpc_config.get('valueExpression'),
                                                   {'params': rpc_request.params},
                                                   'params',
                                                   expression_instead_none=True)

        data_to_send = rpc_config.get('valueExpression')
        for (tag, value) in zip(data_to_send_tags, data_to_send_values):
            data_to_send = data_to_send.replace('${' + tag + '}', dumps(value))

        self._send_message(device_jid, data_to_send)

        response.set_message('success')
        return response

    def _find_rpc_config_by_method_name(self, method_name, rpc_config_list):
        rpc_configs = tuple(filter(lambda config: config['methodRPC'] == method_name, rpc_config_list))
        if len(rpc_configs) == 0:
            return None

        return rpc_configs[0]

    def _process_reserved_rpc(self, rpc_request, device_jid):
        response = RPCResponse(rpc_request.id, device=rpc_request.device_name)

        params = self.__get_reserved_rpc_params(rpc_request.params)
        if len(params) == 0:
            err_msg = 'RPC params are empty, expected format: set value=<value>;'
            self.__log.error(err_msg)
            response.set_error_msg(err_msg)
            return response

        self._send_message(device_jid, dumps(params))
        response.set_message('success')
        return response

    def __get_reserved_rpc_params(self, rpc_params):
        params = {}

        if not rpc_params or not isinstance(rpc_params, str):
            return {}

        for param in rpc_params.split(';'):
            try:
                (key, value) = param.split('=')
            except ValueError:
                continue

            if key and value:
                params[key] = value

        return params
