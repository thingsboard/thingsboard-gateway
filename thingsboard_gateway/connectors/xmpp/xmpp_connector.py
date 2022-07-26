#     Copyright 2022. ThingsBoard
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

from thingsboard_gateway.connectors.connector import Connector, log
from thingsboard_gateway.tb_utility.tb_loader import TBModuleLoader
from thingsboard_gateway.tb_utility.tb_utility import TBUtility
from thingsboard_gateway.gateway.statistics_service import StatisticsService

try:
    from slixmpp import ClientXMPP
except ImportError:
    print("Slixmpp library not found - installing...")
    TBUtility.install_package("bleak")
    from slixmpp import ClientXMPP

from slixmpp.exceptions import IqError, IqTimeout

DEFAULT_UPLINK_CONVERTER = 'XmppUplinkConverter'


class XMPPConnector(Connector, Thread):
    INCOMING_MESSAGES = Queue()
    DATA_TO_SEND = Queue()

    def __init__(self, gateway, config, connector_type):
        self.statistics = {'MessagesReceived': 0,
                           'MessagesSent': 0}
        self.__log = log

        super().__init__()

        self._connector_type = connector_type
        self.__gateway = gateway

        self.__config = config
        self._server_config = config['server']
        self._devices_config = config.get('devices', [])

        self._devices = {}
        self._reformat_devices_config()

        # devices dict for RPC and attributes updates
        # {'deviceName': 'device_jid'}
        self._available_device = {}

        self.setName(config.get("name", 'XMPP Connector ' + ''.join(choice(ascii_lowercase) for _ in range(5))))

        self.__stopped = False
        self._connected = False
        self.daemon = True

        self._xmpp = None

    def _reformat_devices_config(self):
        for config in self._devices_config:
            try:
                device_jid = config.pop('jid')

                converter_name = config.pop('converter', DEFAULT_UPLINK_CONVERTER)
                converter = self._load_converter(converter_name)
                if not converter:
                    continue

                self._devices[device_jid] = config
                self._devices[device_jid]['converter'] = converter(config)
            except KeyError as e:
                self.__log.error('Invalid configuration %s with key error %s', config, e)
                continue

    def _load_converter(self, converter_name):
        module = TBModuleLoader.import_module(self._connector_type, converter_name)

        if module:
            log.debug('Converter %s for device %s - found!', converter_name, self.name)
            return module

        log.error("Cannot find converter for %s device", self.name)
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
                        converted_data = device['converter'].convert(device, msg.values['body'])

                        if converted_data:
                            XMPPConnector.DATA_TO_SEND.put(converted_data)

                            if not self._available_device.get(converted_data['deviceName']):
                                self._available_device[converted_data['deviceName']] = device_jid
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
                data = XMPPConnector.DATA_TO_SEND.get()
                self.statistics['MessagesReceived'] = self.statistics['MessagesReceived'] + 1
                self.__gateway.send_to_storage(self.get_name(), data)
                self.statistics['MessagesSent'] = self.statistics['MessagesSent'] + 1
                self.__log.info('Data to ThingsBoard %s', data)

            sleep(.2)

    def close(self):
        self.__stopped = True
        self._connected = False
        self.__log.info('%s has been stopped.', self.get_name())

    def get_name(self):
        return self.name

    def is_connected(self):
        return self._connected

    @StatisticsService.CollectStatistics(start_stat_type='allBytesSentToDevices')
    def _send_message(self, jid, data):
        self._xmpp.send_message(mto=jid, mfrom=self._server_config['jid'], mbody=data,
                                mtype='chat')

    @StatisticsService.CollectAllReceivedBytesStatistics(start_stat_type='allReceivedBytesFromTB')
    def on_attributes_update(self, content):
        self.__log.debug('Got attribute update: %s', content)

        try:
            device_jid = self._available_device.get(content['device'])
            if not device_jid:
                self.__log.error('Device not found')

            attr_updates = self._devices[device_jid].get('attributeUpdates', [])
            for key, value in content['data'].items():
                for attr_conf in attr_updates:
                    if attr_conf['attributeOnThingsBoard'] == key:
                        data_to_send = attr_conf['valueExpression'].replace('${attributeKey}', key).replace(
                            '${attributeValue}', value)
                        self._send_message(device_jid, data_to_send)
                        return
        except KeyError as e:
            self.__log.error('Key not found %s during processing attribute update', e)

    @StatisticsService.CollectAllReceivedBytesStatistics(start_stat_type='allReceivedBytesFromTB')
    def server_side_rpc_handler(self, content):
        self.__log.debug('Got RPC: %s', content)

        try:
            device_jid = self._available_device.get(content['device'])
            if not device_jid:
                self.__log.error('Device not found')

            rpcs = self._devices[device_jid].get('serverSideRpc', [])
            for rpc_conf in rpcs:
                if rpc_conf['methodRPC'] == content['data']['method']:
                    data_to_send_tags = TBUtility.get_values(rpc_conf.get('valueExpression'), content['data'],
                                                             'params',
                                                             get_tag=True)
                    data_to_send_values = TBUtility.get_values(rpc_conf.get('valueExpression'), content['data'],
                                                               'params',
                                                               expression_instead_none=True)

                    data_to_send = rpc_conf.get('valueExpression')
                    for (tag, value) in zip(data_to_send_tags, data_to_send_values):
                        data_to_send = data_to_send.replace('${' + tag + '}', dumps(value))

                    self._send_message(device_jid, data_to_send)

                    if rpc_conf.get('withResponse', True):
                        self.__gateway.send_rpc_reply(device=content["device"], req_id=content["data"]["id"],
                                                      success_sent=True)

                        return
        except KeyError as e:
            self.__log.error('Key not found %s during processing rpc', e)
