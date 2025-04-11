#      Copyright 2025. ThingsBoard
#
#      Licensed under the Apache License, Version 2.0 (the "License");
#      you may not use this file except in compliance with the License.
#      You may obtain a copy of the License at
#
#          http://www.apache.org/licenses/LICENSE-2.0
#
#      Unless required by applicable law or agreed to in writing, software
#      distributed under the License is distributed on an "AS IS" BASIS,
#      WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#      See the License for the specific language governing permissions and
#      limitations under the License.

import asyncio
from queue import Empty, Queue
from re import fullmatch
from threading import Thread, Event
from time import sleep

from thingsboard_gateway.gateway.constants import STATISTIC_MESSAGE_SENT_PARAMETER
from thingsboard_gateway.gateway.statistics.decorators import CollectAllReceivedBytesStatistics
from thingsboard_gateway.gateway.statistics.statistics_service import StatisticsService
from thingsboard_gateway.tb_utility.tb_utility import TBUtility
from thingsboard_gateway.connectors.connector import Connector
from thingsboard_gateway.tb_utility.tb_logger import init_logger

try:
    from xknx import XKNX
except ImportError:
    print("xknx library not found")
    TBUtility.install_package("xknx")
    from xknx import XKNX

from thingsboard_gateway.connectors.knx.entities.client_config import ClientConfig
from thingsboard_gateway.connectors.knx.entities.gateways_scanner import GatewaysScanner
from thingsboard_gateway.connectors.knx.entities.device import Device

from xknx.tools import read_group_value, group_value_write


class KNXConnector(Connector, Thread):
    def __init__(self, gateway, config, connector_type):
        super().__init__()

        self.__gateway = gateway
        self._connector_type = connector_type

        self.__log = init_logger(self.__gateway, config['name'],
                                 config.get('logLevel', 'INFO'),
                                 enable_remote_logging=config.get('enableRemoteLogging', False),
                                 is_connector_logger=True)
        self.__converter_log = init_logger(self.__gateway, config['name'] + '_converter',
                                           config.get('logLevel', 'INFO'),
                                           enable_remote_logging=config.get('enableRemoteLogging', False),
                                           is_converter_logger=True, attr_name=config['name'])

        self.statistics = {'MessagesReceived': 0, 'MessagesSent': 0}

        self.__config = config
        self.__id = self.__config.get('id')
        self.__connected = False
        self.__stopped = Event()
        self.daemon = True

        self.__process_device_request_queue = Queue(-1)
        self.__data_to_convert_queue = Queue(-1)
        self.__data_to_save_queue = Queue(-1)

        self.__loop = self.__create_event_loop()

        self.__is_client_connected = asyncio.Event()
        self.__client = None

        self.__devices = []
        self.__add_devices()

        self.__log.info('Starting KNX Connector...')

    def __add_devices(self):
        for device_config in self.__config.get('devices', []):
            self.__devices.append(Device(self._connector_type,
                                         self.__is_client_connected,
                                         device_config,
                                         self.__process_device_request_queue,
                                         self.__converter_log))

    def __create_event_loop(self):
        try:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            return loop
        except RuntimeError:
            return asyncio.get_event_loop()

    def close(self):
        self.__stopped.set()
        self.__connected = False

        self.__log.debug('Stopping %s...', self.get_name())

        self.__stop_devices()

        if TBUtility.while_thread_alive(self):
            self.__log.error("Failed to stop connector %s", self.get_name())
        else:
            self.__log.info('%s has been stopped', self.get_name())

        self.__converter_log.stop()
        self.__log.stop()

    def __stop_devices(self):
        for device in self.__devices:
            device.stop()

    def open(self):
        self.start()

    @staticmethod
    def telegram_received_cb(telegram):
        print("Telegram received: {0}".format(telegram))

    def run(self):
        try:
            self.__connected = True
            self.__loop.run_until_complete(self.__start())
        except asyncio.CancelledError as e:
            self.__log.debug('Task was cancelled due to connector stop: %s', e.__str__())
        except Exception as e:
            self.__log.exception(e)

    async def __start(self):
        client_config = self.__config.get('client', {})

        # check if gateways scanner is configured
        await self.__check_and_scan_gateways(client_config)

        self.__create_client(client_config)

        await asyncio.gather(self.__start_client_with_block(),
                             self.__process_device_request(),
                             self.__convert_data(),
                             self.__send_data())

    async def __check_and_scan_gateways(self, client_config):
        if GatewaysScanner.is_configured(client_config):
            gateway_scanner = GatewaysScanner(client_config, self.__log)
            await gateway_scanner.scan()

    async def __start_client_with_block(self):
        while not self.__stopped.is_set():
            try:
                await self.__client.start()
                self.__log.info('Connected to KNX bus')
                self.__is_client_connected.set()

                while not self.__stopped.is_set():
                    await asyncio.sleep(.02)
            except Exception as e:
                self.__log.error('Error connecting to KNX bus: %s', e.__str__())

        await self.__client.stop()
        self.__is_client_connected.clear()

    def __create_client(self, client_config):
        client_config = ClientConfig(client_config)
        self.__client = XKNX(**client_config.__dict__)

    async def __process_device_request(self):
        while not self.__stopped.is_set():
            try:
                device = self.__process_device_request_queue.get_nowait()

                if self.__client.connection_manager.connected:
                    responses = {}

                    for key, config in device.group_addresses_to_read.items():
                        try:
                            response = await read_group_value(self.__client, key, config.get('type'))

                            if response is not None:
                                self.__log.trace('Response from KNX bus: %s', response)
                                responses[key] = {
                                    'type': config['type'],
                                    'response': response,
                                    'keys': config['keys']
                                }
                            else:
                                self.__log.warning('No response from KNX bus for %s.', key)
                        except Exception as e:
                            self.__log.exception('Error processign %s request: %s', device.name, e)

                    if responses:
                        self.__data_to_convert_queue.put((device, responses))
                else:
                    self.__log.error('KNX bus is not connected')
            except Empty:
                await asyncio.sleep(.01)

    async def __convert_data(self):
        while not self.__stopped.is_set():
            try:
                device, data_to_convert = self.__data_to_convert_queue.get_nowait()
                converted_data = device.uplink_converter.convert(data_to_convert)

                if converted_data.telemetry_datapoints_count > 0 or converted_data.attributes_datapoints_count > 0:
                    self.__data_to_save_queue.put((device, converted_data))
            except Empty:
                await asyncio.sleep(.01)
            except Exception as e:
                self.__log.error('Error converting data: %s', e)

    async def __send_data(self):
        while not self.__stopped.is_set():
            try:
                device, data_to_save = self.__data_to_save_queue.get_nowait()
                self.__log.trace('%s data to save: %s', device, data_to_save)
                StatisticsService.count_connector_message(self.get_name(), stat_parameter_name='storageMsgPushed')
                self.__gateway.send_to_storage(self.get_name(), self.get_id(), data_to_save)
                self.statistics[STATISTIC_MESSAGE_SENT_PARAMETER] += 1
            except Empty:
                await asyncio.sleep(.01)
            except Exception as e:
                self.__log.error('Error saving data: %s', e)

    @CollectAllReceivedBytesStatistics(start_stat_type='allReceivedBytesFromTB')
    def on_attributes_update(self, content):
        self.__log.debug('Received attribute update request: %s', content)

        try:
            attribute_request_config, value = self.__find_attribute_request_by_device_and_name(content)
            if attribute_request_config:
                result = {}
                self.__create_task(self.__process_attribute_update,
                                   (attribute_request_config['groupAddress'],
                                    attribute_request_config.get('dataType'),
                                    value),
                                   {'result': result})
            else:
                self.__log.error('No attribute request config found')
        except Exception as e:
            self.__log.error('Error processing attribute update request: %s', e)

    def __find_attribute_request_by_device_and_name(self, content):
        for attribute_request_config in self.__config.get('attributeUpdates', []):
            device_name_match = fullmatch(attribute_request_config['deviceNameFilter'], content['device'])

            attr_name_match_fitler = False
            value = None
            for attr_k, attr_v in content['data'].items():
                if attr_k == attribute_request_config['key']:
                    attr_name_match_fitler = True
                    value = attr_v

            if device_name_match and attr_name_match_fitler:
                return attribute_request_config, value

    async def __process_attribute_update(self, group_address, data_type, value, result={}):
        if self.__client.connection_manager.connected:
            result['response'] = await group_value_write(self.__client, group_address, value, data_type)
        else:
            self.__log.error('KNX bus is not connected')

    @CollectAllReceivedBytesStatistics(start_stat_type='allReceivedBytesFromTB')
    def server_side_rpc_handler(self, content):
        self.__log.debug('Received RPC request: %s', content)

        rpc_method_name = content.get('data', {}).get('method')
        if rpc_method_name is None:
            self.__log.error('Method name not found in RPC request: %r', content)
            return

        # check if RPC method is reserved get/set
        self.__check_and_process_reserved_rpc(rpc_method_name, content)

        rpc_config = self.__find_rpc_config_by_device_and_name(rpc_method_name, content)
        if not rpc_config:
            self.__log.error('No RPC config found')
            return

        self.__process_rpc(content, rpc_config)

    def __check_and_process_reserved_rpc(self, rpc_method_name, content):
        if rpc_method_name in ('get', 'set'):
            params = {}

            for param in content['data']['params'].split(';'):
                try:
                    (key, value) = param.split('=')
                except ValueError:
                    self.__log.trace('Invalid parameter: %s', param)
                    continue

                if key and value:
                    params[key] = value

            content['data'].pop('params', None)

            if params.get('value'):
                content['data']['params'] = params['value']

            self.__process_rpc(content, params)

    def __process_rpc(self, content, rpc_config):
        try:
            result = {}
            value = content.get('data', {}).get('params')
            self.__create_task(self.__process_rpc_request, (rpc_config, ), {
                'value': value, 'result': result})
            self.__log.info('Processed RPC request with result: %r', result)
            self.__gateway.send_rpc_reply(content['device'],
                                          req_id=content['data'].get('id'),
                                          content={'result': str(result.get('response'))})
        except Exception as e:
            self.__log.error('Error processing RPC request: %s', e)
            self.__gateway.send_rpc_reply(device=content['device'],
                                          req_id=content['data'].get('id'),
                                          content={'result': str(e)},
                                          success_sent=False)

    async def __process_rpc_request(self, config, value=None, result={}):
        if self.__client.connection_manager.connected:
            group_address = config['groupAddress']
            data_type = config.get('dataType')

            if value is not None or config['requestType'] == 'write':
                result['response'] = await group_value_write(self.__client, group_address, value, data_type)
            else:
                result['response'] = await read_group_value(self.__client, group_address, data_type)
        else:
            self.__log.error('KNX bus is not connected')

    def __find_rpc_config_by_device_and_name(self, rpc_method_name, content):
        device_name_match_filter = tuple(filter(lambda rpc_config:
                                                fullmatch(rpc_config['deviceNameFilter'], content['device']),
                                                self.__config.get('serverSideRpc', [])))

        rpc_method_name_filter = tuple(filter(lambda rpc_config: rpc_config['method'] == rpc_method_name,
                                              self.__config.get('serverSideRpc', [])))

        if len(device_name_match_filter) and len(rpc_method_name_filter):
            return rpc_method_name_filter[0]

    def __create_task(self, func, args, kwargs):
        task = self.__loop.create_task(func(*args, **kwargs))

        while not task.done() and not self.__stopped.is_set():
            sleep(.02)

    @property
    def connector_type(self):
        return self._connector_type

    def get_id(self):
        return self.__id

    def get_name(self):
        return self.name

    def get_type(self):
        return self._connector_type

    def get_config(self):
        return self.__config

    def is_connected(self):
        return self.__connected

    def is_stopped(self):
        return self.__stopped.is_set()
