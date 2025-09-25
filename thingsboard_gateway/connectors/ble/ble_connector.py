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

import asyncio
from queue import Queue
from random import choice
from string import ascii_lowercase
from threading import Thread
from time import sleep, time, monotonic

from thingsboard_gateway.gateway.entities.converted_data import ConvertedData
from thingsboard_gateway.gateway.statistics.decorators import CollectAllReceivedBytesStatistics
from thingsboard_gateway.gateway.statistics.statistics_service import StatisticsService
from thingsboard_gateway.tb_utility.tb_logger import init_logger
from thingsboard_gateway.tb_utility.tb_utility import TBUtility

try:
    from bleak import BleakScanner
except ImportError:
    print("BLE library not found - installing...")
    TBUtility.install_package("bleak")
    from bleak import BleakScanner

from thingsboard_gateway.connectors.connector import Connector
from thingsboard_gateway.connectors.ble.device import Device


class BLEConnector(Connector, Thread):

    def __init__(self, gateway, config, connector_type):
        self.statistics = {'MessagesReceived': 0,
                           'MessagesSent': 0}
        super().__init__()
        self._connector_type = connector_type
        self.__gateway = gateway
        self.__config = config
        self.__id = self.__config.get('id')
        self.__process_data_queue = Queue(-1)
        self.__devices = []
        self.__scan_data = {}
        self.__scanner_task = None
        self.__scanner_poll_period = self.__config.get('scannerPollPeriod', 5000) / 1000
        self.__scanner_timeout = self.__config.get("scanner_timeout", 10000) / 1000
        self.name = self.__config.get("name", 'BLE Connector ' + ''.join(choice(ascii_lowercase) for _ in range(5)))
        self.__scanned_devices = {}
        self.__device_tasks = []
        self.__log = init_logger(self.__gateway, self.name,
                                 self.__config.get('logLevel', 'INFO'),
                                 enable_remote_logging=self.__config.get('enableRemoteLogging', False),
                                 is_connector_logger=True)
        self.__converter_log = init_logger(self.__gateway, self.name + '_converter',
                                           self.__config.get('logLevel', 'INFO'),
                                           enable_remote_logging=self.__config.get('enableRemoteLogging', False),
                                           is_converter_logger=True, attr_name=self.name)

        self.daemon = True
        try:
            self.__loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self.__loop)

        except RuntimeError:
            self.__loop = asyncio.get_event_loop()

        if self.__config.get('showMap', False):
            self.__loop.run_until_complete(self.__show_map())

        else:
            self.__loop.run_until_complete(self.__scan_devices_on_init())

        self.__stopped = False
        self.__connected = False
        self.__configure_and_load_devices()

    async def __scanner_loop(self):
        poll_period = self.__scanner_poll_period
        while not self.__stopped:
            start_time = time()
            try:
                devices = await BLEConnector.bleak_scanner.discover(timeout=self.__scanner_timeout, return_adv=True)
                self.__scanned_devices = devices

            except Exception as e:
                self.__log.error("Error during scanning: %s", str(e))
                self.__log.debug("An error occurred %s", e, exc_info=True)

            elapsed_time = time() - start_time
            sleep_time = max(0, poll_period - elapsed_time)
            await asyncio.sleep(sleep_time)

    async def __show_map(self):
        scanner = self.__config.get('scanner', {})
        devices = await BleakScanner(
            scanning_mode='passive' if self.__config.get('passiveScanMode', True) else 'active').discover(
            timeout=scanner.get('timeout', 10000) / 1000, return_adv=True)
        self.__devices = devices
        self.__log.info('FOUND DEVICES')
        if scanner.get('deviceName'):
            found_devices = [x.__str__() for x in filter(lambda x: x.name == scanner['deviceName'], devices)]
            if found_devices:
                self.__log.info(', '.join(found_devices))
            else:
                self.__log.info('nothing to show')
        else:
            for device in devices:
                self.__log.info(device)

    async def __scan_devices_on_init(self):
        try:
            devices = await BleakScanner.discover(
                scanning_mode='passive' if self.__config.get('passiveScanMode', True) else 'active',
                timeout=self.__scanner_timeout, return_adv=True)
            self.__scanned_devices = devices

        except Exception as e:
            self.__log.error("Error during scanning: %s", str(e))
            self.__log.debug("An error occurred %s", e, exc_info=True)

    def __configure_and_load_devices(self):
        self.__devices = [
            Device({**device, 'callback': self.callback, 'connector_type': self._connector_type}, self.__log)
            for device in self.__config.get('devices', [])]

    def open(self):
        self.__stopped = False
        self.start()

    def callback(self, not_converted_data):
        self.__process_data_queue.put(not_converted_data)

    def run(self):
        self.__connected = True

        if not hasattr(BLEConnector, "bleak_scanner") or BLEConnector.bleak_scanner is None:
            BLEConnector.bleak_scanner = BleakScanner(scanning_mode='active')
            self.__log.debug('Initialized new BleakScanner instance')

        self.__scanner_task = self.__loop.create_task(self.__scanner_loop())
        self.__device_tasks = [
            self.__loop.create_task(device.run_client(advertisement_packet=self.get_advertisement_packet_callback))
            for device in self.__devices
        ]

        Thread(target=self.__process_data, daemon=True, name='BLE Process Data Thread').start()

        self.__loop.run_forever()

    def __check_is_alive(self):
        start_time = monotonic()

        while self.is_alive():
            if monotonic() - start_time > 10:
                self.__log.error("Failed to stop connector %s", self.get_name())
                return
            sleep(.1)
        self.__log.info("Connector %s stopped", self.get_name())

    async def __disconnect_all_devices(self):
        for device in self.__devices:
            try:
                await device.client.disconnect()
            except Exception as e:
                self.__log.debug("An error occurred while disconnecting device %s: %s", device.name, e)

    def close(self):
        self.__log.info('Closing BLE connector...')
        self.__connected = False
        self.__stopped = True
        for device in self.__devices:
            device.stop()
        for task in self.__device_tasks:
            task.cancel()
        asyncio.run_coroutine_threadsafe(self.__disconnect_all_devices(), self.__loop)
        self.__check_is_alive()

    def get_name(self):
        return self.name

    def get_id(self):
        return self.__id

    def get_type(self):
        return self._connector_type

    def is_connected(self):
        return self.__connected

    def is_stopped(self):
        return self.__stopped

    def __process_data(self):
        while not self.__stopped:
            if not self.__process_data_queue.empty():
                device_config = self.__process_data_queue.get()
                data = device_config.pop('data')
                config = device_config.pop('config')
                converter = device_config.pop('converter')

                StatisticsService.count_connector_message(self.name, stat_parameter_name='connectorMsgsReceived')
                StatisticsService.count_connector_bytes(self.name, data, stat_parameter_name='connectorBytesReceived')

                try:
                    converter = converter(device_config, self.__converter_log)
                    converted_data: ConvertedData = converter.convert(config, data)
                    self.statistics['MessagesReceived'] = self.statistics['MessagesReceived'] + 1
                    self.__log.debug(converted_data)

                    if (converted_data is not None
                            and (converted_data.telemetry_datapoints_count > 0
                                 or converted_data.attributes_datapoints_count > 0)):
                        self.__gateway.send_to_storage(self.get_name(), self.get_id(), converted_data)
                        self.statistics['MessagesSent'] = self.statistics['MessagesSent'] + 1
                        self.__log.info('Data to ThingsBoard %s', converted_data)
                except Exception as e:
                    self.__log.exception(e)
            else:
                sleep(.2)

    @CollectAllReceivedBytesStatistics(start_stat_type='allReceivedBytesFromTB')
    def on_attributes_update(self, content):
        try:
            self.__log.debug('Received attributes update %s', content)

            device = self.__find_device_by_name(content['device'])
            if device is None:
                self.__log.error('Device not found')
                return

            for attribute_update_config in device.config['attributeUpdates']:
                for attribute_update in content['data']:
                    if attribute_update_config['attributeOnThingsBoard'] == attribute_update:
                        device.write_char(attribute_update_config['characteristicUUID'],
                                          bytes(str(content['data'][attribute_update]),
                                                'utf-8'))
        except Exception as e:
            self.__log.error('Error while processing attributes update %s', e)

    def __find_device_by_name(self, name):
        device_filter = tuple(filter(lambda i: i.getName() == name, self.__devices))

        if len(device_filter):
            return device_filter[0]

    @CollectAllReceivedBytesStatistics(start_stat_type='allReceivedBytesFromTB')
    def server_side_rpc_handler(self, content):
        try:
            self.__log.debug('Received RPC request %s', content)

            device = self.__find_device_by_name(content['device'])
            if device is None:
                self.__log.error('Device not found')
                return

            rpc_method_name = content["data"]["method"]

            if self.__check_and_process_reserved_rpc(device, rpc_method_name, content):
                return

            rpc_config_filter = tuple(filter(
                lambda config: config['methodRPC'] == rpc_method_name, device.config['serverSideRpc']))
            if len(rpc_config_filter):
                self.__process_rpc_request(device, rpc_config_filter[0], content)
            else:
                self.__log.error('RPC method not found')
        except Exception as e:
            self.__log.error('Error while processing RPC request %s', e)
            self.__gateway.send_rpc_reply(content['device'],
                                          req_id=content['data']['id'],
                                          content={'error': e.__repr__(), "success": False})

    def __check_and_process_reserved_rpc(self, device, rpc_method_name, content):
        if rpc_method_name in ('get', 'set'):
            self.__log.debug('Processing reserved RPC method: %s', rpc_method_name)

            params = {}
            for param in content['data']['params'].split(';'):
                try:
                    (key, value) = param.split('=')
                except ValueError:
                    continue

                if key and value:
                    params[key] = value

            if rpc_method_name == 'get':
                params['methodProcessing'] = 'READ'

            if rpc_method_name == 'set':
                params['methodProcessing'] = 'WRITE'
                content['data']['params'] = params['value']

            params['withResponse'] = True

            self.__process_rpc_request(device, params, content)

            return True

        return False

    def __process_rpc_request(self, device, rpc_config, content):
        rpc_method = rpc_config['methodProcessing']
        return_result = rpc_config['withResponse']
        result = None

        if rpc_method.upper() == 'READ':
            result = device.read_char(rpc_config['characteristicUUID'])
        elif rpc_method.upper() == 'WRITE':
            result = device.write_char(rpc_config['characteristicUUID'], bytes(str(content['data']['params']), 'utf-8'))
        elif rpc_method.upper() == 'SCAN':
            result = device.scan_self(return_result)

        if return_result:
            self.__gateway.send_rpc_reply(content['device'], content['data']['id'], str(result))

    def get_config(self):
        return self.__config

    def get_advertisement_packet_callback(self):
        return self.__scanned_devices
