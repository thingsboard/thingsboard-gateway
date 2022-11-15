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
from time import sleep
from random import choice
from string import ascii_lowercase
from threading import Thread
from queue import Queue

from thingsboard_gateway.tb_utility.tb_utility import TBUtility
from thingsboard_gateway.gateway.statistics_service import StatisticsService

try:
    from bleak import BleakScanner
except ImportError:
    print("BLE library not found - installing...")
    TBUtility.install_package("bleak")

from thingsboard_gateway.connectors.connector import Connector, log
from thingsboard_gateway.connectors.ble.device import Device


class BLEConnector(Connector, Thread):
    process_data = Queue(-1)

    def __init__(self, gateway, config, connector_type):
        self.statistics = {'MessagesReceived': 0,
                           'MessagesSent': 0}
        super().__init__()
        self._connector_type = connector_type
        self.__gateway = gateway
        self.__config = config

        self.daemon = True
        self.setName(self.__config.get("name", 'BLE Connector ' + ''.join(choice(ascii_lowercase) for _ in range(5))))

        self.__stopped = False
        self.__connected = False

        if self.__config.get('showMap', False):
            loop = asyncio.new_event_loop()
            loop.run_until_complete(self.__show_map())

        self.__devices = []
        self.__configure_and_load_devices()

    async def __show_map(self):
        scanner = self.__config.get('scanner', {})
        devices = await BleakScanner(
            scanning_mode='active' if self.__config.get('passiveScanMode', True) else 'passive').discover(
            timeout=scanner.get('timeout', 10000) / 1000)

        log.info('FOUND DEVICES')
        if scanner.get('deviceName'):
            found_devices = [x.__str__() for x in filter(lambda x: x.name == scanner['deviceName'], devices)]
            if found_devices:
                log.info(', '.join(found_devices))
            else:
                log.info('nothing to show')
        else:
            for device in devices:
                log.info(device)

    def __configure_and_load_devices(self):
        self.__devices = [Device({**device, 'callback': BLEConnector.callback, 'connector_type': self._connector_type})
                          for device in self.__config.get('devices', [])]

    def open(self):
        self.__stopped = False
        self.start()

    @classmethod
    def callback(cls, not_converted_data):
        cls.process_data.put(not_converted_data)

    def run(self):
        self.__connected = True

        thread = Thread(target=self.__process_data, daemon=True, name='BLE Process Data Thread')
        thread.start()

    def close(self):
        self.__stopped = True
        log.info('%s has been stopped.', self.get_name())

    def get_name(self):
        return self.name

    def is_connected(self):
        return self.__connected

    def __process_data(self):
        while not self.__stopped:
            if not BLEConnector.process_data.empty():
                device_config = BLEConnector.process_data.get()
                data = device_config.pop('data')
                config = device_config.pop('config')
                converter = device_config.pop('converter')

                try:
                    converter = converter(device_config)
                    converted_data = converter.convert(config, data)
                    self.statistics['MessagesReceived'] = self.statistics['MessagesReceived'] + 1
                    log.debug(converted_data)

                    if converted_data is not None:
                        self.__gateway.send_to_storage(self.get_name(), converted_data)
                        self.statistics['MessagesSent'] = self.statistics['MessagesSent'] + 1
                        log.info('Data to ThingsBoard %s', converted_data)
                except Exception as e:
                    log.exception(e)
            else:
                sleep(.2)

    @StatisticsService.CollectAllReceivedBytesStatistics(start_stat_type='allReceivedBytesFromTB')
    def on_attributes_update(self, content):
        try:
            device = tuple(filter(lambda i: i.getName() == content['device'], self.__devices))[0]

            for attribute_update_config in device.config['attributeUpdates']:
                for attribute_update in content['data']:
                    if attribute_update_config['attributeOnThingsBoard'] == attribute_update:
                        device.write_char(attribute_update_config['characteristicUUID'],
                                          bytes(str(content['data'][attribute_update]),
                                                'utf-8'))

        except IndexError:
            log.error('Device not found')

    @StatisticsService.CollectAllReceivedBytesStatistics(start_stat_type='allReceivedBytesFromTB')
    def server_side_rpc_handler(self, content):
        try:
            device = tuple(filter(lambda i: i.getName() == content['device'], self.__devices))[0]

            for rpc_config in device.config['serverSideRpc']:
                for (key, value) in content['data'].items():
                    if value == rpc_config['methodRPC']:
                        rpc_method = rpc_config['methodProcessing']
                        return_result = rpc_config['withResponse']
                        result = None

                        if rpc_method.upper() == 'READ':
                            result = device.read_char(rpc_config['characteristicUUID'])
                        elif rpc_method.upper() == 'WRITE':
                            result = device.write_char(rpc_config['characteristicUUID'],
                                                       bytes(str(content['data']['params']), 'utf-8'))
                        elif rpc_method.upper() == 'SCAN':
                            result = device.scan_self(return_result)
                        if return_result:
                            self.__gateway.send_rpc_reply(content['device'], content['data']['id'], str(result))

                        return
        except IndexError:
            log.error('Device not found')
