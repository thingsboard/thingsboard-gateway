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

from threading import Thread
from platform import system
from time import time, sleep
import asyncio

from bleak import BleakClient, BleakScanner

from thingsboard_gateway.connectors.connector import log
from thingsboard_gateway.gateway.statistics_service import StatisticsService
from thingsboard_gateway.tb_utility.tb_loader import TBModuleLoader
from thingsboard_gateway.connectors.ble.error_handler import ErrorHandler

MAC_ADDRESS_FORMAT = {
    'Darwin': '-',
    'other': ':'
}
DEFAULT_CONVERTER_CLASS_NAME = 'BytesBLEUplinkConverter'


class Device(Thread):
    def __init__(self, config):
        super().__init__()
        self.loop = None
        self.stopped = False
        self.name = config['name']
        self.device_type = config.get('deviceType', 'default')
        self.timeout = config.get('timeout', 10000) / 1000
        self.connect_retry = config.get('connectRetry', 5)
        self.connect_retry_in_seconds = config.get('connectRetryInSeconds', 0)
        self.wait_after_connect_retries = config.get('waitAfterConnectRetries', 0)
        self.show_map = config.get('showMap', False)
        self.__connector_type = config['connector_type']

        self.daemon = True

        try:
            self.mac_address = self.validate_mac_address(config['MACAddress'])
            self.client = BleakClient(self.mac_address)
        except ValueError as e:
            self.client = None
            self.stopped = True
            log.error(e)

        self.poll_period = config.get('pollPeriod', 5000) / 1000
        self.config = self._generate_config(config)
        self.adv_only = self._check_adv_mode()
        self.callback = config['callback']
        self.last_polled_time = self.poll_period + 1

        self.notifying_chars = []

        self.start()

    def _check_adv_mode(self):
        if len(self.config['characteristic']['telemetry']) or len(self.config['characteristic']['attributes']):
            return False

        return True

    def _generate_config(self, config):
        new_config = {
            'characteristic': {
                'extension': self.__load_converter(config.get('extension', DEFAULT_CONVERTER_CLASS_NAME if config.get(
                    'type', 'bytes') == 'bytes' else 'HexBytesBLEUplinkConverter')),
                'telemetry': [],
                'attributes': []
            },
            'advertisement': {
                'extension': self.__load_converter(config.get('extension', 'HexBytesBLEUplinkConverter' if config.get(
                    'type', 'hex') == 'hex' else DEFAULT_CONVERTER_CLASS_NAME)),
                'telemetry': [],
                'attributes': []
            },
            'attributeUpdates': config.get('attributeUpdates', []),
            'serverSideRpc': config.get('serverSideRpc', [])
        }

        for section in ('telemetry', 'attributes'):
            for section_config in config[section]:
                if section_config.get('dataSourceType', 'characteristic') == 'characteristic':
                    new_config['characteristic'][section].append(section_config)
                else:
                    new_config['advertisement'][section].append(section_config)

        return new_config

    @staticmethod
    def validate_mac_address(mac_address):
        os_name = system()

        if MAC_ADDRESS_FORMAT[os_name if os_name == 'Darwin' else 'other'] not in mac_address:
            raise ValueError(f'Mac-address is invalid for {os_name} os')

        return mac_address.upper()

    def __load_converter(self, name):
        module = TBModuleLoader.import_module(self.__connector_type, name)

        if module:
            log.debug('Converter %s for device %s - found!', name, self.name)
            return module
        else:
            log.error("Cannot find converter for %s device", self.name)
            self.stopped = True

    async def timer(self):
        while True:
            try:
                if time() - self.last_polled_time >= self.poll_period:
                    self.last_polled_time = time()
                    await self.__process_self()
                    await self._process_adv_data()
                else:
                    await asyncio.sleep(.2)
            except Exception as e:
                log.error('Problem with connection')
                log.debug(e)

                try:
                    await self.client.disconnect()
                except Exception as err:
                    log.exception(err)

                connect_try = 0
                while not self.stopped and not self.client.is_connected:
                    await self.connect_to_device()

                    connect_try += 1
                    if connect_try == self.connect_retry:
                        sleep(self.wait_after_connect_retries)

                    sleep(self.connect_retry_in_seconds)
                    sleep(.2)

    async def notify_callback(self, sender: int, data: bytearray):
        not_converted_data = {'telemetry': [], 'attributes': []}
        for section in ('telemetry', 'attributes'):
            for item in self.config['characteristic'][section]:
                if item.get('handle') and item['handle'] == sender:
                    not_converted_data[section].append({'data': data, **item})

                    data_for_converter = {
                        'deviceName': self.name,
                        'deviceType': self.device_type,
                        'converter': self.config['characteristic']['extension'],
                        'config': {
                            **self.config['characteristic']
                        },
                        'data': not_converted_data
                    }

                    self.callback(data_for_converter)

    async def notify(self, char_id):
        await self.client.start_notify(char_id, self.notify_callback)

    async def __process_self(self):
        not_converted_data = {'telemetry': [], 'attributes': []}
        for section in ('telemetry', 'attributes'):
            for item in self.config['characteristic'][section]:
                char_id = item['characteristicUUID']

                if item['method'] == 'read':
                    try:
                        data = await self.client.read_gatt_char(char_id)
                        not_converted_data[section].append({'data': data, **item})
                    except Exception as e:
                        error = ErrorHandler(e)
                        if error.is_char_not_found() or error.is_operation_not_supported():
                            log.error(e)
                            pass
                        else:
                            raise e
                elif item['method'] == 'notify' and char_id not in self.notifying_chars:
                    try:
                        self.__set_char_handle(item, char_id)
                        self.notifying_chars.append(char_id)
                        await self.notify(char_id)
                    except Exception as e:
                        error = ErrorHandler(e)
                        if error.is_char_not_found() or error.is_operation_not_supported():
                            log.error(e)
                            pass
                        else:
                            raise e

        if len(not_converted_data['telemetry']) > 0 or len(not_converted_data['attributes']) > 0:
            data_for_converter = {
                'deviceName': self.name,
                'deviceType': self.device_type,
                'converter': self.config['characteristic']['extension'],
                'config': {
                    **self.config['characteristic']
                },
                'data': not_converted_data
            }
            self.callback(data_for_converter)

    def __set_char_handle(self, item, char_id):
        for serv in self.client.services:
            for char in serv.characteristics:
                if char.uuid == char_id:
                    item['handle'] = char.handle
                    return

    async def _connect_to_device(self):
        try:
            log.info('Trying to connect to %s with %s MAC address', self.name, self.mac_address)
            await self.client.connect(timeout=self.timeout)
        except Exception as e:
            log.error(e)

    def filter_macaddress(self, device):
        macaddress, device = device
        if macaddress == self.mac_address:
            return True

        return False

    async def _process_adv_data(self):
        devices = await BleakScanner(scanning_mode="active").discover(timeout=self.timeout, return_adv=True)

        try:
            device = tuple(filter(self.filter_macaddress, devices.items()))[0][-1]
        except IndexError:
            log.error('Device with MAC address %s not found!', self.mac_address)
            return

        try:
            advertisement_data = list(device[-1].manufacturer_data.values())[0]
        except (IndexError, AttributeError):
            log.error('Device %s haven\'t advertisement data', self.name)
            return

        data_for_converter = {
            'deviceName': self.name,
            'deviceType': self.device_type,
            'converter': self.config['advertisement']['extension'],
            'config': {
                **self.config['advertisement']
            },
            'data': advertisement_data
        }

        self.callback(data_for_converter)

    async def connect_to_device(self):
        while not self.stopped and not self.client.is_connected:
            await self._connect_to_device()

            sleep(.2)

    async def run_client(self):
        if not self.adv_only or self.show_map:
            # default mode
            await self.connect_to_device()

            if self.client and self.client.is_connected:
                log.info('Connected to %s device', self.name)

                if self.show_map:
                    await self.__show_map()

                await self.timer()
        else:
            while not self.stopped:
                await self._process_adv_data()
                sleep(self.poll_period)

    def run(self):
        self.loop = asyncio.new_event_loop()
        self.loop.run_until_complete(self.run_client())

    async def __show_map(self, return_result=False):
        result = f'MAP FOR {self.name.upper()}'

        for service in self.client.services:
            result += f'\n| [Service] {service}'
            for char in service.characteristics:
                if "read" in char.properties:
                    try:
                        value = bytes(await self.client.read_gatt_char(char.uuid))
                        result += f"\n| \t[Characteristic] {char} ({','.join(char.properties)}), Value: {value}"
                    except Exception as e:
                        result += f"\n| \t[Characteristic] {char} ({','.join(char.properties)}), Value: {e}"

                else:
                    value = None
                    result += f"\n| \t[Characteristic] {char} ({','.join(char.properties)}), Value: {value}"

                for descriptor in char.descriptors:
                    try:
                        value = bytes(
                            await self.client.read_gatt_descriptor(descriptor.handle)
                        )
                        result += f"\n| \t\t[Descriptor] {descriptor}) | Value: {value}"
                    except Exception as e:
                        result += f"| \t\t[Descriptor] {descriptor}) | Value: {e}"

        if return_result:
            return result
        else:
            log.info(result)

    def scan_self(self, return_result):
        task = self.loop.create_task(self.__show_map(return_result))

        while not task.done():
            sleep(.2)

        return task.result()

    async def __write_char(self, char_id, data):
        try:
            await self.client.write_gatt_char(char_id, data, response=True)
            await asyncio.sleep(1.0)
            return 'Ok'
        except Exception as e:
            log.error('Can\'t write data to device')
            log.exception(e)
            return e

    @StatisticsService.CollectStatistics(start_stat_type='allBytesSentToDevices')
    def write_char(self, char_id, data):
        task = self.loop.create_task(self.__write_char(char_id, data))

        while not task.done():
            sleep(.2)

        return task.result()

    async def __read_char(self, char_id):
        try:
            return await self.client.read_gatt_char(char_id)
        except Exception as e:
            log.exception(e)

    def read_char(self, char_id):
        task = self.loop.create_task(self.__read_char(char_id))

        while not task.done():
            sleep(.2)

        return task.result().decode('UTF-8')

    def __str__(self):
        return f'{self.name}'
