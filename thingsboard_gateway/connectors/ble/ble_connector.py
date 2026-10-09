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

from concurrent.futures import TimeoutError as FutureTimeoutError
from json import dumps
import asyncio
from queue import Queue
from random import choice
from re import fullmatch
from string import ascii_lowercase
from threading import Thread
from time import sleep, time, monotonic

from thingsboard_gateway.gateway.entities.converted_data import ConvertedData
from thingsboard_gateway.gateway.entities.rpc_request import RPCType
from thingsboard_gateway.gateway.entities.rpc_response import RPCResponse
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

from thingsboard_gateway.connectors.ble.constants import (
    RESERVED_GET_RPC_SCHEMA,
    RESERVED_SET_RPC_SCHEMA,
    RESERVED_GET_RPC_PATTERN,
    RESERVED_SET_RPC_PATTERN
)
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
        self.__devices_from_config = []
        self.__first_scan_ready_event = asyncio.Event()
        self.__scanner_task = None
        self.__notify_task = None
        self.__scanner_poll_period = self.__config.get('scannerPollPeriod', 5000) / 1000
        self.__scanner_timeout = self.__config.get("scannerTimeout", 10000) / 1000
        self.name = self.__config.get("name", 'BLE Connector ' + ''.join(choice(ascii_lowercase) for _ in range(5)))
        self.__scanned_devices = {}
        self.__devices_tasks = []
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

        self.__stopped = False
        self.__connected = False
        self.__configure_and_load_devices()

    async def __scanner_loop(self):
        poll_period = self.__scanner_poll_period
        while not self.__stopped:
            start_time = time()
            try:
                scanned_devices = await BLEConnector.bleak_scanner.discover(timeout=self.__scanner_timeout,
                                                                            return_adv=True)
                self.__scanned_devices = scanned_devices

                if not self.__first_scan_ready_event.is_set():
                    self.__first_scan_ready_event.set()
                    self.__log.info("Initial device scanning completed")

            except Exception as e:
                self.__first_scan_ready_event.set()
                self.__log.error("Error during scanning: %s", str(e))
                self.__log.debug("An error occurred %s", e, exc_info=True)

            elapsed_time = time() - start_time
            sleep_time = max(0, poll_period - elapsed_time)
            await asyncio.sleep(sleep_time)

    async def __notify_user_on_scan_complete(self):
        while not self.__first_scan_ready_event.is_set():
            self.__log.info(
                "Waiting for the first scan to complete with the timeout of %s seconds...",
                self.__scanner_timeout)
            try:
                await asyncio.wait_for(self.__first_scan_ready_event.wait(), timeout=1.0)

            except asyncio.TimeoutError:
                pass

    async def __show_map(self):
        scanner = self.__config.get('scanner', {})
        devices = await BleakScanner(
            scanning_mode='passive' if self.__config.get('passiveScanMode', True) else 'active').discover(
            timeout=scanner.get('timeout', 10000) / 1000, return_adv=True)

        if scanner.get('deviceName'):
            found_devices = [x.__str__() for x in filter(lambda x: x.name == scanner['deviceName'], devices)]
            if found_devices:
                self.__log.info(', '.join(found_devices))
            else:
                self.__log.info('nothing to show')
        else:
            found_devices = {mac: device[0].name for mac, device in devices.items()}
            formatted_devices = dumps(found_devices, indent=4)
            self.__log.info("The mac:device pairs are: \n%s", formatted_devices)

    def __configure_and_load_devices(self):
        self.__devices_from_config = [
            Device({**device, 'callback': self.callback, 'connector_type': self._connector_type}, self.__log)
            for device in self.__config.get('devices', [])]

    def open(self):
        self.__stopped = False
        self.start()

    def callback(self, not_converted_data):
        self.__process_data_queue.put(not_converted_data)

    def run(self):
        self.__connected = True

        if self.__notify_task is None:
            self.__notify_task = self.__loop.create_task(self.__notify_user_on_scan_complete())

        if not hasattr(BLEConnector, "bleak_scanner") or BLEConnector.bleak_scanner is None:
            BLEConnector.bleak_scanner = BleakScanner(scanning_mode='active')
            self.__log.debug('Initialized new BleakScanner instance')

        self.__scanner_task = self.__loop.create_task(self.__scanner_loop())

        try:
            self.__loop.run_until_complete(
                asyncio.wait_for(self.__first_scan_ready_event.wait(), timeout=self.__scanner_timeout + 1.0)
            )
        except asyncio.TimeoutError:
            self.__log.warning("First scan did not finish in time.")

        except RuntimeError:
            self.__log.debug("The connector has been stopped before the first scan completed.")

        self.__devices_tasks = [
            self.__loop.create_task(device.run_client(scanned_devices_callback=self.get_scanned_devices_callback))
            for device in self.__devices_from_config
        ]

        Thread(target=self.__process_data, daemon=True,
               name='BLE Process Data Thread').start()

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
        for device in self.__devices_from_config:
            try:
                await device.client.disconnect()
            except Exception as e:
                self.__log.debug("An error occurred while disconnecting device %s: %s", device.name, e)

    def close(self):
        self.__log.info('Closing BLE connector...')
        self.__connected = False
        self.__stopped = True
        for device in self.__devices_from_config:
            device.stop()
        for task in self.__devices_tasks:
            self.__loop.call_soon_threadsafe(task.cancel)
        asyncio.run_coroutine_threadsafe(
            self.__disconnect_all_devices(), self.__loop)
        self.__loop.call_soon_threadsafe(self.__loop.stop)
        if self.__scanner_task:
            self.__loop.call_soon_threadsafe(self.__scanner_task.cancel)
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

            for attribute_update_config in device.config['attributeUpdates']:
                for attribute_update in content['data']:
                    if attribute_update_config['attributeOnThingsBoard'] == attribute_update:
                        coroutine = device.write_char(attribute_update_config['characteristicUUID'],
                                                      bytes(str(content['data'][attribute_update]), 'utf-8'))
                        self.__run_coroutine(coroutine, device.timeout)
        except Exception as e:
            self.__log.error('Error while processing attributes update %s', e)

    def __find_device_by_name(self, name):
        device = next((device for device in self.__devices_from_config if device.name == name), None)
        if device is None:
            raise ValueError(f"Device '{name}' not found")

        return device

    @CollectAllReceivedBytesStatistics(start_stat_type='allReceivedBytesFromTB')
    def server_side_rpc_handler(self, rpc_request) -> RPCResponse:
        self.__log.debug('Received RPC request: %s', rpc_request)

        response = RPCResponse(rpc_request.id, device=rpc_request.device_name)
        try:
            if rpc_request.rpc_type == RPCType.DEVICE:
                device = self.__find_device_by_name(rpc_request.device_name)
                rpc_config = self.__get_device_rpc_config(device, rpc_request.method_name, rpc_request.params)
            elif rpc_request.rpc_type == RPCType.RESERVED:
                device = self.__find_device_by_name(rpc_request.device_name)
                rpc_config = self.__get_reserved_rpc_config(rpc_request.method_name, rpc_request.params)
            elif rpc_request.rpc_type == RPCType.CONNECTOR:
                raise ValueError('Connector RPC is not supported by BLE connector')
            else:
                raise ValueError(f'Invalid RPC type request: {rpc_request}')

            coroutine = self.__process_rpc_request(device, **rpc_config)
            response.set_message(self.__run_coroutine(coroutine, rpc_request.timeout))
        except Exception as e:
            self.__log.error('Failed to process RPC request %s: %s', rpc_request, e)
            response.set_error_msg(f"Failed to process '{rpc_request.method_name}' RPC request: {e}")

        return response

    @staticmethod
    def __get_device_rpc_config(device, method_name, params):
        rpc_config = next((config for config in device.config['serverSideRpc']
                           if config['methodRPC'] == method_name), None)
        if rpc_config is None:
            raise ValueError(f"No configuration for '{method_name}' RPC request")

        return {'method_processing': rpc_config['methodProcessing'],
                'characteristic_uuid': rpc_config.get('characteristicUUID'),
                'value': params}

    @staticmethod
    def __get_reserved_rpc_config(method_name, params):
        if not params:
            raise ValueError("No 'params' found in reserved RPC request")

        is_set = method_name.lower() == 'set'
        match = fullmatch(RESERVED_SET_RPC_PATTERN if is_set else RESERVED_GET_RPC_PATTERN, params)
        if match is None:
            expected_schema = RESERVED_SET_RPC_SCHEMA if is_set else RESERVED_GET_RPC_SCHEMA
            raise ValueError(f"The requested RPC does not match with the schema: {expected_schema}")

        return {'method_processing': 'WRITE' if is_set else 'READ',
                'characteristic_uuid': match.group('characteristicUUID'),
                'value': match.group('value') if is_set else None}

    def __run_coroutine(self, coroutine, timeout):
        future = asyncio.run_coroutine_threadsafe(coroutine, self.__loop)
        try:
            return future.result(timeout)
        except FutureTimeoutError:
            future.cancel()
            raise TimeoutError(f'RPC request timeout ({timeout}s)')

    @staticmethod
    async def __process_rpc_request(device, method_processing, characteristic_uuid, value):
        if device.client is None or not device.client.is_connected:
            raise ConnectionError(f"Device '{device.name}' is not connected")

        method_processing = method_processing.upper()

        if method_processing == 'READ':
            data = await device.read_char(characteristic_uuid)
            if data is None:
                raise ValueError(f"Failed to read characteristic {characteristic_uuid}")

            return bytes(data).decode('utf-8')

        if method_processing == 'WRITE':
            result = await device.write_char(characteristic_uuid, bytes(str(value), 'utf-8'))
            if isinstance(result, Exception):
                raise result

            return result

        if method_processing == 'SCAN':
            return await device.scan_self(True)

        raise ValueError(f"Unsupported methodProcessing '{method_processing}'")

    def get_config(self):
        return self.__config

    def get_scanned_devices_callback(self):
        return self.__scanned_devices
