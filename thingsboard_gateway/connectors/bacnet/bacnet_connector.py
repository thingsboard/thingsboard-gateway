#     Copyright 2024. ThingsBoard
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
from asyncio import CancelledError
from queue import Queue, Empty
from threading import Thread
from string import ascii_lowercase
from random import choice
from time import monotonic, sleep
from typing import List

from thingsboard_gateway.connectors.connector import Connector
from thingsboard_gateway.gateway.constants import STATISTIC_MESSAGE_RECEIVED_PARAMETER, STATISTIC_MESSAGE_SENT_PARAMETER
from thingsboard_gateway.gateway.statistics.statistics_service import StatisticsService
from thingsboard_gateway.tb_utility.tb_logger import init_logger
from thingsboard_gateway.tb_utility.tb_utility import TBUtility

try:
    from bacpypes3.apdu import ErrorRejectAbortNack
except ImportError:
    print("bacpypes3 library not found")
    TBUtility.install_package("bacpypes3")
    from bacpypes3.apdu import ErrorRejectAbortNack

from bacpypes3.pdu import Address

from thingsboard_gateway.connectors.bacnet.device import Device
from thingsboard_gateway.connectors.bacnet.entities.device_object_config import DeviceObjectConfig
from thingsboard_gateway.connectors.bacnet.application import Application
from thingsboard_gateway.connectors.bacnet.backward_compatibility_adapter import BackwardCompatibilityAdapter


class AsyncBACnetConnector(Thread, Connector):
    PROCESS_DEVICE_QUEUE = Queue(-1)

    def __init__(self, gateway, config, connector_type):
        self.statistics = {STATISTIC_MESSAGE_RECEIVED_PARAMETER: 0,
                           STATISTIC_MESSAGE_SENT_PARAMETER: 0}
        self.__connector_type = connector_type
        super().__init__()
        self.__gateway = gateway
        self.__config = config
        self.name = config.get('name', 'BACnet ' + ''.join(choice(ascii_lowercase) for _ in range(5)))
        remote_logging = self.__config.get('enableRemoteLogging', False)
        log_level = self.__config.get('logLevel', "INFO")

        self.__log = init_logger(self.__gateway, self.name, log_level,
                                 enable_remote_logging=remote_logging,
                                 is_connector_logger=True)
        self.__converter_log = init_logger(self.__gateway, self.name + '_converter', log_level,
                                           enable_remote_logging=remote_logging,
                                           is_converter_logger=True, attr_name=self.name)
        self.__log.info('Starting BACnet connector...')

        if BackwardCompatibilityAdapter.is_old_config(config):
            backward_compatibility_adapter = BackwardCompatibilityAdapter(config, self.__log)
            self.__config = backward_compatibility_adapter.convert()

        self.__id = self.__config.get('id')
        self.daemon = True
        self.__stopped = False
        self.__connected = False

        self.__application = None

        self.__data_to_convert_queue = Queue(-1)
        self.__data_to_save_queue = Queue(-1)

        try:
            self.loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self.loop)
        except RuntimeError:
            self.loop = asyncio.get_event_loop()

        self.__devices: List[Device] = []
        self.__devices_discover_period = 30
        self.__previous_discover_time = 0

    def open(self):
        self.start()

    def run(self):
        self.__connected = True

        try:
            self.loop.run_until_complete(self.__start())
        except CancelledError as e:
            self.__log.debug('Task was cancelled due to connector stop: %s', e.__str__())
        except Exception as e:
            self.__log.exception(e)

    async def __start(self):
        self.__application = Application(DeviceObjectConfig(
            self.__config['application']), self.indication_callback, self.__log)

        self.__devices_discover_period = self.__config.get('devicesDiscoverPeriodSeconds', 30)
        await self.__discover_devices()
        await asyncio.gather(self.__main_loop(), self.__convert_data(), self.__save_data())

    def indication_callback(self, apdu):
        """
        First check if device is already added
        If added, set active to True
        If not added, check if device is in config
        """

        try:
            device_address = apdu.pduSource.exploded
            self.__log.info('Received APDU, from %s, trying to find device...', device_address)
            added_device = self.__find_device_by_address(device_address)
            if added_device is None:
                device_config = Device.find_self_in_config(self.__config['devices'], apdu)
                if device_config:
                    device = Device(self.connector_type, device_config, apdu, self.callback, self.__converter_log)
                    self.__devices.append(device)
                    self.__gateway.add_device(device.device_info.device_name,
                                              {"connector": self},
                                              device_type=device.device_info.device_type)
                    self.__log.info('Device %s found', device)
                else:
                    self.__log.debug('Device %s not found in config', device_address)
            else:
                added_device.active = True
                self.__log.debug('Device %s already added', added_device)
        except Exception as e:
            self.__log.error('Error processing indication callback: %s', e)

    def __find_device_by_address(self, address):
        for device in self.__devices:
            if device.details.address == address or address in device.alternative_responses_addresses:
                return device

    def __find_device_by_name(self, name):
        device_filter = list(filter(lambda x: x.device_info.device_name == name, self.__devices))
        if len(device_filter):
            return device_filter[0]

    async def __discover_devices(self):
        self.__previous_discover_time = monotonic()
        self.__log.info('Discovering devices...')
        for device_config in self.__config.get('devices', []):
            try:
                DeviceObjectConfig.update_address_in_config_util(device_config)
                await self.__application.do_who_is(device_address=device_config['address'])
                self.__log.debug('WhoIs request sent to device %s', device_config['address'])
            except Exception as e:
                self.__log.error('Error discovering device %s: %s', device_config['address'], e)

    async def __main_loop(self):
        while not self.__stopped:
            try:
                if monotonic() - self.__previous_discover_time >= self.__devices_discover_period:
                    await self.__discover_devices()
            except Exception as e:
                self.__log.error('Error in main loop during discovering devices: %s', e)
                await asyncio.sleep(1)
            try:
                device: Device = self.PROCESS_DEVICE_QUEUE.get_nowait()

                results = []
                for object_to_read in device.uplink_converter_config.objects_to_read:
                    result = await self.__read_property(Address(device.details.address),
                                                        Device.get_object_id(object_to_read),
                                                        object_to_read['propertyId'])
                    results.append(result)

                # TODO: Add handling for device activity/inactivity

                self.__log.trace('%s reading results: %s', device, results)
                self.__data_to_convert_queue.put_nowait((device, results))
            except Empty:
                await asyncio.sleep(.01)
            except Exception as e:
                self.__log.error('Error processing device requests: %s', e)

    async def __read_property(self, address, object_id, property_id):
        try:
            result = await self.__application.read_property(address, object_id, property_id)
            return result
        except ErrorRejectAbortNack as err:
            return Exception(err)
        except Exception as e:
            self.__log.error('Error reading property %s:%s from device %s: %s', object_id, property_id, address, e)

    async def __write_property(self, address, object_id, property_id, value):
        try:
            result = await self.__application.write_property(address, object_id, property_id, value)
            return result
        except ErrorRejectAbortNack as err:
            return err
        except Exception as e:
            self.__log.error('Error writing property %s:%s to device %s: %s', object_id, property_id, address, e)

    async def __convert_data(self):
        while not self.__stopped:
            try:
                device, values = self.__data_to_convert_queue.get_nowait()
                self.__log.trace('%s data to convert: %s', device, values)
                converted_data = device.uplink_converter.convert(values)
                self.__data_to_save_queue.put_nowait((device, converted_data))
            except Empty:
                await asyncio.sleep(.01)
            except Exception as e:
                self.__log.error('Error converting data: %s', e)

    async def __save_data(self):
        while not self.__stopped:
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

    @classmethod
    def callback(cls, device: Device):
        cls.PROCESS_DEVICE_QUEUE.put_nowait(device)

    def close(self):
        self.__log.info('Stopping BACnet connector...')
        self.__connected = False
        self.__stopped = True

        self.__stop_devices()

        if self.__application:
            self.__application.close()

        asyncio.run_coroutine_threadsafe(self.__cancel_all_tasks(), self.loop)

        self.__check_is_alive()

        self.__log.info('BACnet connector stopped')
        self.__log.stop()

    def __stop_devices(self):
        for device in self.__devices:
            device.stop()

        if self.__application:
            self.__application.close()

    def __check_is_alive(self):
        start_time = monotonic()

        while self.is_alive():
            if monotonic() - start_time > 10:
                self.__log.error("Failed to stop connector %s", self.get_name())
                break
            sleep(.1)

    async def __cancel_all_tasks(self):
        await asyncio.sleep(5)
        for task in asyncio.all_tasks(self.loop):
            task.cancel()

    def __create_task(self, func, args, kwargs):
        task = self.loop.create_task(func(*args, **kwargs))

        while not task.done():
            sleep(.02)

    def on_attributes_update(self, content):
        try:
            self.__log.debug('Received Attribute Update request: %r', content)

            device = self.__find_device_by_name(content['device'])
            if device is None:
                self.__log.error('Device %s not found', content['device'])
                return

            for attribute_name, value in content.get('data', {}).items():
                attribute_update_config_filter = list(filter(lambda x: x['key'] == attribute_name,
                                                             device.attributes_updates))
                for attribute_update_config in attribute_update_config_filter:
                    try:
                        object_id = Device.get_object_id(attribute_update_config)
                        result = {}
                        self.__create_task(self.__process_attribute_update,
                                           (Address(device.details.address),
                                            object_id,
                                            attribute_update_config['propertyId'],
                                            value),
                                           {'result': result})
                        self.__log.info('Processed attribute update with result: %r', result)
                    except Exception as e:
                        self.__log.error('Error updating attribute %s: %s', attribute_name, e)
        except Exception as e:
            self.__log.error('Error processing attribute update%s with error: %s', content, e)

    async def __process_attribute_update(self, address, object_id, property_id, value, result={}):
        result['response'] = await self.__write_property(address, object_id, property_id, value)

    def server_side_rpc_handler(self, content):
        self.__log.debug('Received RPC request: %r', content)

        device = self.__find_device_by_name(content['device'])
        if device is None:
            self.__log.error('Device %s not found', content['device'])
            return

        rpc_method_name = content.get('data', {}).get('method')
        if rpc_method_name is None:
            self.__log.error('Method name not found in RPC request: %r', content)
            return

        for rpc_config in device.server_side_rpc:
            if rpc_config['method'] == rpc_method_name:
                try:
                    object_id = Device.get_object_id(rpc_config)
                    result = {}
                    value = content.get('data', {}).get('params')
                    self.__create_task(self.__process_rpc_request,
                                       (Address(device.details.address),
                                        object_id,
                                        rpc_config['propertyId']),
                                       {'value': value, 'result': result})
                    self.__log.info('Processed RPC request with result: %r', result)
                    self.__gateway.send_rpc_reply(device=device.device_info.device_name,
                                                  req_id=content['data'].get('id'),
                                                  content=str(result))
                except Exception as e:
                    self.__log.error('Error processing RPC request %s: %s', rpc_method_name, e)
                    self.__gateway.send_rpc_reply(device=device.device_info.device_name,
                                                  req_id=content['data'].get('id'),
                                                  content={rpc_method_name: str(e)},
                                                  success_sent=False)

    async def __process_rpc_request(self, address, object_id, property_id, value=None, result={}):
        if value is None:
            result['response'] = await self.__read_property(address, object_id, property_id)
        else:
            result['response'] = await self.__write_property(address, object_id, property_id, value)

    def get_id(self):
        return self.__id

    def get_name(self):
        return self.name

    def get_type(self):
        return self.__connector_type

    @property
    def connector_type(self):
        return self.__connector_type

    def get_config(self):
        return self.__config

    def is_connected(self):
        return self.__connected

    def is_stopped(self):
        return self.__stopped
