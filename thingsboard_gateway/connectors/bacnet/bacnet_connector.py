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
from asyncio import Queue, CancelledError, QueueEmpty
from threading import Thread
from string import ascii_lowercase
from random import choice
from time import monotonic, sleep
from typing import TYPE_CHECKING
from copy import deepcopy

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

from thingsboard_gateway.connectors.bacnet.device import Device, Devices
from thingsboard_gateway.connectors.bacnet.entities.device_object_config import DeviceObjectConfig
from thingsboard_gateway.connectors.bacnet.application import Application
from thingsboard_gateway.connectors.bacnet.backward_compatibility_adapter import BackwardCompatibilityAdapter

if TYPE_CHECKING:
    from thingsboard_gateway.gateway.tb_gateway_service import TBGatewayService


class AsyncBACnetConnector(Thread, Connector):
    def __init__(self, gateway, config, connector_type):
        self.statistics = {STATISTIC_MESSAGE_RECEIVED_PARAMETER: 0,
                           STATISTIC_MESSAGE_SENT_PARAMETER: 0}
        self.__connector_type = connector_type
        super().__init__()
        self.__gateway: 'TBGatewayService' = gateway
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

        self.__process_device_queue = Queue(1_000_000)
        self.__data_to_convert_queue = Queue(1_000_000)
        self.__data_to_save_queue = Queue(1_000_000)
        self.__indication_queue = Queue(1_000_000)

        try:
            self.loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self.loop)
        except RuntimeError:
            self.loop = asyncio.get_event_loop()

        self.loop.set_exception_handler(self.exception_handler)

        self.__devices = Devices()
        self.__devices_discover_period = self.__config.get('devicesDiscoverPeriodSeconds', 30)
        self.__previous_discover_time = 0

    def exception_handler(self, _, context):
        if context.get('exception') is not None:
            if str(context['exception']) == 'invalid state transition from COMPLETED to AWAIT_CONFIRMATION':
                pass

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
            self.__config['application']), self.__handle_indication, self.__log)

        await self.__discover_devices()
        await asyncio.gather(self.__main_loop(),
                             self.__convert_data(),
                             self.__save_data(),
                             self.indication_callback(),
                             self.__application.confirmation_handler())

    def __handle_indication(self, apdu):
        self.__indication_queue.put_nowait(apdu)

    async def indication_callback(self):
        """
        First check if device is already added
        If added, set active to True
        If not added, check if device is in config
        """

        while not self.__stopped:
            try:
                apdu = self.__indication_queue.get_nowait()

                device_address = apdu.pduSource.__str__()
                self.__log.info('Received APDU, from %s, trying to find device...', device_address)

                added_device = await self.__devices.get_device_by_id(apdu.iAmDeviceIdentifier[1])
                if added_device is None:
                    self.__log.debug('Device %s not found in devices list', device_address)
                    device_config = Device.find_self_in_config(self.__config['devices'], apdu)
                    if device_config:
                        self.__log.debug('Device %s found in config. Adding...', device_address)
                        self.loop.create_task(self.__add_device(apdu, device_config))
                    else:
                        self.__log.debug('Device %s not found in config', device_address)
                else:
                    added_device.active = True
                    self.__log.debug('Device %s already added', added_device)
            except QueueEmpty:
                await asyncio.sleep(.1)
            except Exception as e:
                self.__log.error('Error processing indication callback: %s', e)

    async def __add_device(self, apdu, device_config):
        if Device.need_to_retrieve_device_name(device_config):
            apdu.deviceName = None
            device_name = await self.__application.get_device_name(apdu)

            if device_name is not None:
                apdu.deviceName = device_name

        device = Device(self.connector_type,
                        device_config,
                        apdu,
                        self.__process_device_queue,
                        self.__converter_log)

        await self.__devices.add(device)
        self.__gateway.add_device(device.device_info.device_name,
                                  {"connector": self},
                                  device_type=device.device_info.device_type)
        self.__log.info('Device %s connected to platform', device.device_info.device_name)

        self.loop.create_task(device.run())
        self.__log.debug('Device %s started', device)

        self.__log.debug('Checking device %s configuration...', device.device_info.device_name)
        await self.__check_and_update_device_config(device, device_config)
        self.__log.debug('Checked device %s configuration.', device.device_info.device_name)

    async def __check_and_update_device_config(self, device, device_config):
        new_config = deepcopy(device_config)

        discover_for = Device.is_discovery_config(device_config)
        if len(discover_for):
            self.__log.debug('Discovering %s device objects...', device.details.address)
            discovering_started = monotonic()

            iter = await self.__application.get_device_objects(device)
            if iter is not None:
                all_done = False
                while not self.__stopped and not all_done:
                    objects, _, all_done = await iter.get_next()
                    config = self.from_object_to_config(device, objects)
                    for section in discover_for:
                        if isinstance(new_config[section], str):
                            new_config[section] = []

                        new_config[section].extend(config)

                    device.config = new_config

            self.__log.debug('Device %s objects discovered, discovering took (sec): %r',
                             device.details.address,
                             monotonic() - discovering_started)

    def from_object_to_config(self, device, object_list):
        config = []
        for obj in object_list:
            try:
                obj_id, _, _, key = obj
                config.append({
                    'objectType': obj_id[0].__str__(),
                    'objectId': obj_id[-1],
                    'propertyId': 'presentValue',
                    'key': key
                })
            except Exception as e:
                self.__log.error('Error converting object to config for device %s: %s', device, e)
                continue

        return config

    async def __discover_devices(self):
        self.__previous_discover_time = monotonic()
        self.__log.info('Discovering devices...')
        for device_config in self.__config.get('devices', []):
            if self.__stopped:
                break

            try:
                DeviceObjectConfig.update_address_in_config_util(device_config)

                who_is_address = Device.get_who_is_address(device_config['address'])
                if who_is_address is None:
                    self.__log.error('Invalid address %s', device_config['address'])
                    continue

                await self.__application.do_who_is(device_address=who_is_address)
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
                device: Device = self.__process_device_queue.get_nowait()
                if device.stopped:
                    self.__log.trace('Device %s stopped', device)
                    continue

                self.loop.create_task(self.__read_multiple_properties(device))
                # TODO: Add handling for device activity/inactivity
            except QueueEmpty:
                await asyncio.sleep(.1)
            except Exception as e:
                self.__log.error('Error processing device requests: %s', e)

    async def __read_multiple_properties(self, device):
        reading_started = monotonic()

        iter = await self.__application.get_device_values(device)
        if iter is not None:
            all_done = False
            while not self.__stopped and not all_done:
                results, config, all_done = await iter.get_next()
                if len(results) > 0:
                    self.__log.trace('%s reading results: %s', device, results)
                    self.__data_to_convert_queue.put_nowait((device, zip(config, [result[-1] for result in results])))

        reading_ended = monotonic()
        current_reading_time = reading_ended - reading_started

        if current_reading_time > device.poll_period:
            device.poll_period = current_reading_time
        elif current_reading_time < device.poll_period:
            if current_reading_time < device.original_poll_period:
                device.poll_period = device.original_poll_period
            else:
                device.poll_period = device.original_poll_period

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
            await self.__application.write_property(address, object_id, property_id, value)
            return "ok"
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
            except QueueEmpty:
                await asyncio.sleep(.1)
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
            except QueueEmpty:
                await asyncio.sleep(.01)
            except Exception as e:
                self.__log.error('Error saving data: %s', e)

    def close(self):
        self.__log.info('Stopping BACnet connector...')
        self.__connected = False
        self.__stopped = True

        self.__devices.stop_all()

        if self.__application:
            self.__application.close()

        asyncio.run_coroutine_threadsafe(self.__cancel_all_tasks(), self.loop)

        self.__check_is_alive()

        self.__log.info('BACnet connector stopped')
        self.__log.stop()

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

            future = asyncio.run_coroutine_threadsafe(self.__devices.get_device_by_name(content['device']), self.loop)

            try:
                device = future.result(10)
            except asyncio.TimeoutError:
                self.__log.error('Timeout while waiting for device %s', content['device'])
                future.cancel()
            except Exception:
                device = None

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

        future = asyncio.run_coroutine_threadsafe(self.__devices.get_device_by_name(content['device']), self.loop)

        try:
            device = future.result(10)
        except asyncio.TimeoutError:
            self.__log.error('Timeout while waiting for device %s', content['device'])
            future.cancel()
        except Exception:
            device = None

        if device is None:
            self.__log.error('Device %s not found', content['device'])
            return

        rpc_method_name = content.get('data', {}).get('method')
        if rpc_method_name is None:
            self.__log.error('Method name not found in RPC request: %r', content)
            return

        # check if RPC method is reserved get/set
        self.__check_and_process_reserved_rpc(rpc_method_name, device, content)

        for rpc_config in device.server_side_rpc:
            if rpc_config['method'] == rpc_method_name:
                self.__process_rpc(rpc_method_name, rpc_config, content, device)

    def __process_rpc(self, rpc_method_name, rpc_config, content, device):
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
                                          content={'result': str(result.get('response'))},)
        except Exception as e:
            self.__log.error(
                'Error processing RPC request %s: %s', rpc_method_name, e)
            self.__gateway.send_rpc_reply(device=device.device_info.device_name,
                                          req_id=content['data'].get('id'),
                                          content={'result': str(e)},
                                          success_sent=False)

    async def __process_rpc_request(self, address, object_id, property_id, value=None, result={}):
        if value is None:
            result['response'] = await self.__read_property(address, object_id, property_id)
        else:
            result['response'] = await self.__write_property(address, object_id, property_id, value)

    def __check_and_process_reserved_rpc(self, rpc_method_name, device, content):
        if rpc_method_name in ('get', 'set'):
            params = {}
            for param in content['data']['params'].split(';'):
                try:
                    (key, value) = param.split('=')
                except ValueError:
                    continue

                if key and value:
                    params[key] = value

            if rpc_method_name == 'get':
                params['requestType'] = 'readProperty'
                content['data'].pop('params')
            elif rpc_method_name == 'set':
                params['requestType'] = 'writeProperty'
                content['data'].pop('params')
                content['data']['params'] = params['value']

            self.__process_rpc(rpc_method_name, params, content, device)

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
