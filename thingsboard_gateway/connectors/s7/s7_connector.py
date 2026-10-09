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

from time import monotonic, sleep
import re
import asyncio
from random import choice
from string import ascii_lowercase
from threading import Thread
from packaging import version

from thingsboard_gateway.connectors.connector import Connector
from thingsboard_gateway.gateway.entities.rpc_request import RPCType
from thingsboard_gateway.gateway.entities.rpc_response import RPCResponse
from thingsboard_gateway.connectors.s7.constants import (
    RESERVED_GET_RPC_SCHEMA,
    RESERVED_SET_RPC_SCHEMA,
    RESERVED_GET_TAG_RPC_SCHEMA,
    RESERVED_SET_TAG_RPC_SCHEMA,
    RESERVED_GET_TAG_RPC_PATTERN,
    RESERVED_SET_TAG_RPC_PATTERN,
    RESERVED_GET_DATA_RPC_SCHEMA,
    RESERVED_SET_DATA_RPC_SCHEMA,
    RESERVED_GET_DATA_RPC_PATTERN,
    RESERVED_SET_DATA_RPC_PATTERN,
    RESERVED_GET_VM_RPC_SCHEMA,
    RESERVED_SET_VM_RPC_SCHEMA,
    RESERVED_GET_VM_RPC_PATTERN,
    RESERVED_SET_VM_RPC_PATTERN,
    RESERVED_RPC_TYPE_PATTERN,
)
from thingsboard_gateway.gateway.constants import (
    STATISTIC_MESSAGE_RECEIVED_PARAMETER,
    STATISTIC_MESSAGE_SENT_PARAMETER,
)
from thingsboard_gateway.gateway.tb_gateway_service import TBGatewayService
from thingsboard_gateway.tb_utility.tb_logger import init_logger
from thingsboard_gateway.tb_utility.tb_utility import TBUtility
from thingsboard_gateway.gateway.statistics.statistics_service import StatisticsService

installation_required = False
required_version = '3.1.2'
force_install = False

try:
    from snap7 import __version__ as s7_version

    if version.parse(s7_version) != version.parse(required_version):
        installation_required = True
        force_install = True
except ImportError:
    installation_required = True

if installation_required:
    print('S7 library not found - installing...')
    TBUtility.install_package(
        'python-snap7', required_version, force_install=force_install
    )

from thingsboard_gateway.connectors.s7.entities.device import Device  # noqa: E402
from thingsboard_gateway.connectors.s7.entities.device_configs import (  # noqa: E402
    DeviceConfigValidationError,
)


class S7Connector(Thread, Connector):
    _RESERVED_RPC_TYPE_PATTERN = re.compile(RESERVED_RPC_TYPE_PATTERN)

    _RESERVED_RPC_PATTERNS_BY_TYPE_AND_METHOD = {
        ('tag', 'get'): re.compile(RESERVED_GET_TAG_RPC_PATTERN),
        ('tag', 'set'): re.compile(RESERVED_SET_TAG_RPC_PATTERN),
        ('data', 'get'): re.compile(RESERVED_GET_DATA_RPC_PATTERN),
        ('data', 'set'): re.compile(RESERVED_SET_DATA_RPC_PATTERN),
        ('vm', 'get'): re.compile(RESERVED_GET_VM_RPC_PATTERN),
        ('vm', 'set'): re.compile(RESERVED_SET_VM_RPC_PATTERN),
    }

    _RESERVED_RPC_SCHEMAS_BY_TYPE_AND_METHOD = {
            ('tag', 'get'): RESERVED_GET_TAG_RPC_SCHEMA,
            ('tag', 'set'): RESERVED_SET_TAG_RPC_SCHEMA,
            ('data', 'get'): RESERVED_GET_DATA_RPC_SCHEMA,
            ('data', 'set'): RESERVED_SET_DATA_RPC_SCHEMA,
            ('vm', 'get'): RESERVED_GET_VM_RPC_SCHEMA,
            ('vm', 'set'): RESERVED_SET_VM_RPC_SCHEMA,
        }

    def __init__(self, gateway, config, connector_type) -> None:
        self.statistics = {
            STATISTIC_MESSAGE_RECEIVED_PARAMETER: 0,
            STATISTIC_MESSAGE_SENT_PARAMETER: 0,
        }
        self.__connector_type = connector_type
        super().__init__()
        self.__gateway: TBGatewayService = gateway
        self.__config = config
        self.name = config.get(
            'name', 'S7 ' + ''.join(choice(ascii_lowercase) for _ in range(5))
        )
        remote_logging = self.__config.get('enableRemoteLogging', False)
        log_level = self.__config.get('logLevel', 'INFO')

        self.__log = init_logger(
            self.__gateway,
            self.name,
            log_level,
            enable_remote_logging=remote_logging,
            is_connector_logger=True,
        )
        self.__converter_log = init_logger(
            self.__gateway,
            self.name + '_converter',
            log_level,
            enable_remote_logging=remote_logging,
            is_converter_logger=True,
            attr_name=self.name,
        )
        self.__log.info('Starting S7 connector...')

        self.__id = self.__config.get('id')
        self.daemon = True
        self.__stopped = False
        self.__connected = False

        self.__process_device_queue = asyncio.Queue(1_000_000)
        self.__data_to_convert_queue = asyncio.Queue(1_000_000)
        self.__data_to_save_queue = asyncio.Queue(1_000_000)

        self._devices = []

        try:
            self.loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self.loop)
        except RuntimeError:
            self.loop = asyncio.get_event_loop()

        self.loop.set_exception_handler(self.exception_handler)

    def exception_handler(self, _, context):
        if context.get('exception') is not None:
            self.__log.exception('handled exception',
                                 exc_info=context['exception'])

    def open(self):
        self.start()

    def run(self):
        self.__connected = True

        try:
            self.loop.run_until_complete(self._run())
        except asyncio.CancelledError as e:
            self.__log.debug(
                'Task was cancelled due to connector stop: %s', e.__str__()
            )
        except Exception as e:
            self.__log.exception(e)

    async def _run(self):
        await self._load_devices()
        await self._connect_to_devices()

        await asyncio.gather(
            self._run_devices(),
            self._read_data_from_devices(),
            self._convert_data(),
            self._save_data(),
        )

    async def _load_devices(self) -> None:
        if len(self.__config.get('devices', [])) == 0:
            self.__log.error('Device list is empty.')
            return

        for device_config in self.__config['devices']:
            try:
                device = Device.create_device_from_config(
                    self.__log, self.__converter_log, device_config, self.__process_device_queue,
                    self.__delete_device_from_platform)
                self._devices.append(device)
            except DeviceConfigValidationError as e:
                self.__log.error(
                    'Error creating %s device: %s', device_config, e)

    async def _connect_to_devices(self) -> None:
        tasks = [device.connect() for device in self._devices]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        for result in results:
            if isinstance(result, Exception):
                self.__log.error('Error connecting to device: %s', result)

    async def _run_devices(self) -> None:
        tasks = [device.run() for device in self._devices]
        await asyncio.gather(*tasks, return_exceptions=True)

    async def _read_data_from_devices(self) -> None:
        while not self.__stopped:
            try:
                device: Device = self.__process_device_queue.get_nowait()
                if device.stopped:
                    self.__log.trace(
                        'Device %s is stopped, skipping read.', device.config.device_name)
                    continue

                results = await device.read_configured_data()

                if len(results) <= 0:
                    self.__log.trace(
                        'No data read from device %s.', device.config.device_name)
                    continue

                self.__data_to_convert_queue.put_nowait((device, results))
            except asyncio.QueueEmpty:
                await asyncio.sleep(0.1)
            except Exception as e:
                self.__log.exception('Error processing device request: %s', e)

    async def _convert_data(self):
        while not self.__stopped:
            try:
                device, values = self.__data_to_convert_queue.get_nowait()
                self.__log.trace('%s data to convert: %s',
                                 device.config.device_name, values)

                converted_data = device.uplink_converter.convert(values)
                self.__data_to_save_queue.put_nowait((device, converted_data))
            except asyncio.QueueEmpty:
                await asyncio.sleep(0.1)
            except Exception as e:
                self.__log.exception('Error converting data: %s', e)

    async def _save_data(self):
        while not self.__stopped:
            try:
                device, data_to_save = self.__data_to_save_queue.get_nowait()
                self.__log.trace('%s data to save: %s',
                                 device.config.device_name, data_to_save)
                StatisticsService.count_connector_message(
                    self.get_name(), stat_parameter_name='storageMsgPushed')
                self.__gateway.send_to_storage(
                    self.get_name(), self.get_id(), data_to_save)
                self.statistics[STATISTIC_MESSAGE_SENT_PARAMETER] += 1
            except asyncio.QueueEmpty:
                await asyncio.sleep(0.1)
            except Exception as e:
                self.__log.exception('Error saving data: %s', e)

    def close(self):
        self.__log.info('Stopping S7 connector...')
        self.__connected = False
        self.__stopped = True

        self._stop_devices()

        asyncio.run_coroutine_threadsafe(self.__cancel_all_tasks(), self.loop)

        self.__check_is_alive()

        self.__log.info('S7 connector stopped')
        self.__log.stop()

    def _stop_devices(self):
        for device in self._devices:
            device.stop()

    def __check_is_alive(self):
        start_time = monotonic()

        while self.is_alive():
            if monotonic() - start_time > 10:
                self.__log.error(
                    "Failed to stop connector %s", self.get_name())
                break
            sleep(.1)

    async def __cancel_all_tasks(self):
        await asyncio.sleep(5)
        for task in asyncio.all_tasks(self.loop):
            task.cancel()

    def on_attributes_update(self, content):
        try:
            self.__log.debug('Received attribute update request: %r', content)

            device_name = content.get('device')
            device = self._get_device_by_name(device_name)
            if device is None:
                self.__log.error('Device %s not found', content['device'])
                return

            if not device.config.attributes_updates:
                self.__log.error("No attribute mapping found for device %s", device.config.device_name)
                return

            filtered_attribute_updates_section_from_config = [
                update_config
                for update_config in device.config.attributes_updates
                if update_config["key"] in content.get("data", {})
            ]
            self._process_attribute_update(filtered_attribute_updates_section_from_config, content, device)
        except Exception as e:
            self.__log.exception('Error processing attribute update %r: %s', content, e)

    def _process_attribute_update(self, attribute_update_config_list, content, device):
        for attribute_update_config in attribute_update_config_list:
            try:
                data_section = content.get('data', {})
                value = data_section.get(attribute_update_config['key'])
                if value is None:
                    self.__log.error("Value for attribute '%s' not found in update request for device '%s'",
                                     attribute_update_config['key'], device.config.device_name)
                    continue

                converted_value = device.downlink_converter.convert(attribute_update_config, value)
                result = device.write(attribute_update_config, converted_value)
                if result != 0:
                    self.__log.error("Failed to write value '%s' to device '%s' for attribute '%s'",
                                     converted_value, device.config.device_name, attribute_update_config['key'])
                else:
                    self.__log.debug("Successfully processed attribute update for key %s",
                                     attribute_update_config['key'])
            except Exception as e:
                self.__log.exception(
                    "Failed to process attribute update for device '%s' attribute '%s': %s",
                    device.config.device_name, attribute_update_config['key'], e)
                continue

    def server_side_rpc_handler(self, rpc_request) -> RPCResponse:
        self.__log.debug('Received RPC request: %s', rpc_request)

        try:
            device = self._get_device_by_name(rpc_request.device_name)
            if device is None:
                error_msg = f"Device with name {rpc_request.device_name} not found for RPC request: {rpc_request}"
                response = RPCResponse(rpc_request.id, device=rpc_request.device_name)
                response.set_error_msg(error_msg)
                return response

            if rpc_request.rpc_type == RPCType.DEVICE:
                return self._process_rpc_to_device(rpc_request, device)
            elif rpc_request.rpc_type == RPCType.RESERVED:
                return self._process_reserved_rpc(rpc_request, device)
            elif rpc_request.rpc_type == RPCType.CONNECTOR:
                return self._process_rpc(rpc_request.method_name,
                                         rpc_request.params,
                                         device,
                                         value=rpc_request.value,
                                         req_id=rpc_request.id)
            else:
                response = RPCResponse(rpc_request.id, device=rpc_request.device_name)
                response.set_error_msg(f"Invalid RPC type request: {rpc_request}")
                return response
        except Exception as e:
            error_msg = f"Error processing RPC request {rpc_request}: {e}"
            response = RPCResponse(rpc_request.id, device=rpc_request.device_name)
            response.set_error_msg(error_msg)
            return response

    def _process_rpc_to_device(self, rpc_request, device):
        filtered_rpc_section_from_config = [rpc_config for rpc_config in device.config.server_side_rpc if
                                            rpc_config['method'] == rpc_request.method_name]
        if not filtered_rpc_section_from_config:
            error_msg = f"Neither of configured device rpc methods match with {rpc_request.method_name}"
            response = RPCResponse(rpc_request.id, device=device.config.device_name)
            response.set_error_msg(error_msg)
            return response

        for rpc_config in filtered_rpc_section_from_config:
            return self._process_rpc(rpc_request.method_name,
                                     rpc_config,
                                     device,
                                     value=rpc_request.params,
                                     req_id=rpc_request.id)

    def _process_reserved_rpc(self, rpc_request, device):
        if not rpc_request.params:
            err_msg = f"No 'params' found in reserved RPC request '{rpc_request.method_name}'"
            response = RPCResponse(rpc_request.id, device=device.config.device_name)
            response.set_error_msg(err_msg)
            return response

        type_match = self._RESERVED_RPC_TYPE_PATTERN.match(rpc_request.params)
        if type_match is None:
            expected_schema = self._get_reserved_rpc_mismatch_schema(rpc_request.method_name)
            error_msg = f"The requested RPC does not match with the schema: {expected_schema}"
            self.__log.error(error_msg)
            response = RPCResponse(rpc_request.id, device=device.config.device_name)
            response.set_error_msg(error_msg)
            return response

        address_type = type_match.group('type')
        pattern = self._RESERVED_RPC_PATTERNS_BY_TYPE_AND_METHOD[(address_type, rpc_request.method_name)]

        match = pattern.match(rpc_request.params)
        if match is None:
            expected_schema = self._get_reserved_rpc_mismatch_schema(rpc_request.method_name, address_type=address_type)
            error_msg = f"The requested RPC does not match with the schema: {expected_schema}"
            self.__log.error(err_msg)
            response = RPCResponse(rpc_request.id, device=device.config.device_name)
            response.set_error_msg(error_msg)
            return response

        rpc_config = self._build_reserved_rpc_config(address_type, rpc_request.method_name, match.groupdict())

        return self._process_rpc(rpc_request.method_name,
                                 rpc_config,
                                 device,
                                 req_id=rpc_request.id,
                                 value=match.group('value') if rpc_request.method_name == 'set' else None)

    def _get_device_by_name(self, device_name):
        for device in self._devices:
            if device.config.device_name == device_name:
                return device
        return None

    def __delete_device_from_platform(self, device):
        if device.config.device_name in self.__gateway.get_devices(connector_id=self.get_id()):
            self.__log.warning('Device %s is disconnected, removing it from the platform.',
                               device.config.device_name)
            self.__gateway.del_device(device.config.device_name)

    def get_device_shared_attributes_keys(self, device_name):
        device = self._get_device_by_name(device_name)
        if device is not None:
            return device.config.shared_attributes_keys
        return []

    def _process_rpc(self, rpc_method_name, rpc_config, device, req_id, value=None):
        response = RPCResponse(req_id, device=device.config.device_name)
        if rpc_config.get('requestType') == 'write':
            msg, ok = self._process_write_rpc(rpc_method_name, rpc_config, device, value)
            if not ok:
                response.set_error_msg(msg)
                response.is_success = False
            else:
                response.set_message(msg)
        elif rpc_config.get('requestType') == 'read':
            result = self._process_read_rpc(rpc_config, device)
            response.set_message(result)
        else:
            error_msg = f"Unsupported requestType {rpc_config.get('requestType')} for RPC method {rpc_method_name}"
            response.set_error_msg(error_msg)
            response.is_success = False

        return response

    def _get_reserved_rpc_mismatch_schema(self, rpc_method_name, address_type=None):
        if address_type is None:
            expected_schema = RESERVED_SET_RPC_SCHEMA if rpc_method_name == 'set' else RESERVED_GET_RPC_SCHEMA
        else:
            expected_schema = self._RESERVED_RPC_SCHEMAS_BY_TYPE_AND_METHOD[(address_type, rpc_method_name)]

        return expected_schema

    @staticmethod
    def _build_reserved_rpc_config(address_type, rpc_method_name, groups):
        rpc_config = {'type': address_type, 'requestType': 'write' if rpc_method_name == 'set' else 'read'}

        if address_type == 'tag':
            rpc_config['tag'] = groups['tag']
        elif address_type == 'vm':
            rpc_config['vmAddress'] = groups['vmAddress']
        elif address_type == 'data':
            rpc_config['dbNumber'] = int(groups['dbNumber'])
            rpc_config['start'] = int(groups['start'])
            rpc_config['dataType'] = groups['dataType']
            if groups.get('size') is not None:
                rpc_config['size'] = int(groups['size'])
            if groups.get('bit') is not None:
                rpc_config['bit'] = int(groups['bit'])
        else:
            raise ValueError(f"Reserved RPC 'type' must be 'tag', 'data' or 'vm', but got '{address_type}'")

        return rpc_config

    def _process_write_rpc(self, rpc_method_name, rpc_config, device, value):
        if value is None:
            error_msg = f"No 'params' found in RPC request for method {rpc_method_name}"
            self.__log.error(error_msg)
            return error_msg, False

        converted_value = device.downlink_converter.convert(
                    rpc_config, value)
        result = device.write(rpc_config, converted_value)
        if result != 0:
            error_msg = f"Failed to write value {converted_value} to device {device.config.device_name} for RPC method {rpc_method_name}"  # noqa: E501
            self.__log.error(error_msg)
            return error_msg, False

        success_msg = f"Successfully wrote value {converted_value} to device {device.config.device_name} for RPC method {rpc_method_name}"  # noqa: E501
        self.__log.debug(success_msg)
        return success_msg, True

    def _process_read_rpc(self, rpc_config, device):
        data = device.read(rpc_config)
        converted_value = device.uplink_converter.convert_data(rpc_config, data)
        return converted_value

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
