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
from threading import Thread
from time import sleep, monotonic
from typing import TYPE_CHECKING, Dict, Union

from _asyncio import Future

from thingsboard_gateway.connectors.modbus.constants import PymodbusDefaults
from thingsboard_gateway.connectors.modbus.bytes_modbus_downlink_converter import BytesModbusDownlinkConverter
from thingsboard_gateway.connectors.modbus.bytes_modbus_uplink_converter import BytesModbusUplinkConverter
from thingsboard_gateway.connectors.modbus.constants import (
    BAUDRATE_PARAMETER,
    BYTE_ORDER_PARAMETER,
    BYTESIZE_PARAMETER,
    CONNECT_ATTEMPT_COUNT_PARAMETER,
    CONNECT_ATTEMPT_TIME_MS_PARAMETER,
    HOST_PARAMETER,
    METHOD_PARAMETER,
    PARITY_PARAMETER,
    PORT_PARAMETER,
    REPACK_PARAMETER,
    RETRIES_PARAMETER,
    RPC_SECTION,
    SERIAL_CONNECTION_TYPE_PARAMETER,
    STOPBITS_PARAMETER,
    TIMEOUT_PARAMETER,
    UNIT_ID_PARAMETER,
    WAIT_AFTER_FAILED_ATTEMPTS_MS_PARAMETER,
    WORD_ORDER_PARAMETER,
    DELAY_BETWEEN_REQUESTS_MS_PARAMETER,
    TAG_PARAMETER
)
from thingsboard_gateway.connectors.modbus.entities.bytes_uplink_converter_config import BytesUplinkConverterConfig
from thingsboard_gateway.connectors.modbus.modbus_converter import ModbusConverter
from thingsboard_gateway.gateway.constants import (
    DEVICE_NAME_PARAMETER,
    DEVICE_TYPE_PARAMETER,
    TYPE_PARAMETER,
    UPLINK_PREFIX,
    CONVERTER_PARAMETER,
    DOWNLINK_PREFIX
)
from thingsboard_gateway.gateway.statistics.statistics_service import StatisticsService
from thingsboard_gateway.tb_utility.tb_loader import TBModuleLoader

if TYPE_CHECKING:
    from thingsboard_gateway.connectors.modbus.modbus_connector import ModbusConnector
    from thingsboard_gateway.connectors.modbus.entities.master import Master


class Slave(Thread):
    def __init__(self, connector: 'ModbusConnector', logger, config):
        super().__init__()
        self.daemon = True
        self.stopped = False
        self._log = logger
        self.connector = connector
        self.type = config.get(TYPE_PARAMETER, 'tcp').lower()

        if self.type == SERIAL_CONNECTION_TYPE_PARAMETER:
            self.name = "Modbus slave processor for unit " + \
                str(config[UNIT_ID_PARAMETER]) + " on port " + \
                str(config[PORT_PARAMETER]) + ' ' + config[DEVICE_NAME_PARAMETER]
        else:
            self.name = "Modbus slave processor for unit " + \
                str(config[UNIT_ID_PARAMETER]) + " on host " + str(config.get('host')) + \
                ":" + str(config[PORT_PARAMETER]) + ' ' + config[DEVICE_NAME_PARAMETER]

        self.callback = connector.callback

        self.unit_id = config[UNIT_ID_PARAMETER]
        self.host = config.get(HOST_PARAMETER)
        self.port = config[PORT_PARAMETER]
        self.method = config[METHOD_PARAMETER].upper()
        self.tls = config.get('tls', {})
        self.timeout = config.get(TIMEOUT_PARAMETER, 30)
        self.retries = self.parse_retries_from_config(config.get(RETRIES_PARAMETER, 3))
        self.baudrate = config.get(BAUDRATE_PARAMETER, 19200)
        self.stopbits = config.get(STOPBITS_PARAMETER, PymodbusDefaults.Stopbits)
        self.bytesize = config.get(BYTESIZE_PARAMETER, PymodbusDefaults.Bytesize)
        self.parity = config.get(PARITY_PARAMETER, PymodbusDefaults.Parity)
        self.repack = config.get(REPACK_PARAMETER, False)
        self.word_order = config.get(WORD_ORDER_PARAMETER, 'LITTLE').upper()
        self.byte_order = config.get(BYTE_ORDER_PARAMETER, 'LITTLE').upper()
        self.handle_local_echo = config.get('handleLocalEcho', False)

        self.attributes_updates_config = config.get('attributeUpdates', [])
        self.rpc_requests_config = config.get(RPC_SECTION, [])

        self.connect_attempt_time_ms = config.get(CONNECT_ATTEMPT_TIME_MS_PARAMETER, 500) \
            if config.get(CONNECT_ATTEMPT_TIME_MS_PARAMETER, 500) > 500 else 500
        self.wait_after_failed_attempts_ms = config.get(WAIT_AFTER_FAILED_ATTEMPTS_MS_PARAMETER, 30000) \
            if config.get(WAIT_AFTER_FAILED_ATTEMPTS_MS_PARAMETER, 30000) > 30000 else 30000
        self.connection_attempt = config.get(CONNECT_ATTEMPT_COUNT_PARAMETER, 2) \
            if config.get(CONNECT_ATTEMPT_COUNT_PARAMETER, 2) >= 2 else 2

        self.__delay_between_requests_ms = config.get(DELAY_BETWEEN_REQUESTS_MS_PARAMETER, 0)

        self.device_name = config[DEVICE_NAME_PARAMETER]
        self.device_type = config.get(DEVICE_TYPE_PARAMETER, 'default')

        self.poll_period = config['pollPeriod'] / 1000

        self.last_connect_time = 0
        self.last_polled_time = 0
        self.last_connection_attempt_time = 0
        self.connection_attempt_count = 0

        self.downlink_converter = self.__load_downlink_converter(config)

        self.uplink_converter_config = BytesUplinkConverterConfig(**config)
        self.uplink_converter = self.__load_uplink_converter(config)

        self.__master: 'Master' = None
        self.available_functions = None

        self.shared_attributes_keys = []
        for attr_config in self.attributes_updates_config:
            self.shared_attributes_keys.append(attr_config[TAG_PARAMETER])

    def __timer(self):
        if self.__master is not None:
            self.__send_callback(monotonic())
            next_poll_time = monotonic() + self.poll_period

            while not self.stopped and not self.connector.is_stopped():
                current_time = monotonic()
                if current_time >= next_poll_time:
                    self.__send_callback(current_time)
                    next_poll_time = current_time + self.poll_period

                sleep_time = max(0.0, next_poll_time - monotonic())
                sleep(sleep_time)

    def __send_callback(self, current_monotonic):
        self.last_polled_time = current_monotonic

        try:
            self.callback(self, self.connector.process_device_requests)
        except Exception as e:
            self._log.exception('Error sending slave callback: %s', e)

    def run(self):
        if self.uplink_converter_config.is_readable():
            self.__timer()

    def close(self, loop):
        future = asyncio.run_coroutine_threadsafe(self.disconnect(), loop)
        try:
            future.result(timeout=5)
        except Exception as e:
            self._log.error('Failed to disconnect from %s: %s', self, e)
        self.stopped = True

    def get_name(self):
        return self.device_name

    def __load_downlink_converter(self, config):
        return self.__load_converter(config, DOWNLINK_PREFIX, {})

    def __load_uplink_converter(self, config):
        return self.__load_converter(config, UPLINK_PREFIX, self.uplink_converter_config)

    def __load_converter(self, config, converter_type, converter_config: Union[Dict, BytesUplinkConverterConfig] = {}):
        try:
            if isinstance(config.get(converter_type + CONVERTER_PARAMETER), str):
                converter = TBModuleLoader.import_module(self.connector.connector_type,
                                                         config[converter_type + CONVERTER_PARAMETER])(
                    converter_config, self._log)
            elif isinstance(config.get(converter_type + CONVERTER_PARAMETER), ModbusConverter):
                converter = config[converter_type + CONVERTER_PARAMETER]
            else:
                if converter_type == DOWNLINK_PREFIX:
                    converter = BytesModbusDownlinkConverter(converter_config, self.connector.converter_log)
                else:
                    converter = BytesModbusUplinkConverter(converter_config, self.connector.converter_log)

            return converter
        except Exception as e:
            self._log.exception('Failed to load %s converter for %s slave: %s', converter_type, self.name, e)

    async def connect(self) -> bool:
        cur_time = monotonic() * 1000
        initial_connection = False
        if not self.master.connected():
            if (self.connection_attempt_count >= self.connection_attempt
                    and cur_time - self.last_connection_attempt_time >= self.wait_after_failed_attempts_ms):
                self.connection_attempt_count = 0

            while not self.master.connected() \
                    and self.connection_attempt_count < self.connection_attempt \
                    and (cur_time - self.last_connection_attempt_time >= self.connect_attempt_time_ms
                         or self.last_connection_attempt_time == 0):
                if self.stopped:
                    return False
                self.connection_attempt_count += 1
                self.last_connection_attempt_time = cur_time
                self._log.debug("Trying connect to %s", self)

                try:
                    await asyncio.wait_for(self.master.connect(), timeout=self.timeout)
                except asyncio.exceptions.TimeoutError:
                    self._log.warning("Timeout error while connecting to %s", self)

                if self.connection_attempt_count == self.connection_attempt:
                    self._log.warning("Maximum attempt count (%i) for device \"%s\" - encountered.",
                                      self.connection_attempt,
                                      self)
                    return False
                initial_connection = True

        if self.connection_attempt_count >= 0 and self.master.connected():
            self.connection_attempt_count = 0
            self.last_connection_attempt_time = cur_time
            if initial_connection:
                self._log.info("Connected to %s", self)
            return True
        else:
            return False

    async def disconnect(self):
        if self.master.connected():
            await self.master.close()

    @property
    def master(self):
        return self.__master

    @master.setter
    def master(self, master):
        if master is not None:
            self.__master = master
            self.available_functions = self.__master.get_available_functions()
        else:
            self._log.warning('Master is not set for slave %s', self.device_name)

    async def read(self, function_code, address, objects_count):
        self._log.debug('Reading %s registers from address %s with function code %s', objects_count, address,
                        function_code)

        result = await self.__read(function_code, address, objects_count)

        StatisticsService.count_connector_message(self.connector.get_name(),
                                                  stat_parameter_name='connectorMsgsReceived')
        StatisticsService.count_connector_bytes(self.connector.get_name(), result,
                                                stat_parameter_name='connectorBytesReceived')

        return result

    async def __read(self, function_code, address, objects_count):
        result = None

        try:
            time_to_sleep = self.__master.get_time_to_pass_delay_between_requests(self.__delay_between_requests_ms)
            if time_to_sleep > 0:
                await asyncio.sleep(time_to_sleep / 1000)
            result = await self.available_functions[function_code](address=address,
                                                                   count=objects_count,
                                                                   unit_id=self.unit_id)
        except KeyError:
            self._log.error('Unknown Modbus function with code: %s', function_code)

        self._log.debug("Read with result: %s", str(result))
        return result

    async def write(self, function_code, address, value):
        self._log.debug('Write %s value to address %s with function code %s', value, address, function_code)
        result = await self.__write(function_code, address, value)

        if isinstance(result, Future):
            if not result.done():
                result = await result
            if result.exception() is not None:
                raise result.exception()

        StatisticsService.count_connector_message(self.connector.get_name(),
                                                  stat_parameter_name='connectorMsgsReceived')
        StatisticsService.count_connector_bytes(self.connector.get_name(), result,
                                                stat_parameter_name='connectorBytesReceived')

        return result

    async def __write(self, function_code, address, value):
        result = None

        try:
            time_to_sleep = self.__master.get_time_to_pass_delay_between_requests(self.__delay_between_requests_ms)
            if time_to_sleep > 0:
                await asyncio.sleep(time_to_sleep / 1000)
            if function_code in (5, 6):
                result = await self.available_functions[function_code](address=address, value=value,
                                                                       unit_id=self.unit_id)
            elif function_code in (15, 16):
                if not isinstance(value, list):
                    value = [value]

                result = await self.available_functions[function_code](address=address, values=value,
                                                                       unit_id=self.unit_id)
            else:
                self._log.error("Unknown Modbus function with code: %s", function_code)
        except Exception as e:
            self._log.error("Failed to write with function code %s: %s", function_code, e)
            future = asyncio.Future()
            future.set_exception(e)
            return future

        self._log.debug("Write with result: %s", str(result))
        return result

    def is_connected_to_platform(self):
        return self.last_connect_time != 0 and monotonic() - self.last_connect_time < 10

    def get_device_rpc_config(self, rpc_method):
        if isinstance(self.rpc_requests_config, dict):
            return self.rpc_requests_config.get(rpc_method)
        elif isinstance(self.rpc_requests_config, list):
            for rpc_command_config in self.rpc_requests_config:
                if rpc_command_config[TAG_PARAMETER] == rpc_method:
                    return rpc_command_config
        else:
            return None

    def __str__(self):
        return f'{self.device_name}'

    @staticmethod
    def parse_retries_from_config(value):
        if isinstance(value, bool):
            return 3 if value is True else 0
        elif isinstance(value, int):
            if value < 0:
                raise ValueError('Retries parameter must be a non-negative integer')
            return value
        else:
            raise ValueError('Retries parameter must be int or bool')
