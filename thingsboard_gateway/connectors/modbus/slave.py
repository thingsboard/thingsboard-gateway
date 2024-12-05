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
from threading import Thread
from time import sleep, monotonic
from typing import Tuple, Dict, Union

from pymodbus.constants import Defaults

from thingsboard_gateway.connectors.modbus.bytes_modbus_downlink_converter import BytesModbusDownlinkConverter
from thingsboard_gateway.connectors.modbus.bytes_modbus_uplink_converter import BytesModbusUplinkConverter
from thingsboard_gateway.connectors.modbus.entities.bytes_uplink_converter_config import BytesUplinkConverterConfig
from thingsboard_gateway.connectors.modbus.modbus_converter import ModbusConverter
from thingsboard_gateway.gateway.constants import UPLINK_PREFIX, CONVERTER_PARAMETER, DOWNLINK_PREFIX, \
    REPORT_STRATEGY_PARAMETER
from thingsboard_gateway.gateway.statistics.statistics_service import StatisticsService
from thingsboard_gateway.tb_utility.tb_loader import TBModuleLoader


class Slave(Thread):
    def __init__(self, connector, logger, config):
        super().__init__()
        self.daemon = True
        self.stopped = False
        self._log = logger
        self.connector = connector
        self.name = "Modbus slave processor for unit " + str(config['unitId']) + " on host " + str(
            config['host']) + ":" + str(config['port']) + ' ' + config['deviceName']

        self.callback = connector.callback

        self.unit_id = config['unitId']
        self.host = config.get('host')
        self.port = config['port']
        self.type = config.get('type', 'tcp').lower()
        self.method = config['method']
        self.tls = config.get('tls', {})
        self.timeout = config.get('timeout')
        self.retry_on_empty = config.get('retryOnEmpty', False)
        self.retry_on_invalid = config.get('retryOnInvalid', False)
        self.retries = config.get('retries', 3)
        self.baudrate = config.get('baudrate', 19200)
        self.stopbits = config.get('stopbits', Defaults.Stopbits)
        self.bytesize = config.get('bytesize', Defaults.Bytesize)
        self.parity = config.get('parity', Defaults.Parity)
        self.strict = config.get('strict', Defaults.Strict)
        self.repack = config.get('repack', False)
        self.word_order = config.get('wordOrder', 'LITTLE').upper()
        self.byte_order = config.get('byteOrder', 'LITTLE').upper()

        self.attributes_updates_config = config.get('attributeUpdates', [])
        self.rpc_requests_config = config.get('rpc', [])

        self.connect_attempt_time_ms = config.get('connectAttemptTimeMs', 500) \
            if config.get('connectAttemptTimeMs', 500) >= 500 else 500
        self.wait_after_failed_attempts_ms = config.get('waitAfterFailedAttemptsMs', 300000) \
            if config.get('waitAfterFailedAttemptsMs', 300000) >= 300000 else 300000
        self.connection_attempt = config.get('connectionAttempt', 5) if config.get('connectionAttempt', 5) >= 5 else 5

        self.device_name = config['deviceName']
        self.device_type = config.get('deviceType', 'default')

        self.poll_period = config['pollPeriod'] / 1000

        self.last_connect_time = 0
        self.last_polled_time = 0
        self.last_connection_attempt_time = 0
        self.connection_attempt_count = 0

        self.downlink_converter = self.__load_downlink_converter(config)

        self.uplink_converter_config = BytesUplinkConverterConfig(**config)
        self.uplink_converter = self.__load_uplink_converter(config)

        self.__master = None
        self.available_functions = None

        self.start()

    def __timer(self):
        self.__send_callback()

        while not self.stopped and not self.connector.is_stopped():
            if monotonic() - self.last_polled_time >= self.poll_period:
                self.__send_callback()

            sleep(.001)

    def __send_callback(self):
        self.last_polled_time = monotonic()

        try:
            self.callback(self, self.connector.process_device_requests)
        except Exception as e:
            self._log.exception('Error sending slave callback: %s', e)

    def run(self):
        self.__timer()

    def close(self):
        self.stopped = True

    def get_name(self):
        return self.device_name

    def __load_downlink_converter(self, config):
        return self.__load_converter(config, DOWNLINK_PREFIX, {})

    def __load_uplink_converter(self, config):
        return self.__load_converter(config, UPLINK_PREFIX, self.uplink_converter_config)

    def __load_converter(self, config, converter_type, converter_config: Union[Dict, BytesUplinkConverterConfig]={}):
        try:
            if isinstance(config.get(converter_type + CONVERTER_PARAMETER), str):
                converter = TBModuleLoader.import_module(self.connector.connector_type,
                                                         config[converter_type + CONVERTER_PARAMETER])(
                    converter_config, self._log)
            elif isinstance(config.get(converter_type + CONVERTER_PARAMETER), ModbusConverter):
                converter = config[converter_type + CONVERTER_PARAMETER]
            else:
                if converter_type == DOWNLINK_PREFIX:
                    converter = BytesModbusDownlinkConverter(converter_config, self._log)
                else:
                    converter = BytesModbusUplinkConverter(converter_config, self._log)

            return converter
        except Exception as e:
            self._log.exception('Failed to load %s converter for % slave: %s', converter_type, self.name, e)

    async def connect(self) -> Tuple[bool, bool]:
        cur_time = monotonic() * 1000
        just_added = False
        if not self.master.connected():
            if (self.connection_attempt_count >= self.connection_attempt
                    and cur_time - self.last_connection_attempt_time >= self.wait_after_failed_attempts_ms):
                self.connection_attempt_count = 0

            while not self.master.connected() \
                    and self.connection_attempt_count < self.connection_attempt \
                    and cur_time - self.last_connection_attempt_time >= self.connect_attempt_time_ms:
                if self.stopped:
                    return False, False
                self.connection_attempt_count += 1
                self.last_connection_attempt_time = cur_time
                self._log.debug("Trying connect to %s", self)
                await self.master.connect()

                if self.connection_attempt_count == self.connection_attempt:
                    self._log.warn("Maximum attempt count (%i) for device \"%s\" - encountered.",
                                    self.connection_attempt,
                                    self)
                    return False, False
            just_added = True

            self._log.info("Connected to %s", self)

        if self.connection_attempt_count >= 0 and self.master.connected():
            self.connection_attempt_count = 0
            self.last_connection_attempt_time = cur_time
            return True, just_added
        else:
            return False, False

    @property
    def master(self):
        return self.__master

    @master.setter
    def master(self, master):
        self.__master = master
        self.available_functions = self.__master.get_available_functions()

    async def read(self, function_code, address, objects_count):
        self._log.debug('Read %s registers from address %s with function code %s', objects_count, address,
                        function_code)

        result = await self.__read(function_code, address, objects_count)

        StatisticsService.count_connector_message(self.connector.get_name(), stat_parameter_name='connectorMsgsReceived')
        StatisticsService.count_connector_bytes(self.connector.get_name(), result, stat_parameter_name='connectorBytesReceived')

        return result

    async def __read(self, function_code, address, objects_count):
        result = None

        try:
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

        StatisticsService.count_connector_message(self.connector.get_name(), stat_parameter_name='connectorMsgsReceived')
        StatisticsService.count_connector_bytes(self.connector.get_name(), result, stat_parameter_name='connectorBytesReceived')

        return result

    async def __write(self, function_code, address, value):
        result = None

        try:
            if function_code in (5, 6):
                result = await self.available_functions[function_code](address=address, value=value, unit_id=self.unit_id)
            elif function_code in (15, 16):
                result = await self.available_functions[function_code](address=address, values=value, unit_id=self.unit_id)
            else:
                self._log.error("Unknown Modbus function with code: %s", function_code)
        except Exception as e:
            self._log.error("Failed to write with function code %s: %s", function_code, e)
            future = asyncio.Future()
            future.set_result(False)
            return future

        self._log.debug("Write with result: %s", str(result))
        return result

    def is_connected_to_platform(self):
        return self.last_connect_time != 0 and monotonic() - self.last_connect_time < 10

    def __str__(self):
        return f'{self.device_name}'
