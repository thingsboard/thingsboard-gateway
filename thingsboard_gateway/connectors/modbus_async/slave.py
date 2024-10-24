import asyncio
from threading import Thread
from time import sleep, monotonic

from pymodbus.constants import Defaults

from thingsboard_gateway.connectors.modbus_async.bytes_modbus_downlink_converter import BytesModbusDownlinkConverter
from thingsboard_gateway.connectors.modbus_async.bytes_modbus_uplink_converter import BytesModbusUplinkConverter
from thingsboard_gateway.connectors.modbus_async.configs.bytes_downlink_converter_config import \
    BytesDownlinkConverterConfig
from thingsboard_gateway.connectors.modbus_async.configs.bytes_uplink_converter_config import BytesUplinkConverterConfig
from thingsboard_gateway.gateway.constants import UPLINK_PREFIX, CONVERTER_PARAMETER, DOWNLINK_PREFIX
from thingsboard_gateway.gateway.statistics.statistics_service import StatisticsService
from thingsboard_gateway.tb_utility.tb_loader import TBModuleLoader


class Slave(Thread):
    def __init__(self, **config):
        super().__init__()
        self.daemon = True
        self.stopped = False
        self._log = config['logger']
        self.connector = config['connector']
        self.name = "Modbus slave processor for unit " + str(config['unitId']) + " on host " + str(
            config['host']) + ":" + str(config['port'])

        self.callback = config['callback']

        self.unit_id = config['unitId']
        self.host = config['host']
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

        self.connect_attempt_time_ms = self.set_connect_attempt_time_ms(config.get('connectAttemptTimeMs', 500))
        self.wait_after_failed_attempts_ms = self.set_wait_after_failed_attempts_ms(
            config.get('waitAfterFailedAttemptsMs', 300000))
        self.connection_attempt = self.set_connection_attempt(config.get('connectionAttempt', 5))

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

        self.master = None
        self.available_functions = None

        self.start()

    def __timer(self):
        self.__send_callback()

        while not self.stopped and not self.connector.is_stopped():
            if monotonic() - self.last_polled_time >= self.poll_period:
                self.__send_callback()

            sleep(.001)

    # TODO: refactor it to use asyncio Task
    def __send_callback(self):
        try:
            self.callback(self)
            self.last_polled_time = monotonic()
        except Exception as e:
            self._log.exception('Error sending slave callback: %s', e)

    def run(self):
        self.__timer()

    def close(self):
        self.stopped = True

    def get_name(self):
        return self.device_name

    def __load_downlink_converter(self, config):
        try:
            if config.get(DOWNLINK_PREFIX + CONVERTER_PARAMETER) is not None:
                converter = TBModuleLoader.import_module(self.connector.connector_type,
                                                         config[DOWNLINK_PREFIX + CONVERTER_PARAMETER])({}, self._log)
            else:
                converter = BytesModbusDownlinkConverter({}, self._log)

            return converter
        except Exception as e:
            self._log.exception(e)

    def __load_uplink_converter(self, config):
        try:
            if config.get(UPLINK_PREFIX + CONVERTER_PARAMETER) is not None:
                converter = TBModuleLoader.import_module(self.connector.connector_type,
                                                         config[UPLINK_PREFIX + CONVERTER_PARAMETER])(
                    self.uplink_converter_config, self._log)
            else:
                converter = BytesModbusUplinkConverter(self.uplink_converter_config, self._log)

            return converter
        except Exception as e:
            self._log.exception(e)

    async def connect(self) -> bool:
        cur_time = monotonic() * 1000
        if not self.master.connected:
            if (self.connection_attempt_count >= self.connection_attempt
                    and cur_time - self.last_connection_attempt_time >= self.wait_after_failed_attempts_ms):
                self.connection_attempt_count = 0

            while not self.master.connected \
                    and self.connection_attempt_count < self.connection_attempt \
                    and cur_time - self.last_connection_attempt_time >= self.connect_attempt_time_ms:
                if self.stopped:
                    return False
                self.connection_attempt_count += 1
                self.last_connection_attempt_time = cur_time
                self._log.debug("Trying connect to %s", self)
                await self.master.connect()

                if self.connection_attempt_count == self.connection_attempt:
                    self._log.warn("Maximum attempt count (%i) for device \"%s\" - encountered.",
                                    self.connection_attempt,
                                    self)
                    return False

        if self.connection_attempt_count >= 0 and self.master.connected:
            self.connection_attempt_count = 0
            self.last_connection_attempt_time = cur_time
            return True
        else:
            return False

    def set_master_connection(self, master_connection):
        self.master = master_connection
        self.available_functions = self.__get_available_functions()

    def __get_available_functions(self):
        return {
            1: self.master.read_coils,
            2: self.master.read_discrete_inputs,
            3: self.master.read_holding_registers,
            4: self.master.read_input_registers,
            5: self.master.write_coil,
            6: self.master.write_register,
            15: self.master.write_coils,
            16: self.master.write_registers,
        }

    async def read(self, function_code, address, objects_count):
        self._log.debug('Read %s registers from address %s with function code %s', objects_count, address,
                        function_code)

        result = await self.__read(function_code, address, objects_count)

        StatisticsService.count_connector_message(self.name, stat_parameter_name='connectorMsgsReceived')
        StatisticsService.count_connector_bytes(self.name, result, stat_parameter_name='connectorBytesReceived')

        return result

    async def __read(self, function_code, address, objects_count):
        result = None

        try:
            result = await self.available_functions[function_code](address=address, count=objects_count, slave=self.unit_id)
        except KeyError:
            self._log.error('Unknown Modbus function with code: %s', function_code)

        self._log.debug("Read with result: %s", str(result))
        return result

    async def write(self, function_code, address, value):
        self._log.debug('Write %s value to address %s with function code %s', value, address, function_code)
        result = await self.__write(function_code, address, value)

        StatisticsService.count_connector_message(self.name, stat_parameter_name='connectorMsgsReceived')
        StatisticsService.count_connector_bytes(self.name, result, stat_parameter_name='connectorBytesReceived')

        return result

    async def __write(self, function_code, address, value):
        result = None

        if function_code in (5, 6):
            result = await self.available_functions[function_code](address=address, value=value, slave=self.unit_id)
        elif function_code in (15, 16):
            result = await self.available_functions[function_code](address=address, values=value, slave=self.unit_id)
        else:
            self._log.error("Unknown Modbus function with code: %s", function_code)

        self._log.debug("Write with result: %s", str(result))
        return result

    @staticmethod
    def set_connect_attempt_time_ms(connect_attempt_time_ms):
        return connect_attempt_time_ms if connect_attempt_time_ms >= 500 else 500

    @staticmethod
    def set_wait_after_failed_attempts_ms(wait_after_failed_attempts_ms):
        return wait_after_failed_attempts_ms if wait_after_failed_attempts_ms >= 1000 else 1000

    @staticmethod
    def set_connection_attempt(connection_attempt):
        return connection_attempt if connection_attempt >= 1 else 1

    def is_connected_to_platform(self):
        return self.last_connect_time != 0 and monotonic() - self.last_connect_time < 10

    def __str__(self):
        return f'{self.device_name}'