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

from threading import Thread
from time import sleep, monotonic

from pymodbus.constants import Defaults

from thingsboard_gateway.connectors.modbus_old.constants import *
from thingsboard_gateway.connectors.modbus_old.bytes_modbus_uplink_converter import BytesModbusUplinkConverter
from thingsboard_gateway.connectors.modbus_old.bytes_modbus_downlink_converter import BytesModbusDownlinkConverter
from thingsboard_gateway.tb_utility.tb_loader import TBModuleLoader


class Slave(Thread):
    def __init__(self, **kwargs):
        super().__init__()
        self.timeout = kwargs.get('timeout')
        self.device_name = kwargs['deviceName']
        self._log = kwargs['logger']
        self.poll_period = kwargs['pollPeriod'] / 1000

        self.last_connect_time = 0

        self.byte_order = kwargs.get('byteOrder', 'LITTLE')
        self.word_order = kwargs.get('wordOrder', 'LITTLE')
        self.config = {
            'unitId': kwargs['unitId'],
            'deviceType': kwargs.get('deviceType', 'default'),
            'type': kwargs['type'],
            'host': kwargs.get('host'),
            'port': kwargs['port'],
            'byteOrder': kwargs.get('byteOrder', 'LITTLE'),
            'wordOrder': kwargs.get('wordOrder', 'LITTLE'),
            'tls': kwargs.get('tls', kwargs.get('security')),
            'timeout': kwargs.get('timeout', 35),
            'stopbits': kwargs.get('stopbits', Defaults.Stopbits),
            'bytesize': kwargs.get('bytesize', Defaults.Bytesize),
            'parity': kwargs.get('parity', Defaults.Parity),
            'strict': kwargs.get('strict', True),
            'retries': kwargs.get('retries', 3),
            'connection_attempt': 0,
            'last_connection_attempt_time': 0,
            'waitAfterFailedAttemptsMs': kwargs.get('waitAfterFailedAttemptsMs', 0),
            'connectAttemptTimeMs': kwargs.get('connectAttemptTimeMs', 0),
            'retry_on_empty': kwargs.get('retryOnEmpty', False),
            'retry_on_invalid': kwargs.get('retryOnInvalid', False),
            'method': kwargs.get('method', 'rtu'),
            'baudrate': kwargs.get('baudrate', 19200),
            'attributes': kwargs.get('attributes', []),
            'timeseries': kwargs.get('timeseries', []),
            'attributeUpdates': kwargs.get('attributeUpdates', []),
            'rpc': kwargs.get('rpc', [])
        }

        if REPORT_STRATEGY_PARAMETER in kwargs:
            self.config[REPORT_STRATEGY_PARAMETER] = kwargs[REPORT_STRATEGY_PARAMETER]

        self.__load_converters(kwargs['connector'])

        self.callback = kwargs['callback']

        self.__last_polled_time = None
        self.daemon = True
        self.stop = False

        self.name = "Modbus slave processor for unit " + str(self.config['unitId']) + " on host " + str(
            self.config['host']) + ":" + str(self.config['port'])

        self.start()

    def timer(self):
        self.callback(self, RequestType.POLL)
        self.__last_polled_time = monotonic()

        while not self.stop:
            try:
                current_monotonic = monotonic()
                if current_monotonic - self.__last_polled_time >= self.poll_period:
                    self.__last_polled_time = current_monotonic
                    self.callback(self, RequestType.POLL)
            except Exception as e:
                self._log.exception("Error in slave timer: %s", e)

            sleep(0.001)

    def run(self):
        self.timer()

    def close(self):
        self.stop = True

    def get_name(self):
        return self.device_name

    def __load_converters(self, connector):
        try:
            if self.config.get(UPLINK_PREFIX + CONVERTER_PARAMETER) is not None:
                converter = TBModuleLoader.import_module(connector.connector_type,
                                                         self.config[UPLINK_PREFIX + CONVERTER_PARAMETER])(self, self._log)
            else:
                converter = BytesModbusUplinkConverter({**self.config, 'deviceName': self.device_name}, self._log)

            if self.config.get(DOWNLINK_PREFIX + CONVERTER_PARAMETER) is not None:
                downlink_converter = TBModuleLoader.import_module(connector.connector_type, self.config[
                    DOWNLINK_PREFIX + CONVERTER_PARAMETER])(self, self._log)
            else:
                downlink_converter = BytesModbusDownlinkConverter(self.config, self._log)

            self.config[UPLINK_PREFIX + CONVERTER_PARAMETER] = converter
            self.config[DOWNLINK_PREFIX + CONVERTER_PARAMETER] = downlink_converter
        except Exception as e:
            self._log.exception(e)

    def __str__(self):
        return f'{self.device_name}'
