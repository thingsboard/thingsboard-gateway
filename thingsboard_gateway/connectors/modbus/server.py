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
from asyncio import CancelledError
from threading import Thread
from time import monotonic, sleep

from pymodbus.datastore import ModbusSparseDataBlock, ModbusServerContext, ModbusSlaveContext
from pymodbus.device import ModbusDeviceIdentification
from pymodbus.framer.ascii_framer import ModbusAsciiFramer
from pymodbus.framer.rtu_framer import ModbusRtuFramer
from pymodbus.framer.socket_framer import ModbusSocketFramer
from pymodbus.server import StartAsyncTcpServer, StartAsyncTlsServer, StartAsyncUdpServer, StartAsyncSerialServer
from pymodbus.version import version

from thingsboard_gateway.connectors.modbus.bytes_modbus_downlink_converter import BytesModbusDownlinkConverter
from thingsboard_gateway.connectors.modbus.entities.bytes_downlink_converter_config import \
    BytesDownlinkConverterConfig
from thingsboard_gateway.connectors.modbus.constants import ADDRESS_PARAMETER, BYTE_ORDER_PARAMETER, FUNCTION_CODE_SLAVE_INITIALIZATION, FUNCTION_TYPE, \
    FUNCTION_CODE_READ, HOST_PARAMETER, IDENTITY_SECTION, METHOD_PARAMETER, OBJECTS_COUNT_PARAMETER, PORT_PARAMETER, REPACK_PARAMETER, SERIAL_CONNECTION_TYPE_PARAMETER, TAG_PARAMETER, WORD_ORDER_PARAMETER
from thingsboard_gateway.gateway.constants import DEVICE_NAME_PARAMETER, TYPE_PARAMETER

SLAVE_TYPE = {
    'tcp': StartAsyncTcpServer,
    'tls': StartAsyncTlsServer,
    'udp': StartAsyncUdpServer,
    'serial': StartAsyncSerialServer
}

FRAMER_TYPE = {
    'rtu': ModbusRtuFramer,
    'socket': ModbusSocketFramer,
    'ascii': ModbusAsciiFramer
}


class Server(Thread):
    def __init__(self, config, logger):
        super().__init__()
        self.__stopped = False
        self.daemon = True
        self.name = 'Gateway Modbus Server (Slave)'

        self.__log = logger

        self.__config = config

        self.device_name = config.get('deviceName', 'Modbus Slave')
        self.device_type = config.get('deviceType', 'default')
        self.poll_period = config.get('pollPeriod', 5000)

        self.__type = config.get('type', 'tcp').lower()
        self.__identity = self.__get_identity(self.__config)
        self.__server_context = self.__get_server_context(self.__config)
        self.__connection_config = self.__get_connection_config(self.__config)

        self.__server = None

        try:
            self.loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self.loop)
        except RuntimeError:
            self.loop = asyncio.get_event_loop()

    def __str__(self):
        return self.name

    def run(self):
        try:
            self.loop.run_until_complete(self.start_server())
        except CancelledError:
            self.__log.debug("Server %s has been stopped", self.name)
        except Exception as e:
            self.__log.error("Server has been stopped with error: %s", e)

    def stop(self):
        self.__stopped = True

        asyncio.run_coroutine_threadsafe(self.__shutdown(), self.loop)

        self.__check_is_alive()

    def __check_is_alive(self):
        start_time = monotonic()

        while self.is_alive():
            if monotonic() - start_time > 10:
                self.__log.error("Failed to stop slave %s", self.name)
                break
            sleep(.1)

    async def __shutdown(self):
        await self.__server.shutdown()

    async def start_server(self):
        try:
            self.__server = await SLAVE_TYPE[self.__type](identity=self.__identity, context=self.__server_context,
                                                          **self.__connection_config, defer_start=True,
                                                          allow_reuse_address=True, allow_reuse_port=True)
            
            if self.__config[TYPE_PARAMETER] == SERIAL_CONNECTION_TYPE_PARAMETER:
                await self.__server.start()
            await self.__server.serve_forever()
        except Exception as e:
            self.__stopped = True
            self.__log.error('Failed to start Gateway Modbus Server (Slave): %s', e)

    def get_slave_config_format(self):
        """
        Function return configuration in slave format for adding it to gateway slaves list
        """

        config = {
            **self.__config,
            'deviceName': self.device_name,
            'deviceType': self.device_type,
            'pollPeriod': self.poll_period
        }

        for (register, register_values) in self.__config.get('values', {}).items():
            for (section_name, section_values) in register_values.items():
                if not config.get(section_name):
                    config[section_name] = []

                for item in section_values:
                    item_config = {
                        **item,
                        'functionCode': FUNCTION_CODE_READ[register]
                        if section_name not in ('attributeUpdates', 'rpc') else item['functionCode'],
                    }
                    config[section_name].append(item_config)

        return config

    @staticmethod
    def is_runnable(config):
        return config.get('slave') and config.get('slave', {}).get('sendDataToThingsBoard', False)

    @staticmethod
    def __get_identity(config):
        identity = None

        if config.get(IDENTITY_SECTION):
            identity = ModbusDeviceIdentification()
            identity.VendorName = config[IDENTITY_SECTION].get('vendorName', '')
            identity.ProductCode = config[IDENTITY_SECTION].get('productCode', '')
            identity.VendorUrl = config[IDENTITY_SECTION].get('vendorUrl', '')
            identity.ProductName = config[IDENTITY_SECTION].get('productName', '')
            identity.ModelName = config[IDENTITY_SECTION].get('ModelName', '')
            identity.MajorMinorRevision = version.short()

        return identity

    @staticmethod
    def __get_connection_config(config):
        return {
            TYPE_PARAMETER: config[TYPE_PARAMETER],

            ADDRESS_PARAMETER: (config.get(HOST_PARAMETER), config.get(PORT_PARAMETER))
            if (config[TYPE_PARAMETER] == 'tcp' or 'udp') else None,

            PORT_PARAMETER: config.get(PORT_PARAMETER)
            if config[TYPE_PARAMETER] == SERIAL_CONNECTION_TYPE_PARAMETER else None,

            'framer': FRAMER_TYPE[config.get(METHOD_PARAMETER, 'socket')],
            'security': config.get('security', {})
        }

    def __get_server_context(self, config):
        blocks = {}
        if (config.get('values') is None) or (not len(config.get('values'))):
            self.__log.error("No values to read from device %s", config.get(DEVICE_NAME_PARAMETER, 'Modbus Slave'))
            return

        for (key, value) in config.get('values').items():
            values = {}
            converter = BytesModbusDownlinkConverter({}, self.__log)
            for section in ('attributes', 'timeseries', 'attributeUpdates', 'rpc'):
                for item in value.get(section, []):
                    try:
                        function_code = FUNCTION_CODE_SLAVE_INITIALIZATION[key][0] \
                            if item[OBJECTS_COUNT_PARAMETER] <= 1 else FUNCTION_CODE_SLAVE_INITIALIZATION[key][1]
                        converter_config = BytesDownlinkConverterConfig(
                            device_name=config.get(DEVICE_NAME_PARAMETER, 'Gateway'),
                            byte_order=config[BYTE_ORDER_PARAMETER],
                            word_order=config.get(WORD_ORDER_PARAMETER, 'LITTLE'),
                            repack=config.get(REPACK_PARAMETER, False),
                            objects_count=item[OBJECTS_COUNT_PARAMETER],
                            function_code=function_code,
                            lower_type=item.get(
                                TYPE_PARAMETER, item.get(TAG_PARAMETER, 'error')),
                            address=item.get(ADDRESS_PARAMETER, 0)
                        )
                        converted_value = converter.convert(
                            converter_config, {'data': {'params': item['value']}})
                        if converted_value is not None:
                            values[item[ADDRESS_PARAMETER] + 1] = converted_value
                        else:
                            self.__log.error("Failed to convert value %s with type %s, skipping...", item['value'],
                                             item['type'])
                    except Exception as e:
                        self.__log.error("Failed to configure value %s with error: %s, skipping...", item['value'], e)

                try:
                    if len(values):
                        blocks[FUNCTION_TYPE[key]] = ModbusSparseDataBlock(values)
                except Exception as e:
                    self.__log.error("Failed to configure block %s with error: %s", key, e)

        if not len(blocks):
            self.__log.info("%s - will be initialized without values", config.get(DEVICE_NAME_PARAMETER, 'Modbus Slave'))

        return ModbusServerContext(slaves=ModbusSlaveContext(**blocks), single=True)
