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
from threading import Thread
from time import monotonic, sleep

from pymodbus.datastore import ModbusSparseDataBlock, ModbusServerContext, ModbusSlaveContext
from pymodbus.device import ModbusDeviceIdentification
from pymodbus.framer.ascii_framer import ModbusAsciiFramer
from pymodbus.framer.rtu_framer import ModbusRtuFramer
from pymodbus.framer.socket_framer import ModbusSocketFramer
from pymodbus.server import StartAsyncTcpServer, StartAsyncTlsServer, StartAsyncUdpServer, StartAsyncSerialServer, \
    ServerAsyncStop
from pymodbus.version import version

from thingsboard_gateway.connectors.modbus.bytes_modbus_downlink_converter import BytesModbusDownlinkConverter
from thingsboard_gateway.connectors.modbus.entities.bytes_downlink_converter_config import \
    BytesDownlinkConverterConfig
from thingsboard_gateway.connectors.modbus.constants import FUNCTION_CODE_SLAVE_INITIALIZATION, FUNCTION_TYPE, \
    FUNCTION_CODE_READ

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

        self.unit_id = config['unitId']
        self.host = config['host']
        self.port = config['port']
        self.device_name = config.get('deviceName', 'Modbus Slave')
        self.device_type = config.get('deviceType', 'default')
        self.poll_period = config.get('pollPeriod', 5000)
        self.method = config.get('method', 'socket').lower()

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
            await self.__server.serve_forever()
        except Exception as e:
            self.__stopped = True
            self.__log.error('Failed to start Gateway Modbus Server (Slave): %s', e)

    def get_slave_config_format(self):
        """
        Function return configuration in slave format for adding it to gateway slaves list
        """

        config = {
            'unitId': self.unit_id,
            'deviceName': self.device_name,
            'deviceType': self.device_type,
            'pollPeriod': self.poll_period,
            'host': self.host,
            'port': self.port,
            'method': self.method,
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

        if config.get('identity'):
            identity = ModbusDeviceIdentification()
            identity.VendorName = config['identity'].get('vendorName', '')
            identity.ProductCode = config['identity'].get('productCode', '')
            identity.VendorUrl = config['identity'].get('vendorUrl', '')
            identity.ProductName = config['identity'].get('productName', '')
            identity.ModelName = config['identity'].get('ModelName', '')
            identity.MajorMinorRevision = version.short()

        return identity

    @staticmethod
    def __get_connection_config(config):
        return {
            'type': config['type'],
            'address': (config.get('host'), config.get('port')) if (config['type'] == 'tcp' or 'udp') else None,
            'port': config.get('port') if config['type'] == 'serial' else None,
            'framer': FRAMER_TYPE[config['method']],
            'security': config.get('security', {})
        }

    def __get_server_context(self, config):
        blocks = {}
        if (config.get('values') is None) or (not len(config.get('values'))):
            self.__log.error("No values to read from device %s", config.get('deviceName', 'Modbus Slave'))
            return

        for (key, value) in config.get('values').items():
            values = {}
            converter = BytesModbusDownlinkConverter({}, self.__log)
            for section in ('attributes', 'timeseries', 'attributeUpdates', 'rpc'):
                for item in value.get(section, []):
                    try:
                        function_code = FUNCTION_CODE_SLAVE_INITIALIZATION[key][0] if item['objectsCount'] <= 1 else \
                            FUNCTION_CODE_SLAVE_INITIALIZATION[key][1]
                        converter_config = BytesDownlinkConverterConfig(
                            device_name=config.get('deviceName', 'Gateway'),
                            byte_order=config['byteOrder'],
                            word_order=config.get('wordOrder', 'LITTLE'),
                            repack=config.get('repack', False),
                            objects_count=item['objectsCount'],
                            function_code=function_code,
                            lower_type=item.get(
                                'type', item.get('tag', 'error')),
                            address=item.get('address', 0)
                        )
                        converted_value = converter.convert(
                            converter_config, {'data': {'params': item['value']}})
                        if converted_value is not None:
                            values[item['address'] + 1] = converted_value
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
            self.__log.info("%s - will be initialized without values", config.get('deviceName', 'Modbus Slave'))

        return ModbusServerContext(slaves=ModbusSlaveContext(**blocks), single=True)
