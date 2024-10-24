from threading import Thread

from pymodbus.datastore import ModbusSparseDataBlock, ModbusServerContext, ModbusSlaveContext
from pymodbus.device import ModbusDeviceIdentification
from pymodbus.version import version

from thingsboard_gateway.connectors.modbus_async.bytes_modbus_downlink_converter import BytesModbusDownlinkConverter
from thingsboard_gateway.connectors.modbus.modbus_connector import FRAMER_TYPE, FUNCTION_CODE_SLAVE_INITIALIZATION, \
    FUNCTION_TYPE, FUNCTION_CODE_READ


class Server(Thread):
    def __init__(self, config, logger):
        super().__init__()
        self.stopped = False
        self.daemon = True
        self.name = 'Gateway Modbus Server (Slave)'

        self.__log = logger

        self.__config = config

        self.identity = self.__get_identity(self.__config)
        self.server_context = self.__get_server_context(self.__config)
        self.connection_config = self.__get_connection_config(self.__config)

        self.start()

    def __str__(self):
        return self.name

    # TODO: implement run method
    def run(self):
        pass

    def stop(self):
        self.stopped = True

    def get_slave_config_format(self):
        """
        Function return configuration in slave format for adding it to gateway slaves list
        """

        config = {}

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
                    function_code = FUNCTION_CODE_SLAVE_INITIALIZATION[key][0] if item['objectsCount'] <= 1 else \
                        FUNCTION_CODE_SLAVE_INITIALIZATION[key][1]
                    converted_value = converter.convert(
                        {**item,
                         'device': config.get('deviceName', 'Gateway'), 'functionCode': function_code,
                         'byteOrder': config['byteOrder'], 'wordOrder': config.get('wordOrder', 'LITTLE')},
                        {'data': {'params': item['value']}})
                    if converted_value is not None:
                        values[item['address'] + 1] = converted_value
                    else:
                        self.__log.error("Failed to convert value %s with type %s, skipping...", item['value'],
                                         item['type'])
                if len(values):
                    blocks[FUNCTION_TYPE[key]] = ModbusSparseDataBlock(values)

        if not len(blocks):
            self.__log.info("%s - will be initialized without values", config.get('deviceName', 'Modbus Slave'))

        return ModbusServerContext(slaves=ModbusSlaveContext(**blocks), single=True)
