import logging
from json import dumps
from connectors.modbus.modbus_uplink_converter import ModbusUplinkConverter

log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)

class BytesModbusUplinkConverter(ModbusUplinkConverter):
    def __init__(self, config):
        log.debug("Modbus converter created with config %s", dumps(config))

    def convert(self):
        pass
