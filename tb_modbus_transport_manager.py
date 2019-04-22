import logging
from pymodbus.client.sync import ModbusTcpClient
log = logging.getLogger(__name__)


class TBModbusTransportManager:
    _TIMEOUT = 3

    def __init__(self, config):
        log.info(config)
        self.client = None
        transport = config["type"]
        if transport == "tcp":
            # todo rework timeout, pymodbus timeout param isnt working correctly
            # todo add reconnect param use
            self.client = ModbusTcpClient(config["host"],
                                          port=502 if not config.get("port") else config.get("port"))
        self.client.connect()
        self._dict_functions = {
            1: self.client.read_coils,
            2: self.client.read_discrete_inputs,
            3: self.client.read_holding_registers,
            4: self.client.read_input_registers
        }

    def get_data_from_device(self, config):
        # todo add bit parameter support, this will not work for boolean with bit parameter correctly
        # todo this is hardcode, rework it
        result = self._dict_functions[1](config["address"],
                                         1 if not config.get("registerCount") else config["registerCount"])
        return result
