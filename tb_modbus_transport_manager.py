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
            # todo rework timeout, pymodbus timeout param isn't working correctly
            self.client = ModbusTcpClient(config["host"],
                                          port=502 if not config.get("port") else config["port"])

        self.client.connect()
        log.debug("connected to host {host}:{port}".format(host=config["host"], port=config["port"]))
        self._dict_functions = {
            1: self.client.read_coils,
            2: self.client.read_discrete_inputs,
            3: self.client.read_input_registers,
            4: self.client.read_holding_registers
        }

    def get_data_from_device(self, config, unit_id):
        result = self._dict_functions[config["functionCode"]](config["address"],
                                                              self.get_parameter(config, "registerCount", 1),
                                                              unit=unit_id)
        return result

    def write_data_to_device(self, config):
        # todo add
        pass

    @staticmethod
    def get_parameter(data, param, default_value):
        if param not in data:
            return default_value
        else:
            return data[param]