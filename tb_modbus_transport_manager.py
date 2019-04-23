import logging
from pymodbus.client.sync import ModbusTcpClient, ModbusUdpClient, ModbusRtuFramer
log = logging.getLogger(__name__)


class TBModbusTransportManager:
    def __init__(self, config):
        log.info(config)
        transport = config["type"]
        host = TBModbusTransportManager.get_parameter(config, "host", "127.0.0.1")
        port = TBModbusTransportManager.get_parameter(config, "port", 502)
        client = None
        if transport == "tcp":
            client = ModbusTcpClient
        elif transport == "udp":
            client = ModbusUdpClient
        else:
            log.warning("Unknown transport type, possible options now are tcp and udp")
        rtu = ModbusRtuFramer if (config.get("rtuOverTcp") or config.get("rtuOverUdp")) else None
        if rtu:
            self.client = client(host, port, ModbusRtuFramer)
        else:
            self.client = client(host, port)

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
