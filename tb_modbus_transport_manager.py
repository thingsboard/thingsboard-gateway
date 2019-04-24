import logging
from pymodbus.client.sync import ModbusTcpClient, ModbusUdpClient, ModbusRtuFramer
log = logging.getLogger(__name__)


class TBModbusTransportManager:
    def __init__(self, config):
        log.info(config)

        transport = config["type"]
        host = TBModbusTransportManager.get_parameter(config, "host", "127.0.0.1")
        port = TBModbusTransportManager.get_parameter(config, "port", 502)
        rtu_over_everything = ModbusRtuFramer if (config.get("rtuOverTcp") or config.get("rtuOverUdp")) else False
        client = None

        if transport == "tcp":
            client = ModbusTcpClient
        elif transport == "udp":
            client = ModbusUdpClient
        # elif transport == "rtu":
        #     client = ModbusSerialClient
        else:
            log.warning("Unknown transport type, possible options are 'tcp' and 'udp'")  # and "rtu"

        # if transport == "rtu":
        #     dict_parity = {
        #         "none": "N",
        #         "even": "E",
        #         "odd": "O"
        #     }
        #     self.client = client(method=config["encoding"],
        #                          port=config["portName"],
        #                          stopbits=config["stopBits"],
        #                          baudrate=config["baudRate"],
        #                          parity=dict_parity[config["parity"]])

        if rtu_over_everything:  # if rtu is used, there is "elif" in place of "if"
            self.client = client(host, port, ModbusRtuFramer)
        else:
            self.client = client(host, port)
        self._dict_read_functions = {
            1: self.client.read_coils,
            2: self.client.read_discrete_inputs,
            3: self.client.read_input_registers,
            4: self.client.read_holding_registers
        }
        self._dict_write_functions ={
            5: self.client.write_coil,
            6: self.client.write_registers,
            15: self.client.write_coils,
            16: self.client.write_registers
        }
        self.client.connect()

    def get_data_from_device(self, config, unit_id):
        result = self._dict_read_functions[config["functionCode"]](config["address"],
                                                                   self.get_parameter(config, "registerCount", 1),
                                                                   unit=unit_id)
        return result

    def write_data_to_device(self, config):
        # todo here add call to transform_value_to_device_format
        resp = self._dict_write_functions[config["functionCode"]](config["address"],
                                                                  config["value"]
                                                                  )  # todo here unitId may be used
        log.info("----------------------------------")
        log.info(resp)
        log.info("----------------------------------")
        # todo add write resp processing
        pass

    @staticmethod
    def get_parameter(data, param, default_value):
        if param not in data:
            return default_value
        else:
            return data[param]
