import logging
from utility.tb_utility import TBUtility
from pymodbus.bit_write_message import WriteSingleCoilResponse
from pymodbus.client.sync import ModbusTcpClient, ModbusUdpClient, ModbusRtuFramer
from pymodbus.register_write_message import WriteMultipleRegistersResponse

log = logging.getLogger(__name__)


class TBModbusTransportManager():
    def __init__(self, config, ext_id):
        log.info(config)
        self.ext_id = ext_id
        transport = config["type"]
        host = TBUtility.get_parameter(config, "host", "127.0.0.1")
        port = TBUtility.get_parameter(config, "port", 502)
        rtu_over_everything = ModbusRtuFramer if (config.get("rtuOverTcp") or config.get("rtuOverUdp")) else False
        client = None

        if transport == "tcp":
            client = ModbusTcpClient
        elif transport == "udp":
            client = ModbusUdpClient
        else:
            raise Exception("invalid modbus transport type, not tcp or udp, extension {}".format(self.ext_id))

        if rtu_over_everything:
            self.client = client(host, port, ModbusRtuFramer)
        else:
            self.client = client(host, port)
        self._dict_read_functions = {
            1: self.client.read_coils,
            2: self.client.read_discrete_inputs,
            3: self.client.read_input_registers,
            4: self.client.read_holding_registers
        }
        self._dict_write_functions = {
            5: self.client.write_coil,
            6: self.client.write_registers,
            # 15: self.client.write_coils, # check docs, there is not such function, maybe add it?
            16: self.client.write_registers
        }
        self.client.connect()

    def get_data_from_device(self, config, unit_id):
        result = self._dict_read_functions[config["functionCode"]](config["address"],
                                                                   TBUtility.get_parameter(config, "registerCount", 1),
                                                                   unit=unit_id)
        return result

    def write_data_to_device(self, config):
        log.debug(config)
        resp = None
        try:
            resp = self._dict_write_functions[config["functionCode"]](config["address"],
                                                                      config["payload"],
                                                                      unit=config["unitId"])
        except Exception as e:
            log.error(e)
        if type(resp) in (WriteMultipleRegistersResponse, WriteSingleCoilResponse):
            return True
        else:
            return False
