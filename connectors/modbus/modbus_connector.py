import time
import logging
import threading
from pymodbus.bit_write_message import WriteSingleCoilResponse
from pymodbus.client.sync import ModbusTcpClient, ModbusUdpClient, ModbusRtuFramer
from pymodbus.register_write_message import WriteMultipleRegistersResponse
from random import choice
from string import ascii_lowercase
from tb_utility.tb_utility import TBUtility
from connectors.connector import Connector
from connectors.modbus.modbus_uplink_converter import ModbusUplinkConverter

log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)


class ModbusConnector(Connector, threading.Thread):
    def __init__(self, gateway, config):
        super().__init__()
        self.__master = None
        self.__server_conf = config.get("server")
        self.__configure_master()
        self.__gateway = gateway
        self.__device_converters = {}
        self.setName(TBUtility.get_parameter(self.__server_conf,
                                             "name",
                                             'Modbus Default ' + ''.join(choice(ascii_lowercase) for _ in range(5))))
        self.__load_converters()
        self.__stopped = False
        self.daemon = True

    def open(self):
        self.__stopped = False
        log.info("Starting Modbus connector...")
        if self.__server_conf.get("rtuOverTcp"):
            pass  #TODO rtuOverTcp
        else:
            self.__master = modbus_tcp.TcpMaster(host=self.__server_conf.get("host"),
                                                 port=TBUtility.get_parameter(self.__server_conf, "port", 502))
            self.__master.set_timeout(self.__server_conf.get("timeout"))
            log.info("Modbus connected.")

    def run(self):
        while True:
            time.sleep(1)
            if self.__stopped:
                break

    def __load_converters(self):
        try:
            for device in self.__server_conf["devices"]:
                if self.__server_conf.get("converter") is not None and self.__server_conf.get("converter") == "Custom":
                    pass  # TODO Add custom converter
                else:
                    if device.get('deviceName') not in self.__gateway.get_devices():
                        self.__gateway.add(device.get('deviceName'), {"connector": self})
                        self.__device_converters[device['deviceName']]['converter'] = ModbusUplinkConverter(device)
        except Exception as e:
            log.exception(e)

    def close(self):
        self.__stopped = True
        log.info('%s has been stopped.', self.get_name())

    def get_name(self):
        return self.name

    def on_attributes_update(self):
        pass

    def server_side_rpc_handler(self, content):
        pass

    def __configure_master(self):
        host = TBUtility.get_parameter(self.__server_conf, "host", "localhost")
        port = TBUtility.get_parameter(self.__server_conf, "port", "502")
        rtu = ModbusRtuFramer if self.__server_conf.get("rtuOverTcp") or self.__server_conf.get("rtuOverUdp") else False
        client = None
        if self.__server_conf.get('transport') == 'tcp':
            client = ModbusTcpClient
        elif self.__server_conf.get('transport') == 'udp':
            client = ModbusUdpClient
        else:
            log.error("Invalid Modbus transport type.")