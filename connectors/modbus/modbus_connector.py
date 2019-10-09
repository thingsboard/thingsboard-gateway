import time
import logging
import threading
from json import dumps
from pymodbus.bit_write_message import WriteSingleCoilResponse
from pymodbus.client.sync import ModbusTcpClient, ModbusUdpClient, ModbusRtuFramer
from pymodbus.register_write_message import WriteMultipleRegistersResponse
from random import choice
from string import ascii_lowercase
from tb_utility.tb_utility import TBUtility
from connectors.connector import Connector
from pymodbus.exceptions import ConnectionException
from connectors.modbus.bytes_modbus_uplink_converter import BytesModbusUplinkConverter

log = logging.getLogger(__name__)


class ModbusConnector(Connector, threading.Thread):
    def __init__(self, gateway, config):
        super().__init__()
        self.__gateway = gateway
        self.__master = None
        self.__server_conf = config.get("server")
        self.__configure_master()
        self.__devices = {}
        self.setName(TBUtility.get_parameter(self.__server_conf,
                                             "name",
                                             'Modbus Default ' + ''.join(choice(ascii_lowercase) for _ in range(5))))
        self.__load_converters()
        self.__connected = False
        self.__stopped = False
        self.daemon = True

    def open(self):
        self.__stopped = False
        self.start()
        log.info("Starting Modbus connector...")

    def run(self):
        if self.__server_conf.get("rtuOverTcp") or self.__server_conf.get("rtuOverUdp"):
            pass  #TODO rtuOverTcp or rtuOverUdp
        else:
            while not self.__master.connect():
                time.sleep(5)
                log.warning("Modbus trying reconnect to %s", self.__server_conf.get("name"))
            log.info("Modbus connected.")
            self.__connected = True

        while True:
            time.sleep(1)
            self.__process_devices()
            if self.__stopped:
                break

    def __load_converters(self):
        try:
            for device in self.__server_conf["devices"]:
                if self.__server_conf.get("converter") is not None and self.__server_conf.get("converter") == "Custom":
                    pass  # TODO Add custom converter
                else:
                    if device.get('deviceName') not in self.__gateway.get_devices():
                        self.__gateway.add_device(device.get('deviceName'), {"connector": self})
                        if device.get('converter') is None:
                            self.__devices[device["deviceName"]] = {"config": device,
                                                                    "converter": BytesModbusUplinkConverter(device),
                                                                    "next_attributes_check": 0,
                                                                    "next_timeseries_check": 0,
                                                                    "last_timeseries": {},
                                                                    "last_attributes": {},
                                                                    }
                        else:
                            pass  #TODO Add custom converter
        except Exception as e:
            log.exception(e)

    def close(self):
        self.__stopped = True
        self.__master.close()
        log.info('%s has been stopped.', self.get_name())

    def get_name(self):
        return self.name

    def __process_devices(self):
        for device in self.__devices:
            current_time = time.time()
            try:
                if self.__devices[device]["attributes"] or self.__devices[device]["timeseries"]:
                    if self.__devices[device]["next_attributes_check"] < current_time:
                        #  Reading attributes
                        log.debug("Checking attribute for device %s", device)
                        self.__devices[device]["next_attributes_check"] = current_time + self.__devices[device]["config"]["attributesPollPeriod"]/1000

                    if self.__devices[device]["next_timeseries_check"] < current_time:
                        #  Reading timeseries
                        log.debug("Checking timeseries for device %s", device)
                        self.__devices[device]["next_timeseries_check"] = current_time + self.__devices[device]["config"]["timeseriesPollPeriod"]/1000

                        for ts in range(len(self.__devices[device]["config"]["timeseries"])):
                            device_response = self.__function_to_device(self.__devices[device]["config"]["timeseries"][ts],
                                                                        self.__devices[device]["config"]["unitId"])
                            log.debug(device_response)
                else:
                    log.debug("No timeseries and attributes in config.")
            except ConnectionException:
                log.error("Connection lost! Trying to reconnect...")
            except Exception as e:
                log.exception(e)

    def on_attributes_update(self):
        pass

    def server_side_rpc_handler(self, content):
        pass

    def __configure_master(self):
        host = TBUtility.get_parameter(self.__server_conf, "host", "localhost")
        port = TBUtility.get_parameter(self.__server_conf, "port", "502")
        timeout = TBUtility.get_parameter(self.__server_conf, "timeout", 35)
        rtu = ModbusRtuFramer if self.__server_conf.get("rtuOverTcp") or self.__server_conf.get("rtuOverUdp") else False
        if self.__server_conf.get('type') == 'tcp':
            client = ModbusTcpClient
        elif self.__server_conf.get('type') == 'udp':
            client = ModbusUdpClient
        else:
            raise Exception("Invalid Modbus transport type.")

        if rtu:
            self.__master = client(host, port, rtu, timeout=timeout)
        else:
            self.__master = client(host, port, timeout=timeout)
        self.__available_functions = {
            1: self.__master.read_coils,
            2: self.__master.read_coils,
            3: self.__master.read_coils,
            4: self.__master.read_coils,
            5: self.__master.write_coils,
            6: self.__master.write_registers,
            15: self.__master.write_coils,
            16: self.__master.write_registers,
        }

    def __function_to_device(self, config, unit_id):
        log.debug(config)
        function_code = config.get('functionCode')
        result = None
        if function_code in (1, 2, 3, 4):
            result = self.__available_functions[function_code](config["address"],
                                                               TBUtility.get_parameter(config, "registerCount", 1),
                                                               unit=unit_id)
        elif function_code in (5, 6, 15, 16):
            result = self.__available_functions[function_code](config["address"],
                                                               config["payload"],
                                                               unit=unit_id)
        else:
            log.error("Unknown Modbus function.")

        return result

