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
        self.setName(self.__server_conf.get("name",
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
            pass  # TODO rtuOverTcp or rtuOverUdp
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
                if self.__server_conf.get("converter") is not None:
                    converter = TBUtility.check_and_import('modbus', self.__server_conf["converter"])(device)
                else:
                    converter = BytesModbusUplinkConverter(device)
                if device.get('deviceName') not in self.__gateway.get_devices():
                    self.__gateway.add_device(device.get('deviceName'), {"connector": self})
                    self.__devices[device["deviceName"]] = {"config": device,
                                                            "converter": converter,
                                                            "next_attributes_check": 0,
                                                            "next_timeseries_check": 0,
                                                            "last_data_sended": {},
                                                            }
        except Exception as e:
            log.exception(e)

    def close(self):
        self.__stopped = True
        self.__master.close()
        log.info('%s has been stopped.', self.get_name())

    def get_name(self):
        return self.name

    def __process_devices(self):
        device_responses = {"timeseries": {},
                            "attributes": {},
                            }
        for device in self.__devices:
            current_time = time.time()
            try:
                for config_data in device_responses:
                    if self.__devices[device]["config"][config_data]:
                        unit_id = self.__devices[device]["config"]["unitId"]
                        if self.__devices[device]["next_"+config_data+"_check"] < current_time:
                            #  Reading data from device
                            for interested_data in range(len(self.__devices[device]["config"][config_data])):
                                current_data = self.__devices[device]["config"][config_data][interested_data]
                                device_responses[config_data][current_data["tag"]] = {"sended_data": current_data,
                                                                                      "input_data": self.__function_to_device(current_data, unit_id)}

                            log.debug("Checking %s for device %s", config_data, device)
                            self.__devices[device]["next_"+config_data+"_check"] = current_time + self.__devices[device]["config"][config_data+"PollPeriod"]/1000
                            converted_data = self.__devices[device]["converter"].convert(device_responses)
                            #  TODO Add sending to storage
                            self.__devices[device]["last_data_sended"] = converted_data
            except ConnectionException:
                log.error("Connection lost! Trying to reconnect...")
            except Exception as e:
                log.exception(e)

    def on_attributes_update(self, content):
        pass

    def server_side_rpc_handler(self, content):
        pass

    def __configure_master(self):
        host = self.__server_conf.get("host", "localhost")
        port = self.__server_conf.get("port", 502)
        timeout = self.__server_conf.get("timeout", 35)
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
            2: self.__master.read_discrete_inputs,
            3: self.__master.read_input_registers,
            4: self.__master.read_holding_registers,
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
                                                               config.get("registerCount", 1),
                                                               unit=unit_id)
        elif function_code in (5, 6, 15, 16):
            result = self.__available_functions[function_code](config["address"],
                                                               config["payload"],
                                                               unit=unit_id)
        else:
            log.error("Unknown Modbus function.")

        return result

