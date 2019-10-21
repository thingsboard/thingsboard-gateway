import time
import threading
from pymodbus.client.sync import ModbusTcpClient, ModbusUdpClient, ModbusRtuFramer
from pymodbus.bit_write_message import WriteSingleCoilResponse, WriteMultipleCoilsResponse
from pymodbus.register_write_message import WriteMultipleRegistersResponse, WriteSingleRegisterResponse
from random import choice
from string import ascii_lowercase
from tb_utility.tb_utility import TBUtility
from connectors.connector import Connector, log
from pymodbus.exceptions import ConnectionException
from connectors.modbus.bytes_modbus_uplink_converter import BytesModbusUplinkConverter
from connectors.modbus.bytes_modbus_downlink_converter import BytesModbusDownlinkConverter


class ModbusConnector(Connector, threading.Thread):
    def __init__(self, gateway, config):
        super(Connector, self).__init__()
        super(threading.Thread, self).__init__()
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
                if self.__server_conf.get("downlink_converter") is not None:
                    downlink_converter = TBUtility.check_and_import('modbus', self.__server_conf["downlink_converter"])(device)
                else:
                    downlink_converter = BytesModbusDownlinkConverter(device)
                if device.get('deviceName') not in self.__gateway.get_devices():
                    self.__gateway.add_device(device.get('deviceName'), {"connector": self})
                    self.__devices[device["deviceName"]] = {"config": device,
                                                            "converter": converter,
                                                            "downlink_converter": downlink_converter,
                                                            "next_attributes_check": 0,
                                                            "next_timeseries_check": 0,
                                                            "telemetry": {},
                                                            "attributes": {},
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
        for device in self.__devices:
            current_time = time.time()
            device_responses = {"timeseries": {},
                                "attributes": {},
                                }
            try:
                for config_data in device_responses:
                    if self.__devices[device]["config"][config_data]:
                        unit_id = self.__devices[device]["config"]["unitId"]
                        if self.__devices[device]["next_"+config_data+"_check"] < current_time:
                            #  Reading data from device
                            for interested_data in range(len(self.__devices[device]["config"][config_data])):
                                current_data = self.__devices[device]["config"][config_data][interested_data]
                                current_data["deviceName"] = device
                                input_data = self.__function_to_device(current_data, unit_id)
                                if input_data.isError():
                                    log.exception(input_data)
                                    continue
                                device_responses[config_data][current_data["tag"]] = {"data_sent": current_data,
                                                                                      "input_data": input_data}

                            log.debug("Checking %s for device %s", config_data, device)
                            self.__devices[device]["next_"+config_data+"_check"] = current_time + self.__devices[device]["config"][config_data+"PollPeriod"]/1000
                            converted_data = self.__devices[device]["converter"].convert(device_responses)

                            if converted_data["telemetry"] != self.__devices[device]["telemetry"] or\
                               converted_data["attributes"] != self.__devices[device]["attributes"]:
                                to_send = {"deviceName": converted_data["deviceName"], "deviceType": converted_data["deviceType"]}
                                if converted_data["telemetry"] != self.__devices[device]["telemetry"]:
                                    self.__devices[device]["last_telemetry"] = converted_data["telemetry"]
                                    to_send["telemetry"] = converted_data["telemetry"]
                                if converted_data["attributes"] != self.__devices[device]["attributes"]:
                                    self.__devices[device]["last_telemetry"] = converted_data["attributes"]
                                    to_send["attributes"] = converted_data["attributes"]
                                self.__gateway.send_to_storage(self.get_name(), to_send)
            except ConnectionException:
                log.error("Connection lost! Trying to reconnect...")
            except Exception as e:
                log.exception(e)

    def on_attributes_update(self, content):
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
            log.error("Unknown Modbus function with code: %i", function_code)
        log.debug("To modbus device %s, \n%s", config["deviceName"], config)
        log.debug("With result %s", result)

        return result

    def server_side_rpc_handler(self, content):
        log.debug("Modbus connector received rpc request for %s with content: %s", self.get_name(), content)
        rpc_command_config = self.__devices[content["device"]]["config"]["rpc"].get(content["data"].get("method"))
        if rpc_command_config.get('bit') is not None:
            rpc_command_config["functionCode"] = 6
            rpc_command_config["unitId"] = self.__devices[content["device"]]["config"]["unitId"]

        if rpc_command_config is not None:
            rpc_command_config["payload"] = self.__devices[content["device"]]["downlink_converter"].convert(content, rpc_command_config)
            response = None
            try:
                response = self.__function_to_device(rpc_command_config, rpc_command_config["unitId"])
            except Exception as e:
                log.exception(e)
            if response is not None:
                log.debug(response)
                if type(response) in (WriteMultipleRegistersResponse,
                                      WriteMultipleCoilsResponse,
                                      WriteSingleCoilResponse,
                                      WriteSingleRegisterResponse):
                    response = True
                else:
                    response = False
                log.debug(response)
                self.__gateway.send_rpc_reply(content["device"],
                                              content["data"]["id"],
                                              {content["data"]["method"]: response})
        else:
            log.error("Received rpc request, but method %s not found in config for %s.",
                      content["data"].get("method"),
                      self.get_name())
