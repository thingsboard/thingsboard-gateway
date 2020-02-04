#     Copyright 2020. ThingsBoard
#
#     Licensed under the Apache License, Version 2.0 (the "License");
#     you may not use this file except in compliance with the License.
#     You may obtain a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#     Unless required by applicable law or agreed to in writing, software
#     distributed under the License is distributed on an "AS IS" BASIS,
#     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#     See the License for the specific language governing permissions and
#     limitations under the License.

import time
import threading
from random import choice
from string import ascii_lowercase
from thingsboard_gateway.tb_utility.tb_utility import TBUtility
from pymodbus.client.sync import ModbusTcpClient, ModbusUdpClient, ModbusSerialClient, ModbusRtuFramer, ModbusSocketFramer
from pymodbus.bit_write_message import WriteSingleCoilResponse, WriteMultipleCoilsResponse
from pymodbus.register_write_message import WriteMultipleRegistersResponse, WriteSingleRegisterResponse
from pymodbus.register_read_message import ReadRegistersResponseBase
from pymodbus.exceptions import ConnectionException
from thingsboard_gateway.connectors.connector import Connector, log
from thingsboard_gateway.connectors.modbus.bytes_modbus_uplink_converter import BytesModbusUplinkConverter
from thingsboard_gateway.connectors.modbus.bytes_modbus_downlink_converter import BytesModbusDownlinkConverter


class ModbusConnector(Connector, threading.Thread):
    def __init__(self, gateway, config, connector_type):
        self.statistics = {'MessagesReceived': 0,
                           'MessagesSent': 0}
        super().__init__()
        self.__gateway = gateway
        self.__connector_type = connector_type
        self.__master = None
        self.__config = config.get("server")
        self.__configure_master()
        self.__devices = {}
        self.setName(self.__config.get("name",
                                       'Modbus Default ' + ''.join(choice(ascii_lowercase) for _ in range(5))))
        self.__load_converters()
        self.__connected = False
        self.__stopped = False
        self.daemon = True

    def is_connected(self):
        return self.__connected

    def open(self):
        self.__stopped = False
        self.start()
        log.info("Starting Modbus connector")

    def run(self):
        while not self.__master.connect():
            time.sleep(5)
            log.warning("Modbus trying reconnect to %s", self.__config.get("name"))
        log.info("Modbus connected.")
        self.__connected = True

        while True:
            time.sleep(1)
            self.__process_devices()
            if self.__stopped:
                break

    def __load_converters(self):
        try:
            for device in self.__config["devices"]:
                if self.__config.get("converter") is not None:
                    converter = TBUtility.check_and_import(self.__connector_type, self.__config["converter"])(device)
                else:
                    converter = BytesModbusUplinkConverter(device)
                if self.__config.get("downlink_converter") is not None:
                    downlink_converter = TBUtility.check_and_import(self.__connector_type, self.__config["downlink_converter"])(device)
                else:
                    downlink_converter = BytesModbusDownlinkConverter(device)
                if device.get('deviceName') not in self.__gateway.get_devices():
                    self.__gateway.add_device(device.get('deviceName'), {"connector": self}, device_type=device.get("deviceType"))
                    self.__devices[device["deviceName"]] = {"config": device,
                                                            "converter": converter,
                                                            "downlink_converter": downlink_converter,
                                                            "next_attributes_check": 0,
                                                            "next_timeseries_check": 0,
                                                            "telemetry": {},
                                                            "attributes": {},
                                                            "last_telemetry": {},
                                                            "last_attributes": {}
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
                    if self.__devices[device]["config"].get(config_data) is not None:
                        unit_id = self.__devices[device]["config"]["unitId"]
                        if self.__devices[device]["next_"+config_data+"_check"] < current_time:
                            #  Reading data from device
                            for interested_data in range(len(self.__devices[device]["config"][config_data])):
                                current_data = self.__devices[device]["config"][config_data][interested_data]
                                current_data["deviceName"] = device
                                input_data = self.__function_to_device(current_data, unit_id)
                                if not isinstance(input_data, ReadRegistersResponseBase) and input_data.isError():
                                    log.exception(input_data)
                                    continue
                                device_responses[config_data][current_data["tag"]] = {"data_sent": current_data,
                                                                                      "input_data": input_data}

                            log.debug("Checking %s for device %s", config_data, device)
                            self.__devices[device]["next_"+config_data+"_check"] = current_time + self.__devices[device]["config"][config_data+"PollPeriod"]/1000
                            log.debug(device_responses)
                            converted_data = self.__devices[device]["converter"].convert(config=None, data=device_responses)

                            if self.__devices[device]["config"].get("sendDataOnlyOnChange"):
                                self.statistics['MessagesReceived'] += 1
                                to_send = {"deviceName": converted_data["deviceName"], "deviceType": converted_data["deviceType"]}
                                if to_send.get("telemetry") is None:
                                    to_send["telemetry"] = []
                                if to_send.get("attributes") is None:
                                    to_send["attributes"] = []
                                for telemetry_dict in converted_data["telemetry"]:
                                    for key, value in telemetry_dict.items():
                                        if self.__devices[device]["last_telemetry"].get(key) is None or \
                                           self.__devices[device]["last_telemetry"][key] != value:
                                            self.__devices[device]["last_telemetry"][key] = value
                                            to_send["telemetry"].append({key: value})
                                for attribute_dict in converted_data["attributes"]:
                                    for key, value in attribute_dict.items():
                                        if self.__devices[device]["last_attributes"].get(key) is None or \
                                           self.__devices[device]["last_attributes"][key] != value:
                                            self.__devices[device]["last_attributes"][key] = value
                                            to_send["attributes"].append({key: value})
                                        # to_send["telemetry"] = converted_data["telemetry"]
                                # if converted_data["attributes"] != self.__devices[device]["attributes"]:
                                    # self.__devices[device]["last_attributes"] = converted_data["attributes"]
                                    # to_send["attributes"] = converted_data["attributes"]
                                if to_send.get("attributes") or to_send.get("telemetry"):
                                    self.__gateway.send_to_storage(self.get_name(), to_send)
                                    self.statistics['MessagesSent'] += 1
                                else:
                                    log.debug("Data has not been changed.")
                            elif self.__devices[device]["config"].get("sendDataOnlyOnChange") is None or not self.__devices[device]["config"].get("sendDataOnlyOnChange"):
                                self.statistics['MessagesReceived'] += 1
                                to_send = {"deviceName": converted_data["deviceName"], "deviceType": converted_data["deviceType"]}
                                # if converted_data["telemetry"] != self.__devices[device]["telemetry"]:
                                self.__devices[device]["last_telemetry"] = converted_data["telemetry"]
                                to_send["telemetry"] = converted_data["telemetry"]
                                # if converted_data["attributes"] != self.__devices[device]["attributes"]:
                                self.__devices[device]["last_telemetry"] = converted_data["attributes"]
                                to_send["attributes"] = converted_data["attributes"]
                                self.__gateway.send_to_storage(self.get_name(), to_send)
                                self.statistics['MessagesSent'] += 1
            except ConnectionException:
                log.error("Connection lost! Trying to reconnect")
            except Exception as e:
                log.exception(e)

    def on_attributes_update(self, content):
        pass

    def __configure_master(self):
        host = self.__config.get("host", "localhost")
        port = self.__config.get("port", 502)
        baudrate = self.__config.get('baudrate', 19200)
        timeout = self.__config.get("timeout", 35)
        method = self.__config.get('method', 'rtu')
        rtu = ModbusRtuFramer if self.__config.get("method") == "rtu" else ModbusSocketFramer
        if self.__config.get('type') == 'tcp':
            self.__master = ModbusTcpClient(host, port, rtu, timeout=timeout)
        elif self.__config.get('type') == 'udp':
            self.__master = ModbusUdpClient(host, port, rtu, timeout=timeout)
        elif self.__config.get('type') == 'serial':
            self.__master = ModbusSerialClient(method=method, port=port, timeout=timeout, baudrate=baudrate)
        else:
            raise Exception("Invalid Modbus transport type.")
        self.__available_functions = {
            1: self.__master.read_coils,
            2: self.__master.read_discrete_inputs,
            3: self.__master.read_holding_registers,
            4: self.__master.read_input_registers,
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
            rpc_command_config["payload"] = self.__devices[content["device"]]["downlink_converter"].convert(rpc_command_config, content)
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
