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
# Try import Pymodbus library or install it and import
try:
    from pymodbus.constants import Defaults
except ImportError:
    print("Modbus library not found - installing...")
    TBUtility.install_package("pymodbus", ">=2.3.0")
    TBUtility.install_package('pyserial')
    from pymodbus.constants import Defaults

from pymodbus.client.sync import ModbusTcpClient, ModbusUdpClient, ModbusSerialClient, ModbusRtuFramer, ModbusSocketFramer
from pymodbus.bit_write_message import WriteSingleCoilResponse, WriteMultipleCoilsResponse
from pymodbus.register_write_message import WriteMultipleRegistersResponse, \
                                            WriteSingleRegisterResponse
from pymodbus.register_read_message import ReadRegistersResponseBase
from pymodbus.bit_read_message import ReadBitsResponseBase
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
        self._connector_type = connector_type
        self.__config = config.get("server")
        self.__current_master, self.__available_functions = self.__configure_master()
        self.__default_config_parameters = ['host', 'port', 'baudrate', 'timeout', 'method', 'stopbits', 'bytesize', 'parity', 'strict', 'type']
        self.__byte_order = self.__config.get("byteOrder")
        self.__word_order = self.__config.get("wordOrder")
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
        self.__connect_to_current_master()
        self.__connected = True

        while True:
            time.sleep(.01)
            self.__process_devices()
            if self.__stopped:
                break

    def __load_converters(self):
        try:
            for device in self.__config["devices"]:
                if self.__config.get("converter") is not None:
                    converter = TBUtility.check_and_import(self._connector_type, self.__config["converter"])(device)
                else:
                    converter = BytesModbusUplinkConverter(device)
                if self.__config.get("downlink_converter") is not None:
                    downlink_converter = TBUtility.check_and_import(self._connector_type, self.__config["downlink_converter"])(device)
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
        self.__stop_connections_to_masters()
        log.info('%s has been stopped.', self.get_name())

    def get_name(self):
        return self.name

    def __process_devices(self):
        for device in self.__devices:
            current_time = time.time()
            device_responses = {"timeseries": {},
                                "attributes": {},
                                }
            to_send = {}
            try:
                for config_data in device_responses:
                    if self.__devices[device]["config"].get(config_data) is not None:
                        unit_id = self.__devices[device]["config"]["unitId"]
                        if self.__devices[device]["next_"+config_data+"_check"] < current_time:
                            self.__connect_to_current_master(device)
                            #  Reading data from device
                            for interested_data in range(len(self.__devices[device]["config"][config_data])):
                                current_data = self.__devices[device]["config"][config_data][interested_data]
                                current_data["deviceName"] = device
                                input_data = self.__function_to_device(current_data, unit_id)
                                # if not isinstance(input_data, ReadRegistersResponseBase) and input_data.isError():
                                #     log.exception(input_data)
                                #     continue
                                device_responses[config_data][current_data["tag"]] = {"data_sent": current_data,
                                                                                      "input_data": input_data}

                            log.debug("Checking %s for device %s", config_data, device)
                            self.__devices[device]["next_"+config_data+"_check"] = current_time + self.__devices[device]["config"][config_data+"PollPeriod"]/1000
                            log.debug(device_responses)
                        converted_data = {}
                        try:
                            converted_data = self.__devices[device]["converter"].convert(config={**self.__devices[device]["config"],
                                                                                                 "byteOrder": self.__devices[device]["config"].get("byteOrder", self.__byte_order),
                                                                                                 "wordOrder": self.__devices[device]["config"].get("wordOrder", self.__word_order)},
                                                                                         data=device_responses)
                        except Exception as e:
                            log.error(e)

                        if converted_data and self.__devices[device]["config"].get("sendDataOnlyOnChange"):
                            if current_data["sendDataOnlyOnChange"] == False:
                                pass
                            else:
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
                                if not to_send.get("attributes") and not to_send.get("telemetry"):
                                    # self.__gateway.send_to_storage(self.get_name(), to_send)
                                    # self.statistics['MessagesSent'] += 1
                                    log.debug("Data has not been changed.")
                            elif converted_data and self.__devices[device]["config"].get("sendDataOnlyOnChange") is None or not self.__devices[device]["config"].get("sendDataOnlyOnChange"):
                                self.statistics['MessagesReceived'] += 1
                                to_send = {"deviceName": converted_data["deviceName"],
                                           "deviceType": converted_data["deviceType"]}
                                # if converted_data["telemetry"] != self.__devices[device]["telemetry"]:
                                self.__devices[device]["last_telemetry"] = converted_data["telemetry"]
                                to_send["telemetry"] = converted_data["telemetry"]
                                # if converted_data["attributes"] != self.__devices[device]["attributes"]:
                                self.__devices[device]["last_attributes"] = converted_data["attributes"]
                                to_send["attributes"] = converted_data["attributes"]
                                # self.__gateway.send_to_storage(self.get_name(), to_send)
                                # self.statistics['MessagesSent'] += 1

                if to_send.get("attributes") or to_send.get("telemetry"):
                    self.__gateway.send_to_storage(self.get_name(), to_send)
                    self.statistics['MessagesSent'] += 1
            except ConnectionException:
                time.sleep(5)
                log.error("Connection lost! Reconnecting...")
            except Exception as e:
                log.exception(e)

    def on_attributes_update(self, content):
        try:
            for attribute_updates_command_config in self.__devices[content["device"]]["config"]["attributeUpdates"]:
                for attribute_updated in content["data"]:
                    if attribute_updates_command_config["tag"] == attribute_updated:
                        to_process = {
                            "device": content["device"],
                            "data": {
                                "method": attribute_updated,
                                "params": content["data"][attribute_updated]
                            }
                        }
                        self.__process_rpc_request(to_process, attribute_updates_command_config)
        except Exception as e:
            log.exception(e)

    def __connect_to_current_master(self, device=None):
        connect_attempt_count = 5
        connect_attempt_time_ms = 100
        if device is None:
            device = list(self.__devices.keys())[0]
        if self.__devices[device].get('master') is None:
            self.__devices[device]['master'], self.__devices[device]['available_functions'] = self.__configure_master(
                self.__devices[device]["config"])
        if self.__devices[device]['master'] != self.__current_master:
            self.__current_master = self.__devices[device]['master']
            self.__available_functions = self.__devices[device]['available_functions']
        connect_attempt_count = self.__devices[device]["config"].get("connectAttemptCount", connect_attempt_count)
        connect_attempt_time_ms = self.__devices[device]["config"].get("connectAttemptTimeMs", connect_attempt_time_ms)
        attempt = 0
        if not self.__current_master.is_socket_open():
            while not self.__current_master.is_socket_open() and attempt < connect_attempt_count:
                attempt = attempt + 1
                self.__current_master.connect()
                if not self.__current_master.is_socket_open():
                    time.sleep(connect_attempt_time_ms / 1000)
                log.debug("Modbus trying connect to %s", device)
        if attempt > 0 and self.__current_master.is_socket_open():
            log.debug("Modbus connected.")

    def __configure_master(self, config=None):
        current_config = self.__config if config is None else config

        host = current_config['host'] if current_config.get("host") is not None else self.__config.get("host", "localhost")
        try:
            port = int(current_config['port']) if current_config.get("port") is not None else self.__config.get(int("port"), 502)
        except ValueError:
            port = current_config['port'] if current_config.get("port") is not None else self.__config.get("port", 502)
        baudrate = current_config['baudrate'] if current_config.get('baudrate') is not None else self.__config.get('baudrate', 19200)
        timeout = current_config['timeout'] if current_config.get("timeout") is not None else self.__config.get("timeout", 35)
        method = current_config['method'] if current_config.get('method') is not None else self.__config.get('method', 'rtu')
        stopbits = current_config['stopbits'] if current_config.get('stopbits') is not None else self.__config.get('stopbits', Defaults.Stopbits)
        bytesize = current_config['bytesize'] if current_config.get('bytesize') is not None else self.__config.get('bytesize', Defaults.Bytesize)
        parity = current_config['parity'] if current_config.get('parity') is not None else self.__config.get('parity',   Defaults.Parity)
        strict = current_config["strict"] if current_config.get("strict") is not None else self.__config.get("strict", True)
        rtu = ModbusRtuFramer if current_config.get("method") == "rtu" or (current_config.get("method") is None and self.__config.get("method") == "rtu") else ModbusSocketFramer
        if current_config.get('type') == 'tcp' or (current_config.get("type") is None and self.__config.get("type") == "tcp"):
            master = ModbusTcpClient(host, port, rtu, timeout=timeout)
        elif current_config.get('type') == 'udp' or (current_config.get("type") is None and self.__config.get("type") == "udp"):
            master = ModbusUdpClient(host, port, rtu, timeout=timeout)
        elif current_config.get('type') == 'serial' or (current_config.get("type") is None and self.__config.get("type") == "serial"):
            master = ModbusSerialClient(method=method,
                                               port=port,
                                               timeout=timeout,
                                               baudrate=baudrate,
                                               stopbits=stopbits,
                                               bytesize=bytesize,
                                               parity=parity,
                                               strict=strict)
        else:
            raise Exception("Invalid Modbus transport type.")
        available_functions = {
            1: master.read_coils,
            2: master.read_discrete_inputs,
            3: master.read_holding_registers,
            4: master.read_input_registers,
            5: master.write_coil,
            6: master.write_register,
            15: master.write_coils,
            16: master.write_registers,
        }
        return master, available_functions

    def __stop_connections_to_masters(self):
        for device in self.__devices:
            self.__devices[device]['master'].close()

    def __function_to_device(self, config, unit_id):
        function_code = config.get('functionCode')
        result = None
        if function_code in (1, 2, 3, 4):
            result = self.__available_functions[function_code](config["address"],
                                                               config.get("objectsCount", config.get("registersCount",  config.get("registerCount", 1))),
                                                               unit=unit_id)
        elif function_code in (5, 6, 15, 16):
            result = self.__available_functions[function_code](config["address"],
                                                               config["payload"],
                                                               unit=unit_id)
        else:
            log.error("Unknown Modbus function with code: %i", function_code)
        log.debug("With result %s", str(result))
        if "Exception" in str(result):
            log.exception(result)
            result = str(result)
        return result

    def server_side_rpc_handler(self, content):
        try:
            if content.get("device") is not None:

                log.debug("Modbus connector received rpc request for %s with content: %s", content["device"], content)
                if isinstance(self.__devices[content["device"]]["config"]["rpc"], dict):
                    rpc_command_config = self.__devices[content["device"]]["config"]["rpc"].get(content["data"]["method"])
                    if rpc_command_config is not None:
                        self.__process_rpc_request(content, rpc_command_config)
                elif isinstance(self.__devices[content["device"]]["config"]["rpc"], list):
                    for rpc_command_config in self.__devices[content["device"]]["config"]["rpc"]:
                        if rpc_command_config["tag"] == content["data"]["method"]:
                            self.__process_rpc_request(content, rpc_command_config)
                            break
                else:
                    log.error("Received rpc request, but method %s not found in config for %s.",
                              content["data"].get("method"),
                              self.get_name())
                    self.__gateway.send_rpc_reply(content["device"],
                                                  content["data"]["id"],
                                                  {content["data"]["method"]: "METHOD NOT FOUND!"})
            else:
                log.debug("Received RPC to connector: %r", content)
        except Exception as e:
            log.exception(e)

    def __process_rpc_request(self, content, rpc_command_config):
        if rpc_command_config is not None:
            rpc_command_config["unitId"] = self.__devices[content["device"]]["config"]["unitId"]
            self.__connect_to_current_master(content["device"])
            # if rpc_command_config.get('bit') is not None:
            #     rpc_command_config["functionCode"] = 6
            if rpc_command_config.get("functionCode") in (5, 6, 15, 16):
                rpc_command_config["payload"] = self.__devices[content["device"]]["downlink_converter"].convert(
                    rpc_command_config, content)
            response = None
            try:
                response = self.__function_to_device(rpc_command_config, rpc_command_config["unitId"])
            except Exception as e:
                log.exception(e)
                response = e
            if isinstance(response, (ReadRegistersResponseBase, ReadBitsResponseBase)):
                to_converter = {"rpc": {content["data"]["method"]: {"data_sent": rpc_command_config,
                                                                    "input_data": response}}}
                response = self.__devices[content["device"]]["converter"].convert(config={**self.__devices[content["device"]]["config"],
                                                                                                     "byteOrder": self.__devices[content["device"]]["config"].get("byteOrder", self.__byte_order),
                                                                                                     "wordOrder": self.__devices[content["device"]]["config"].get("wordOrder", self.__word_order)}, data=to_converter)
                log.debug("Received RPC method: %s, result: %r", content["data"]["method"], response)
                # response = {"success": response}
            elif isinstance(response, (WriteMultipleRegistersResponse,
                                       WriteMultipleCoilsResponse,
                                       WriteSingleCoilResponse,
                                       WriteSingleRegisterResponse)):
                log.debug("Write %r", str(response))
                response = {"success": True}
            if content.get("id") or (content.get("data") is not None and content["data"].get("id")):
                if isinstance(response, Exception):
                    self.__gateway.send_rpc_reply(content["device"],
                                                  content["data"]["id"],
                                                  {content["data"]["method"]: str(response)})
                else:
                    self.__gateway.send_rpc_reply(content["device"],
                                                  content["data"]["id"],
                                                  response)
            log.debug("%r", response)
