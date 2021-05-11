#     Copyright 2021. ThingsBoard
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

from thingsboard_gateway.tb_utility.tb_loader import TBModuleLoader
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
from pymodbus.register_write_message import WriteMultipleRegistersResponse, WriteSingleRegisterResponse
from pymodbus.register_read_message import ReadRegistersResponseBase
from pymodbus.bit_read_message import ReadBitsResponseBase
from pymodbus.exceptions import ConnectionException

from thingsboard_gateway.connectors.connector import Connector, log
from thingsboard_gateway.connectors.modbus.constants import *
from thingsboard_gateway.connectors.modbus.bytes_modbus_uplink_converter import BytesModbusUplinkConverter
from thingsboard_gateway.connectors.modbus.bytes_modbus_downlink_converter import BytesModbusDownlinkConverter

CONVERTED_DATA_SECTIONS = [ATTRIBUTES_PARAMETER, TELEMETRY_PARAMETER]


class ModbusConnector(Connector, threading.Thread):

    def __init__(self, gateway, config, connector_type):
        self.statistics = {STATISTIC_MESSAGE_RECEIVED_PARAMETER: 0,
                           STATISTIC_MESSAGE_SENT_PARAMETER: 0}
        super().__init__()
        self.__gateway = gateway
        self._connector_type = connector_type
        self.__config = config.get(CONFIG_SERVER_SECTION_PARAMETER)
        self.__current_master, self.__available_functions = self.__configure_master()
        self.__default_config_parameters = [HOST_PARAMETER, 
                                            PORT_PARAMETER, 
                                            BAUDRATE_PARAMETER, 
                                            TIMEOUT_PARAMETER, 
                                            METHOD_PARAMETER, 
                                            STOPBITS_PARAMETER, 
                                            BYTESIZE_PARAMETER, 
                                            PARITY_PARAMETER, 
                                            STRICT_PARAMETER, 
                                            TYPE_PARAMETER]
        self.__byte_order = self.__config.get(BYTE_ORDER_PARAMETER)
        self.__word_order = self.__config.get(WORD_ORDER_PARAMETER)
        self.__configure_master()
        self.__devices = {}
        self.setName(self.__config.get("name", 'Modbus Default ' + ''.join(choice(ascii_lowercase) for _ in range(5))))
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
            for device in self.__config[CONFIG_DEVICES_SECTION_PARAMETER]:
                if self.__config.get(UPLINK_PREFIX + CONVERTER_PARAMETER) is not None:
                    converter = TBModuleLoader.import_module(self._connector_type, self.__config[UPLINK_PREFIX + CONVERTER_PARAMETER])(device)
                else:
                    converter = BytesModbusUplinkConverter(device)
                if self.__config.get(DOWNLINK_PREFIX + CONVERTER_PARAMETER) is not None:
                    downlink_converter = TBModuleLoader.import_module(self._connector_type, self.__config[DOWNLINK_PREFIX + CONVERTER_PARAMETER])(device)
                else:
                    downlink_converter = BytesModbusDownlinkConverter(device)
                if device.get(DEVICE_NAME_PARAMETER) not in self.__gateway.get_devices():
                    self.__gateway.add_device(device.get(DEVICE_NAME_PARAMETER), {CONNECTOR_PARAMETER: self}, device_type=device.get(DEVICE_TYPE_PARAMETER))
                self.__devices[device[DEVICE_NAME_PARAMETER]] = {CONFIG_SECTION_PARAMETER: device,
                                                                 UPLINK_PREFIX + CONVERTER_PARAMETER: converter,
                                                                 DOWNLINK_PREFIX + CONVERTER_PARAMETER: downlink_converter,
                                                                 NEXT_PREFIX + ATTRIBUTES_PARAMETER + CHECK_POSTFIX: 0,
                                                                 NEXT_PREFIX + TIMESERIES_PARAMETER + CHECK_POSTFIX: 0,
                                                                 TELEMETRY_PARAMETER: {},
                                                                 ATTRIBUTES_PARAMETER: {},
                                                                 LAST_PREFIX + TELEMETRY_PARAMETER: {},
                                                                 LAST_PREFIX + ATTRIBUTES_PARAMETER: {},
                                                                 CONNECTION_ATTEMPT_PARAMETER: 0
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
            device_responses = {TIMESERIES_PARAMETER: {},
                                ATTRIBUTES_PARAMETER: {},
                                }
            to_send = {}
            try:
                for config_section in device_responses:
                    if self.__devices[device][CONFIG_SECTION_PARAMETER].get(config_section) is not None:
                        current_device_config = self.__devices[device][CONFIG_SECTION_PARAMETER]
                        unit_id = current_device_config[UNIT_ID_PARAMETER]
                        if self.__devices[device][NEXT_PREFIX + config_section + CHECK_POSTFIX] < current_time:
                            self.__connect_to_current_master(device)
                            if not self.__current_master.is_socket_open() or not len(current_device_config[config_section]):
                                continue
                            #  Reading data from device
                            for interested_data in range(len(current_device_config[config_section])):
                                current_data = current_device_config[config_section][interested_data]
                                current_data[DEVICE_NAME_PARAMETER] = device
                                input_data = self.__function_to_device(current_data, unit_id)
                                device_responses[config_section][current_data[TAG_PARAMETER]] = {"data_sent": current_data,
                                                                                                 "input_data": input_data}

                            log.debug("Checking %s for device %s", config_section, device)
                            self.__devices[device][NEXT_PREFIX + config_section + CHECK_POSTFIX] = current_time + current_device_config[
                                config_section + POLL_PERIOD_POSTFIX] / 1000
                            log.debug(device_responses)
                            converted_data = {}
                            try:
                                converted_data = self.__devices[device][UPLINK_PREFIX + CONVERTER_PARAMETER].convert(config={
                                    **current_device_config,
                                    BYTE_ORDER_PARAMETER: current_device_config.get(BYTE_ORDER_PARAMETER, self.__byte_order),
                                    WORD_ORDER_PARAMETER: current_device_config.get(WORD_ORDER_PARAMETER, self.__word_order)
                                    }, 
                                    data=device_responses)
                            except Exception as e:
                                log.error(e)
                            if len(converted_data[ATTRIBUTES_PARAMETER]) == 0 and len(converted_data[TELEMETRY_PARAMETER]) == 0:
                                log.warn("Converted data is empty!")
                                continue
                            to_send = {DEVICE_NAME_PARAMETER: converted_data[DEVICE_NAME_PARAMETER],
                                       DEVICE_TYPE_PARAMETER: converted_data[DEVICE_TYPE_PARAMETER],
                                       TELEMETRY_PARAMETER: [],
                                       ATTRIBUTES_PARAMETER: []
                                       }
                            if current_device_config.get(SEND_DATA_ONLY_ON_CHANGE_PARAMETER):
                                self.statistics[STATISTIC_MESSAGE_RECEIVED_PARAMETER] += 1
                                for converted_data_section in CONVERTED_DATA_SECTIONS:
                                    for current_section_dict in converted_data[converted_data_section]:
                                        for key, value in current_section_dict.items():
                                            if self.__devices[device][LAST_PREFIX + converted_data_section].get(key) is None or \
                                                    self.__devices[device][LAST_PREFIX + converted_data_section][key] != value:
                                                self.__devices[device][LAST_PREFIX + converted_data_section][key] = value
                                                to_send[converted_data_section].append({key: value})
                                if not to_send.get(ATTRIBUTES_PARAMETER) and not to_send.get(TELEMETRY_PARAMETER):
                                    log.debug("Data has not been changed.")
                                    continue
                            elif converted_data and current_device_config.get(SEND_DATA_ONLY_ON_CHANGE_PARAMETER) is None or \
                                    not current_device_config.get(SEND_DATA_ONLY_ON_CHANGE_PARAMETER):
                                self.statistics[STATISTIC_MESSAGE_RECEIVED_PARAMETER] += 1
                                for converted_data_section in CONVERTED_DATA_SECTIONS:
                                    self.__devices[device][LAST_PREFIX + converted_data_section] = converted_data[converted_data_section]
                                    to_send[converted_data_section] = converted_data[converted_data_section]

                if to_send.get(ATTRIBUTES_PARAMETER) or to_send.get(TELEMETRY_PARAMETER):
                    self.__gateway.send_to_storage(self.get_name(), to_send)
                    self.statistics[STATISTIC_MESSAGE_SENT_PARAMETER] += 1
            except ConnectionException:
                time.sleep(5)
                log.error("Connection lost! Reconnecting...")
            except Exception as e:
                log.exception(e)

    def on_attributes_update(self, content):
        try:
            for attribute_updates_command_config in self.__devices[content[DEVICE_SECTION_PARAMETER]][CONFIG_SECTION_PARAMETER]["attributeUpdates"]:
                for attribute_updated in content[DATA_PARAMETER]:
                    if attribute_updates_command_config[TAG_PARAMETER] == attribute_updated:
                        to_process = {
                            DEVICE_SECTION_PARAMETER: content[DEVICE_SECTION_PARAMETER],
                            DATA_PARAMETER: {
                                RPC_METHOD_PARAMETER: attribute_updated,
                                RPC_PARAMS_PARAMETER: content[DATA_PARAMETER][attribute_updated]
                                }
                            }
                        self.__process_rpc_request(to_process, attribute_updates_command_config)
        except Exception as e:
            log.exception(e)

    def __connect_to_current_master(self, device=None):
        connect_attempt_count = 5
        connect_attempt_time_ms = 100
        wait_after_failed_attempts_ms = 300000
        if device is None:
            device = list(self.__devices.keys())[0]
        if self.__devices[device].get(MASTER_PARAMETER) is None:
            self.__devices[device][MASTER_PARAMETER], self.__devices[device][AVAILABLE_FUNCTIONS_PARAMETER] = self.__configure_master(
                self.__devices[device][CONFIG_SECTION_PARAMETER])
        if self.__devices[device][MASTER_PARAMETER] != self.__current_master:
            self.__current_master = self.__devices[device][MASTER_PARAMETER]
            self.__available_functions = self.__devices[device][AVAILABLE_FUNCTIONS_PARAMETER]
        connect_attempt_count = self.__devices[device][CONFIG_SECTION_PARAMETER].get(CONNECT_ATTEMPT_COUNT_PARAMETER, connect_attempt_count)
        if connect_attempt_count < 1:
            connect_attempt_count = 1
        connect_attempt_time_ms = self.__devices[device][CONFIG_SECTION_PARAMETER].get(CONNECT_ATTEMPT_TIME_MS_PARAMETER, connect_attempt_time_ms)
        if connect_attempt_time_ms < 500:
            connect_attempt_time_ms = 500
        wait_after_failed_attempts_ms = self.__devices[device][CONFIG_SECTION_PARAMETER].get(WAIT_AFTER_FAILED_ATTEMPTS_MS_PARAMETER, wait_after_failed_attempts_ms)
        if wait_after_failed_attempts_ms < 1000:
            wait_after_failed_attempts_ms = 1000
        current_time = time.time() * 1000
        if not self.__current_master.is_socket_open():
            if self.__devices[device][CONNECTION_ATTEMPT_PARAMETER] >= connect_attempt_count and \
                    current_time - self.__devices[device][LAST_CONNECTION_ATTEMPT_TIME_PARAMETER] >= wait_after_failed_attempts_ms:
                self.__devices[device][CONNECTION_ATTEMPT_PARAMETER] = 0
            while not self.__current_master.is_socket_open() \
                    and self.__devices[device][CONNECTION_ATTEMPT_PARAMETER] < connect_attempt_count \
                    and current_time - self.__devices[device].get(LAST_CONNECTION_ATTEMPT_TIME_PARAMETER, 0) >= connect_attempt_time_ms:
                self.__devices[device][CONNECTION_ATTEMPT_PARAMETER] = self.__devices[device][CONNECTION_ATTEMPT_PARAMETER] + 1
                self.__devices[device][LAST_CONNECTION_ATTEMPT_TIME_PARAMETER] = current_time
                log.debug("Modbus trying connect to %s", device)
                self.__current_master.connect()
                if self.__devices[device][CONNECTION_ATTEMPT_PARAMETER] == connect_attempt_count:
                    log.warn("Maximum attempt count (%i) for device \"%s\" - encountered.", connect_attempt_count, device)
                #     time.sleep(connect_attempt_time_ms / 1000)
                # if not self.__current_master.is_socket_open():
        if self.__devices[device][CONNECTION_ATTEMPT_PARAMETER] >= 0 and self.__current_master.is_socket_open():
            self.__devices[device][CONNECTION_ATTEMPT_PARAMETER] = 0
            self.__devices[device][LAST_CONNECTION_ATTEMPT_TIME_PARAMETER] = current_time
            log.debug("Modbus connected to device %s.", device)

    def __configure_master(self, config=None):
        current_config = self.__config if config is None else config

        host = current_config[HOST_PARAMETER] if current_config.get(HOST_PARAMETER) is not None else self.__config.get(HOST_PARAMETER, "localhost")
        try:
            port = int(current_config[PORT_PARAMETER]) if current_config.get(PORT_PARAMETER) is not None else self.__config.get(int(PORT_PARAMETER), 502)
        except ValueError:
            port = current_config[PORT_PARAMETER] if current_config.get(PORT_PARAMETER) is not None else self.__config.get(PORT_PARAMETER, 502)
        baudrate = current_config[BAUDRATE_PARAMETER] if current_config.get(BAUDRATE_PARAMETER) is not None else self.__config.get(BAUDRATE_PARAMETER, 19200)
        timeout = current_config[TIMEOUT_PARAMETER] if current_config.get(TIMEOUT_PARAMETER) is not None else self.__config.get(TIMEOUT_PARAMETER, 35)
        method = current_config[METHOD_PARAMETER] if current_config.get(METHOD_PARAMETER) is not None else self.__config.get(METHOD_PARAMETER, "rtu")
        stopbits = current_config[STOPBITS_PARAMETER] if current_config.get(STOPBITS_PARAMETER) is not None else self.__config.get(STOPBITS_PARAMETER, Defaults.Stopbits)
        bytesize = current_config[BYTESIZE_PARAMETER] if current_config.get(BYTESIZE_PARAMETER) is not None else self.__config.get(BYTESIZE_PARAMETER, Defaults.Bytesize)
        parity = current_config[PARITY_PARAMETER] if current_config.get(PARITY_PARAMETER) is not None else self.__config.get(PARITY_PARAMETER, Defaults.Parity)
        strict = current_config[STRICT_PARAMETER] if current_config.get(STRICT_PARAMETER) is not None else self.__config.get(STRICT_PARAMETER, True)
        rtu = ModbusRtuFramer if current_config.get(METHOD_PARAMETER) == "rtu" or (
                    current_config.get(METHOD_PARAMETER) is None and self.__config.get(METHOD_PARAMETER) == "rtu") else ModbusSocketFramer
        if current_config.get(TYPE_PARAMETER) == 'tcp' or (current_config.get(TYPE_PARAMETER) is None and self.__config.get(TYPE_PARAMETER) == "tcp"):
            master = ModbusTcpClient(host, port, rtu, timeout=timeout)
        elif current_config.get(TYPE_PARAMETER) == 'udp' or (current_config.get(TYPE_PARAMETER) is None and self.__config.get(TYPE_PARAMETER) == "udp"):
            master = ModbusUdpClient(host, port, rtu, timeout=timeout)
        elif current_config.get(TYPE_PARAMETER) == 'serial' or (current_config.get(TYPE_PARAMETER) is None and self.__config.get(TYPE_PARAMETER) == "serial"):
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
            self.__devices[device][MASTER_PARAMETER].close()

    def __function_to_device(self, config, unit_id):
        function_code = config.get(FUNCTION_CODE_PARAMETER)
        result = None
        if function_code in (1, 2, 3, 4):
            result = self.__available_functions[function_code](address=config[ADDRESS_PARAMETER],
                                                               count=config.get(OBJECTS_COUNT_PARAMETER, config.get("registersCount", config.get("registerCount", 1))),
                                                               unit=unit_id)
        elif function_code in (5, 6):
            result = self.__available_functions[function_code](address=config[ADDRESS_PARAMETER],
                                                               value=config[PAYLOAD_PARAMETER],
                                                               unit=unit_id)
        elif function_code in (15, 16):
            result = self.__available_functions[function_code](address=config[ADDRESS_PARAMETER],
                                                               values=config[PAYLOAD_PARAMETER],
                                                               unit=unit_id)
        else:
            log.error("Unknown Modbus function with code: %i", function_code)
        log.debug("With result %s", str(result))
        if "Exception" in str(result):
            log.exception(result)
        return result

    def server_side_rpc_handler(self, server_rpc_request):
        try:
            if server_rpc_request.get(DEVICE_SECTION_PARAMETER) is not None:

                log.debug("Modbus connector received rpc request for %s with server_rpc_request: %s",
                          server_rpc_request[DEVICE_SECTION_PARAMETER],
                          server_rpc_request)
                if isinstance(self.__devices[server_rpc_request[DEVICE_SECTION_PARAMETER]][CONFIG_SECTION_PARAMETER][RPC_SECTION], dict):
                    rpc_command_config = self.__devices[server_rpc_request[DEVICE_SECTION_PARAMETER]][CONFIG_SECTION_PARAMETER][RPC_SECTION].get(
                        server_rpc_request[DATA_PARAMETER][RPC_METHOD_PARAMETER])
                    if rpc_command_config is not None:
                        self.__process_rpc_request(server_rpc_request, rpc_command_config)
                elif isinstance(self.__devices[server_rpc_request[DEVICE_SECTION_PARAMETER]][CONFIG_SECTION_PARAMETER][RPC_SECTION], list):
                    for rpc_command_config in self.__devices[server_rpc_request[DEVICE_SECTION_PARAMETER]][CONFIG_SECTION_PARAMETER][RPC_SECTION]:
                        if rpc_command_config[TAG_PARAMETER] == server_rpc_request[DATA_PARAMETER][RPC_METHOD_PARAMETER]:
                            self.__process_rpc_request(server_rpc_request, rpc_command_config)
                            break
                else:
                    log.error("Received rpc request, but method %s not found in config for %s.",
                              server_rpc_request[DATA_PARAMETER].get(RPC_METHOD_PARAMETER),
                              self.get_name())
                    self.__gateway.send_rpc_reply(server_rpc_request[DEVICE_SECTION_PARAMETER],
                                                  server_rpc_request[DATA_PARAMETER][RPC_ID_PARAMETER],
                                                  {server_rpc_request[DATA_PARAMETER][RPC_METHOD_PARAMETER]: "METHOD NOT FOUND!"})
            else:
                log.debug("Received RPC to connector: %r", server_rpc_request)
        except Exception as e:
            log.exception(e)

    def __process_rpc_request(self, content, rpc_command_config):
        if rpc_command_config is not None:
            rpc_command_config[UNIT_ID_PARAMETER] = self.__devices[content[DEVICE_SECTION_PARAMETER]][CONFIG_SECTION_PARAMETER][UNIT_ID_PARAMETER]
            self.__connect_to_current_master(content[DEVICE_SECTION_PARAMETER])
            # if rpc_command_config.get('bit') is not None:
            #     rpc_command_config[FUNCTION_CODE_PARAMETER] = 6
            if rpc_command_config.get(FUNCTION_CODE_PARAMETER) in (5, 6, 15, 16):
                rpc_command_config[PAYLOAD_PARAMETER] = self.__devices[content[DEVICE_SECTION_PARAMETER]][DOWNLINK_PREFIX + CONVERTER_PARAMETER].convert(
                    rpc_command_config, content)
            response = None
            try:
                response = self.__function_to_device(rpc_command_config, rpc_command_config[UNIT_ID_PARAMETER])
            except Exception as e:
                log.exception(e)
                response = e
            if isinstance(response, (ReadRegistersResponseBase, ReadBitsResponseBase)):
                to_converter = {RPC_SECTION: {content[DATA_PARAMETER][RPC_METHOD_PARAMETER]: {"data_sent": rpc_command_config,
                                                                                              "input_data": response}}}
                response = self.__devices[content[DEVICE_SECTION_PARAMETER]][UPLINK_PREFIX + CONVERTER_PARAMETER].convert(
                    config={**self.__devices[content[DEVICE_SECTION_PARAMETER]][CONFIG_SECTION_PARAMETER],
                            BYTE_ORDER_PARAMETER: self.__devices[content[DEVICE_SECTION_PARAMETER]][CONFIG_SECTION_PARAMETER].get(BYTE_ORDER_PARAMETER,
                                                                                                                                  self.__byte_order),
                            WORD_ORDER_PARAMETER: self.__devices[content[DEVICE_SECTION_PARAMETER]][CONFIG_SECTION_PARAMETER].get(WORD_ORDER_PARAMETER,
                                                                                                                                  self.__word_order)
                            },
                    data=to_converter)
                log.debug("Received RPC method: %s, result: %r", content[DATA_PARAMETER][RPC_METHOD_PARAMETER], response)
                # response = {"success": response}
            elif isinstance(response, (WriteMultipleRegistersResponse,
                                       WriteMultipleCoilsResponse,
                                       WriteSingleCoilResponse,
                                       WriteSingleRegisterResponse)):
                log.debug("Write %r", str(response))
                response = {"success": True}
            if content.get(RPC_ID_PARAMETER) or (content.get(DATA_PARAMETER) is not None and content[DATA_PARAMETER].get(RPC_ID_PARAMETER)):
                if isinstance(response, Exception):
                    self.__gateway.send_rpc_reply(content[DEVICE_SECTION_PARAMETER],
                                                  content[DATA_PARAMETER][RPC_ID_PARAMETER],
                                                  {content[DATA_PARAMETER][RPC_METHOD_PARAMETER]: str(response)})
                else:
                    self.__gateway.send_rpc_reply(content[DEVICE_SECTION_PARAMETER],
                                                  content[DATA_PARAMETER][RPC_ID_PARAMETER],
                                                  response)
            log.debug("%r", response)
