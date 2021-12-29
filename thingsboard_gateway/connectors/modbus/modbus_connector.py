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

from threading import Thread
from time import sleep, time
from queue import Queue
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
    TBUtility.install_package('twisted')
    from pymodbus.constants import Defaults

from twisted.internet import reactor
from pymodbus.bit_write_message import WriteSingleCoilResponse, WriteMultipleCoilsResponse
from pymodbus.register_write_message import WriteMultipleRegistersResponse, WriteSingleRegisterResponse
from pymodbus.register_read_message import ReadRegistersResponseBase
from pymodbus.bit_read_message import ReadBitsResponseBase
from pymodbus.client.sync import ModbusTcpClient, ModbusUdpClient, ModbusSerialClient
from pymodbus.client.sync import ModbusRtuFramer, ModbusSocketFramer, ModbusAsciiFramer
from pymodbus.exceptions import ConnectionException
from pymodbus.server.asynchronous import StartTcpServer, StartUdpServer, StartSerialServer, StopServer
from pymodbus.device import ModbusDeviceIdentification
from pymodbus.version import version
from pymodbus.datastore import ModbusSlaveContext, ModbusServerContext
from pymodbus.datastore import ModbusSparseDataBlock

from thingsboard_gateway.connectors.connector import Connector, log
from thingsboard_gateway.connectors.modbus.constants import *
from thingsboard_gateway.connectors.modbus.slave import Slave
from thingsboard_gateway.connectors.modbus.backward_compability_adapter import BackwardCompatibilityAdapter
from thingsboard_gateway.connectors.modbus.bytes_modbus_downlink_converter import BytesModbusDownlinkConverter

CONVERTED_DATA_SECTIONS = [ATTRIBUTES_PARAMETER, TELEMETRY_PARAMETER]
FRAMER_TYPE = {
    'rtu': ModbusRtuFramer,
    'socket': ModbusSocketFramer,
    'ascii': ModbusAsciiFramer
}
SLAVE_TYPE = {
    'tcp': StartTcpServer,
    'udp': StartUdpServer,
    'serial': StartSerialServer
}
FUNCTION_TYPE = {
    'coils_initializer': 'ci',
    'holding_registers': 'hr',
    'input_registers': 'ir',
    'discrete_inputs': 'di'
}
FUNCTION_CODE_WRITE = {
    'holding_registers': (6, 16),
    'coils_initializer': (5, 15)
}
FUNCTION_CODE_READ = {
    'holding_registers': 3,
    'coils_initializer': 1,
    'input_registers': 4,
    'discrete_inputs': 2
}


class ModbusConnector(Connector, Thread):
    process_requests = Queue(-1)

    def __init__(self, gateway, config, connector_type):
        self.statistics = {STATISTIC_MESSAGE_RECEIVED_PARAMETER: 0,
                           STATISTIC_MESSAGE_SENT_PARAMETER: 0}
        super().__init__()
        self.__gateway = gateway
        self._connector_type = connector_type

        self.__backward_compatibility_adapter = BackwardCompatibilityAdapter(config)
        self.__config = self.__backward_compatibility_adapter.convert()

        self.setName(self.__config.get("name", 'Modbus Default ' + ''.join(choice(ascii_lowercase) for _ in range(5))))
        self.__connected = False
        self.__stopped = False
        self.daemon = True

        if self.__config.get('slave'):
            self.__slave_thread = Thread(target=self.__configure_and_run_slave, args=(self.__config['slave'],),
                                         daemon=True, name='Gateway as a slave')
            self.__slave_thread.start()

            if config['slave'].get('sendDataToThingsBoard', False):
                self.__modify_main_config()

        self.__slaves = []
        self.__load_slaves()

    def is_connected(self):
        return self.__connected

    def open(self):
        self.__stopped = False
        self.start()

    def run(self):
        self.__connected = True

        while True:
            if not self.__stopped and not ModbusConnector.process_requests.empty():
                thread = Thread(target=self.__process_slaves, daemon=True)
                thread.start()

            if self.__stopped:
                break

            sleep(.2)

    @staticmethod
    def __configure_and_run_slave(config):
        identity = None
        if config.get('identity'):
            identity = ModbusDeviceIdentification()
            identity.VendorName = config['identity'].get('vendorName', '')
            identity.ProductCode = config['identity'].get('productCode', '')
            identity.VendorUrl = config['identity'].get('vendorUrl', '')
            identity.ProductName = config['identity'].get('productName', '')
            identity.ModelName = config['identity'].get('ModelName', '')
            identity.MajorMinorRevision = version.short()

        blocks = {}
        for (key, value) in config.get('values').items():
            values = {}
            converter = BytesModbusDownlinkConverter({})
            for item in value:
                for section in ('attributes', 'timeseries', 'attributeUpdates', 'rpc'):
                    for val in item[section]:
                        function_code = FUNCTION_CODE_WRITE[key][0] if val['objectsCount'] <= 1 else \
                            FUNCTION_CODE_WRITE[key][1]
                        converted_value = converter.convert(
                            {**val,
                             'device': config.get('deviceName', 'Gateway'), 'functionCode': function_code,
                             'byteOrder': config['byteOrder']},
                            {'data': {'params': val['value']}})
                        values[val['address']] = converted_value

            blocks[FUNCTION_TYPE[key]] = ModbusSparseDataBlock(values)

        context = ModbusServerContext(slaves=ModbusSlaveContext(**blocks), single=True)
        SLAVE_TYPE[config['type']](context, identity=identity,
                                   address=(config.get('host'), config.get('port')) if (
                                           config['type'] == 'tcp' or 'udp') else None,
                                   port=config.get('port') if config['type'] == 'serial' else None,
                                   framer=FRAMER_TYPE[config['method']])

    def __modify_main_config(self):
        config = self.__config['slave']

        values = config.pop('values')
        device = config

        for (register, reg_values) in values.items():
            for value in reg_values:
                for section in ('attributes', 'timeseries', 'attributeUpdates', 'rpc'):
                    if not device.get(section):
                        device[section] = []

                    for item in value.get(section, []):
                        device[section].append({**item, 'functionCode': FUNCTION_CODE_READ[register]})

        self.__config['master']['slaves'].append(device)

    def __load_slaves(self):
        self.__slaves = [
            Slave(**{**device, 'connector': self, 'gateway': self.__gateway, 'callback': ModbusConnector.callback}) for
            device in self.__config.get('master', {'slaves': []}).get('slaves', [])]

    @classmethod
    def callback(cls, slave):
        cls.process_requests.put(slave)

    @property
    def connector_type(self):
        return self._connector_type

    def __convert_and_save_data(self, config_tuple):
        device, current_device_config, config, device_responses = config_tuple
        converted_data = {}

        try:
            converted_data = device.config[UPLINK_PREFIX + CONVERTER_PARAMETER].convert(
                config=config,
                data=device_responses)
        except Exception as e:
            log.error(e)

        to_send = {DEVICE_NAME_PARAMETER: converted_data[DEVICE_NAME_PARAMETER],
                   DEVICE_TYPE_PARAMETER: converted_data[DEVICE_TYPE_PARAMETER],
                   TELEMETRY_PARAMETER: [],
                   ATTRIBUTES_PARAMETER: []
                   }

        if current_device_config.get('sendDataOnlyOnChange'):
            self.statistics[STATISTIC_MESSAGE_RECEIVED_PARAMETER] += 1

            for converted_data_section in CONVERTED_DATA_SECTIONS:
                for current_section_dict in converted_data[converted_data_section]:
                    for key, value in current_section_dict.items():
                        if device.config[LAST_PREFIX + converted_data_section].get(key) is None or \
                                device.config[LAST_PREFIX + converted_data_section][key] != value:
                            device.config[LAST_PREFIX + converted_data_section][key] = value
                            to_send[converted_data_section].append({key: value})
        elif converted_data and current_device_config.get('sendDataOnlyOnChange') is None or \
                not current_device_config.get('sendDataOnlyOnChange'):
            self.statistics[STATISTIC_MESSAGE_RECEIVED_PARAMETER] += 1

            for converted_data_section in CONVERTED_DATA_SECTIONS:
                device.config[LAST_PREFIX + converted_data_section] = converted_data[
                    converted_data_section]
                to_send[converted_data_section] = converted_data[converted_data_section]

        if to_send.get(ATTRIBUTES_PARAMETER) or to_send.get(TELEMETRY_PARAMETER):
            self.__gateway.send_to_storage(self.get_name(), to_send)
            self.statistics[STATISTIC_MESSAGE_SENT_PARAMETER] += 1

    def close(self):
        self.__stopped = True
        self.__stop_connections_to_masters()
        if reactor.running:
            StopServer()
        log.info('%s has been stopped.', self.get_name())

    def get_name(self):
        return self.name

    def __process_slaves(self):
        # TODO: write documentation
        device = ModbusConnector.process_requests.get()

        device_responses = {'timeseries': {}, 'attributes': {}}
        current_device_config = {}
        try:
            for config_section in device_responses:
                if device.config.get(config_section) is not None:
                    current_device_config = device.config

                    self.__connect_to_current_master(device)

                    if not device.config['master'].is_socket_open() or not len(
                            current_device_config[config_section]):
                        continue

                    # Reading data from device
                    for interested_data in range(len(current_device_config[config_section])):
                        current_data = current_device_config[config_section][interested_data]
                        current_data[DEVICE_NAME_PARAMETER] = device
                        input_data = self.__function_to_device(device, current_data)
                        device_responses[config_section][current_data[TAG_PARAMETER]] = {
                            "data_sent": current_data,
                            "input_data": input_data}

                    log.debug("Checking %s for device %s", config_section, device)
                    log.debug('Device response: ', device_responses)

            if device_responses.get('timeseries') or device_responses.get('attributes'):
                self.__convert_and_save_data((device, current_device_config, {
                    **current_device_config,
                    BYTE_ORDER_PARAMETER: current_device_config.get(BYTE_ORDER_PARAMETER,
                                                                    device.byte_order),
                    WORD_ORDER_PARAMETER: current_device_config.get(WORD_ORDER_PARAMETER,
                                                                    device.word_order)
                }, device_responses))

        except ConnectionException:
            sleep(5)
            log.error("Connection lost! Reconnecting...")
        except Exception as e:
            log.exception(e)

    def __connect_to_current_master(self, device=None):
        # TODO: write documentation
        connect_attempt_count = 5
        connect_attempt_time_ms = 100
        wait_after_failed_attempts_ms = 300000

        if device.config.get('master') is None:
            device.config['master'], device.config['available_functions'] = self.__configure_master(device.config)

        if connect_attempt_count < 1:
            connect_attempt_count = 1

        connect_attempt_time_ms = device.config.get('connectAttemptTimeMs', connect_attempt_time_ms)

        if connect_attempt_time_ms < 500:
            connect_attempt_time_ms = 500

        wait_after_failed_attempts_ms = device.config.get('waitAfterFailedAttemptsMs', wait_after_failed_attempts_ms)

        if wait_after_failed_attempts_ms < 1000:
            wait_after_failed_attempts_ms = 1000

        current_time = time() * 1000

        if not device.config['master'].is_socket_open():
            if device.config['connection_attempt'] >= connect_attempt_count and current_time - device.config[
                    'last_connection_attempt_time'] >= wait_after_failed_attempts_ms:
                device.config['connection_attempt'] = 0

            while not device.config['master'].is_socket_open() \
                    and device.config['connection_attempt'] < connect_attempt_count \
                    and current_time - device.config.get('last_connection_attempt_time',
                                                         0) >= connect_attempt_time_ms:
                device.config['connection_attempt'] = device.config[
                                                          'connection_attempt'] + 1
                device.config['last_connection_attempt_time'] = current_time
                log.debug("Modbus trying connect to %s", device)
                device.config['master'].connect()

                if device.config['connection_attempt'] == connect_attempt_count:
                    log.warn("Maximum attempt count (%i) for device \"%s\" - encountered.", connect_attempt_count,
                             device)

        if device.config['connection_attempt'] >= 0 and device.config['master'].is_socket_open():
            device.config['connection_attempt'] = 0
            device.config['last_connection_attempt_time'] = current_time

    @staticmethod
    def __configure_master(config):
        current_config = config
        current_config["rtu"] = FRAMER_TYPE[current_config['method']]

        if current_config.get('type') == 'tcp':
            master = ModbusTcpClient(current_config["host"],
                                     current_config["port"],
                                     current_config["rtu"],
                                     timeout=current_config["timeout"],
                                     retry_on_empty=current_config["retry_on_empty"],
                                     retry_on_invalid=current_config["retry_on_invalid"],
                                     retries=current_config["retries"])
        elif current_config.get(TYPE_PARAMETER) == 'udp':
            master = ModbusUdpClient(current_config["host"],
                                     current_config["port"],
                                     current_config["rtu"],
                                     timeout=current_config["timeout"],
                                     retry_on_empty=current_config["retry_on_empty"],
                                     retry_on_invalid=current_config["retry_on_invalid"],
                                     retries=current_config["retries"])
        elif current_config.get(TYPE_PARAMETER) == 'serial':
            master = ModbusSerialClient(method=current_config["method"],
                                        port=current_config["port"],
                                        timeout=current_config["timeout"],
                                        retry_on_empty=current_config["retry_on_empty"],
                                        retry_on_invalid=current_config["retry_on_invalid"],
                                        retries=current_config["retries"],
                                        baudrate=current_config["baudrate"],
                                        stopbits=current_config["stopbits"],
                                        bytesize=current_config["bytesize"],
                                        parity=current_config["parity"],
                                        strict=current_config["strict"])
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
        for slave in self.__slaves:
            if slave.config.get('master') is not None and slave.config.get('master').is_socket_open():
                slave.config['master'].close()

    @staticmethod
    def __function_to_device(device, config):
        function_code = config.get('functionCode')
        result = None
        if function_code in (1, 2, 3, 4):
            result = device.config['available_functions'][function_code](address=config[ADDRESS_PARAMETER],
                                                                         count=config.get(OBJECTS_COUNT_PARAMETER,
                                                                                          config.get("registersCount",
                                                                                                     config.get(
                                                                                                         "registerCount",
                                                                                                         1))),
                                                                         unit=device.config['unitId'])
        elif function_code in (5, 6):
            result = device.config['available_functions'][function_code](address=config[ADDRESS_PARAMETER],
                                                                         value=config[PAYLOAD_PARAMETER],
                                                                         unit=device.config['unitId'])
        elif function_code in (15, 16):
            result = device.config['available_functions'][function_code](address=config[ADDRESS_PARAMETER],
                                                                         values=config[PAYLOAD_PARAMETER],
                                                                         unit=device.config['unitId'])
        else:
            log.error("Unknown Modbus function with code: %s", function_code)

        log.debug("With result %s", str(result))

        if "Exception" in str(result):
            log.exception(result)

        return result

    def on_attributes_update(self, content):
        try:
            device = tuple(filter(lambda slave: slave.name == content[DEVICE_SECTION_PARAMETER], self.__slaves))[0]

            for attribute_updates_command_config in device.config['attributeUpdates']:
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

    def server_side_rpc_handler(self, server_rpc_request):
        try:
            if server_rpc_request.get(DEVICE_SECTION_PARAMETER) is not None:
                log.debug("Modbus connector received rpc request for %s with server_rpc_request: %s",
                          server_rpc_request[DEVICE_SECTION_PARAMETER],
                          server_rpc_request)
                device = tuple(
                    filter(
                        lambda slave: slave.name == server_rpc_request[DEVICE_SECTION_PARAMETER], self.__slaves
                    )
                )[0]

                if isinstance(device.config[RPC_SECTION], dict):
                    rpc_command_config = device.config[RPC_SECTION].get(
                        server_rpc_request[DATA_PARAMETER][RPC_METHOD_PARAMETER])

                    if rpc_command_config is not None:
                        self.__process_rpc_request(server_rpc_request, rpc_command_config)

                elif isinstance(device.config[RPC_SECTION], list):
                    for rpc_command_config in device.config[RPC_SECTION]:
                        if rpc_command_config[TAG_PARAMETER] == server_rpc_request[DATA_PARAMETER][
                                RPC_METHOD_PARAMETER]:
                            self.__process_rpc_request(server_rpc_request, rpc_command_config)
                            break
                else:
                    log.error("Received rpc request, but method %s not found in config for %s.",
                              server_rpc_request[DATA_PARAMETER].get(RPC_METHOD_PARAMETER),
                              self.get_name())
                    self.__gateway.send_rpc_reply(server_rpc_request[DEVICE_SECTION_PARAMETER],
                                                  server_rpc_request[DATA_PARAMETER][RPC_ID_PARAMETER],
                                                  {server_rpc_request[DATA_PARAMETER][
                                                       RPC_METHOD_PARAMETER]: "METHOD NOT FOUND!"})
            else:
                log.debug("Received RPC to connector: %r", server_rpc_request)
        except Exception as e:
            log.exception(e)

    def __process_rpc_request(self, content, rpc_command_config):
        if rpc_command_config is not None:
            device = tuple(filter(lambda slave: slave.name == content[DEVICE_SECTION_PARAMETER], self.__slaves))[0]
            rpc_command_config[UNIT_ID_PARAMETER] = device.config['unitId']
            self.__connect_to_current_master(device)

            if rpc_command_config.get(FUNCTION_CODE_PARAMETER) in (5, 6, 15, 16):
                converted_data = device.config[DOWNLINK_PREFIX + CONVERTER_PARAMETER].convert(rpc_command_config,
                                                                                              content)
                try:
                    rpc_command_config[PAYLOAD_PARAMETER] = converted_data[0]
                except IndexError:
                    rpc_command_config[PAYLOAD_PARAMETER] = converted_data

            try:
                response = self.__function_to_device(device, rpc_command_config)
            except Exception as e:
                log.exception(e)
                response = e

            if isinstance(response, (ReadRegistersResponseBase, ReadBitsResponseBase)):
                to_converter = {
                    RPC_SECTION: {content[DATA_PARAMETER][RPC_METHOD_PARAMETER]: {"data_sent": rpc_command_config,
                                                                                  "input_data": response}}}
                response = device.config[
                    UPLINK_PREFIX + CONVERTER_PARAMETER].convert(
                    config={**device.config,
                            BYTE_ORDER_PARAMETER: device.byte_order,
                            WORD_ORDER_PARAMETER: device.word_order
                            },
                    data=to_converter)
                log.debug("Received RPC method: %s, result: %r", content[DATA_PARAMETER][RPC_METHOD_PARAMETER],
                          response)
            elif isinstance(response, (WriteMultipleRegistersResponse,
                                       WriteMultipleCoilsResponse,
                                       WriteSingleCoilResponse,
                                       WriteSingleRegisterResponse)):
                log.debug("Write %r", str(response))
                response = {"success": True}

            if content.get(RPC_ID_PARAMETER) or (
                    content.get(DATA_PARAMETER) is not None and content[DATA_PARAMETER].get(RPC_ID_PARAMETER)):
                if isinstance(response, Exception):
                    self.__gateway.send_rpc_reply(content[DEVICE_SECTION_PARAMETER],
                                                  content[DATA_PARAMETER][RPC_ID_PARAMETER],
                                                  {content[DATA_PARAMETER][RPC_METHOD_PARAMETER]: str(response)})
                else:
                    self.__gateway.send_rpc_reply(content[DEVICE_SECTION_PARAMETER],
                                                  content[DATA_PARAMETER][RPC_ID_PARAMETER],
                                                  response)

            log.debug("%r", response)
