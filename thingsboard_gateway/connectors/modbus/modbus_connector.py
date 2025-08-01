#     Copyright 2025. ThingsBoard
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

import asyncio
from asyncio import CancelledError, Queue as AsyncQueue, QueueEmpty
from queue import Queue, Empty
from threading import Thread
from random import choice
from string import ascii_lowercase
from time import monotonic, sleep

from packaging import version

from thingsboard_gateway.connectors.modbus.constants import ADDRESS_PARAMETER, TAG_PARAMETER, \
    FUNCTION_CODE_PARAMETER
from thingsboard_gateway.connectors.modbus.entities.rpc_request import RPCRequest, RPCType
from thingsboard_gateway.gateway.entities.converted_data import ConvertedData
from thingsboard_gateway.gateway.statistics.statistics_service import StatisticsService
from thingsboard_gateway.tb_utility.tb_utility import TBUtility
from thingsboard_gateway.connectors.connector import Connector
from thingsboard_gateway.gateway.constants import STATISTIC_MESSAGE_RECEIVED_PARAMETER, \
    STATISTIC_MESSAGE_SENT_PARAMETER, CONNECTOR_PARAMETER, DEVICE_SECTION_PARAMETER, DATA_PARAMETER, \
    RPC_METHOD_PARAMETER, RPC_PARAMS_PARAMETER
from thingsboard_gateway.tb_utility.tb_logger import init_logger

# Try import Pymodbus library or install it and import
installation_required = False
required_version = '3.9.2'
force_install = False

try:
    from pymodbus import __version__ as pymodbus_version

    if version.parse(pymodbus_version) < version.parse(required_version):
        installation_required = True

    if version.parse(
            pymodbus_version) > version.parse(required_version):
        installation_required = True
        force_install = True

    from serial import SerialException  # noqa
    from serial_asyncio import create_serial_connection  # noqa

except ImportError:
    installation_required = True

if installation_required:
    print("Modbus library not found - installing...")
    TBUtility.install_package("pymodbus", required_version, force_install=force_install)
    TBUtility.install_package('pyserial')
    TBUtility.install_package('pyserial-asyncio')

from thingsboard_gateway.connectors.modbus.entities.master import Master  # noqa: E402
from thingsboard_gateway.connectors.modbus.server import Server  # noqa: E402
from thingsboard_gateway.connectors.modbus.slave import Slave  # noqa: E402
from thingsboard_gateway.connectors.modbus.entities.bytes_downlink_converter_config import \
    BytesDownlinkConverterConfig  # noqa: E402
from thingsboard_gateway.connectors.modbus.backward_compatibility_adapter import BackwardCompatibilityAdapter  # noqa

from pymodbus.exceptions import ConnectionException  # noqa: E402
from pymodbus.pdu import ModbusPDU  # noqa
from pymodbus.pdu.bit_message import WriteMultipleCoilsResponse, WriteSingleCoilResponse  # noqa: E402
from pymodbus.constants import Endian  # noqa: E402
from pymodbus.pdu import ExceptionResponse  # noqa: E402
from pymodbus.pdu.register_message import WriteMultipleRegistersResponse, WriteSingleRegisterResponse  # noqa: E402


class AsyncModbusConnector(Connector, Thread):
    def __init__(self, gateway, config, connector_type):
        self.statistics = {STATISTIC_MESSAGE_RECEIVED_PARAMETER: 0,
                           STATISTIC_MESSAGE_SENT_PARAMETER: 0}
        super().__init__()
        self.__gateway = gateway
        self._connector_type = connector_type
        self.name = config.get("name", 'Modbus Connector ' + ''.join(choice(ascii_lowercase) for _ in range(5)))
        self.__log = init_logger(self.__gateway, config.get('name', self.name),
                                 config.get('logLevel', 'INFO'),
                                 enable_remote_logging=config.get('enableRemoteLogging', False),
                                 is_connector_logger=True)
        self.converter_log = init_logger(self.__gateway, self.name + '_converter',
                                         config.get('logLevel', 'INFO'),
                                         enable_remote_logging=config.get('enableRemoteLogging', False),
                                         is_converter_logger=True, attr_name=self.name)
        self.__backward_compatibility_adapter = BackwardCompatibilityAdapter(config, gateway.get_config_path(),
                                                                             logger=self.__log)
        self.__config = self.__backward_compatibility_adapter.convert()
        self.__log.info('Starting Modbus Connector...')
        self.__id = self.__config.get('id')
        self.__connected = False
        self.__stopped = False
        self.daemon = True

        try:
            self.loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self.loop)
        except RuntimeError:
            self.loop = asyncio.get_event_loop()

        self.process_device_requests = AsyncQueue(100000)
        self.__data_to_convert = Queue(-1)
        self.__data_to_save = Queue(-1)

        self.__server = None

        self._master_connections = {}

        self.__slaves = []

    def close(self):
        self.__stopped = True
        self.__connected = False

        self.__log.debug('Stopping %s...', self.get_name())

        for slave in self.__slaves:
            slave.close(self.loop)

        if self.__server:
            self.__server.stop()

        asyncio.run_coroutine_threadsafe(self.__cancel_all_tasks(), self.loop)

        self.__check_is_alive()

        self.__log.info('%s has been stopped', self.get_name())
        self.__log.stop()

    def __check_is_alive(self):
        start_time = monotonic()

        while self.is_alive():
            if monotonic() - start_time > 10:
                self.__log.error("Failed to stop connector %s", self.get_name())
                break
            sleep(.1)

    async def __cancel_all_tasks(self):
        await asyncio.sleep(5)
        for task in asyncio.all_tasks(self.loop):
            task.cancel()

    def open(self):
        self.start()

    def run(self):
        self.__connected = True

        try:
            self.loop.run_until_complete(self.__run())
        except CancelledError as e:
            self.__log.debug('Task was cancelled due to connector stop: %s', e.__str__())
        except Exception as e:
            self.__log.exception(e)

    async def __run(self):
        # check if Gateway as a server need to be started
        if Server.is_runnable(self.__config):
            self.__run_server()

        await self.__add_slaves(self.__config.get('master', {'slaves': []}).get('slaves', []))

        await asyncio.gather(
            self.__process_requests(),
            self.__convert_data(),
            self.__save_data()
        )

    def __run_server(self):
        try:
            self.__log.info("Starting Modbus server")
            self.__server = Server(self.__config['slave'], self.__log)
            self.__server.start()
            self.__add_slave(self.__server.get_slave_config_format())
            self.__log.info("Modbus server started")
        except Exception as e:
            self.__log.exception('Failed to start Modbus server: %s', e)
            self.__connected = False

    def __get_master(self, slave: Slave):
        """
        Method check if connection to master already exists and return it
        or create new connection and return it

        Newly created master connection structure will look like:
        self._master_connections = {
            '127.0.0.1:5021': AsyncClient object (TCP/UDP),
            '/dev/ttyUSB0': AsyncClient object (Serial)
        }
        """
        if not slave.host:
            master_connection_name = slave.port
        else:
            master_connection_name = slave.host + ':' + str(slave.port)
        if master_connection_name not in self._master_connections:
            master_connection = Master.configure_master(slave)
            master = Master(slave.type, master_connection)
            self._master_connections[master_connection_name] = master

        return self._master_connections[master_connection_name]

    def __add_slave(self, slave_config):
        slave = Slave(self, self.__log, slave_config)
        master = self.__get_master(slave)
        slave.master = master

        self.__slaves.append(slave)
        slave.start()

    async def __add_slaves(self, slaves_config):
        for slave_config in slaves_config:
            try:
                self.__add_slave(slave_config)
            except Exception as e:
                self.__log.exception('Failed to add slave: %s', e)

        self.__log.debug('Added %d slaves', len(self.__slaves))

    @classmethod
    def callback(cls, slave: Slave, queue: Queue):
        queue.put_nowait(slave)

    async def __process_requests(self):
        while not self.__stopped:
            try:
                slave = self.process_device_requests.get_nowait()

                if slave.type == 'serial':
                    await self.__poll_device(slave)
                else:
                    self.loop.create_task(self.__poll_device(slave))
            except QueueEmpty:
                await asyncio.sleep(0.1)
                continue
            except Exception as e:
                self.__log.exception('Failed to poll device: %s', e)

    async def __poll_device(self, slave: Slave):
        self.__log.debug("Polling %s slave", slave)

        # check if device have attributes or telemetry to poll
        if slave.uplink_converter_config.attributes or slave.uplink_converter_config.telemetry:
            try:
                connected_to_master = await slave.connect()

                if connected_to_master:
                    self.__manage_device_connectivity_to_platform(slave)

                if connected_to_master:
                    slave_data = await self.__read_slave_data(slave)
                    self.__data_to_convert.put_nowait((slave, slave_data))
                else:
                    self.__log.error('Device %s is not connected, cannot connect to server, skipping reading...', slave)
                    self.__delete_device_from_platform(slave)
            except ConnectionException:
                self.__delete_device_from_platform(slave)
                await asyncio.sleep(5)
                self.__log.error('Failed to connect to device %s', slave)
            except asyncio.exceptions.TimeoutError:
                self.__log.error('Timeout error for device %s', slave)
                await slave.disconnect()
            except Exception as e:
                self.__delete_device_from_platform(slave)
                self.__log.error('Failed to poll %s device: %s', slave, e)
                self.__log.debug('Exception:', exc_info=e)
        else:
            self.__log.debug('Config is empty. Nothing to read, for device %s', slave)

    async def __read_slave_data(self, slave: Slave):
        result = {
            'telemetry': {},
            'attributes': {}
        }

        for config_section in ('attributes', 'telemetry'):
            for config in getattr(slave.uplink_converter_config, config_section):
                try:
                    response = await slave.read(config['functionCode'], config['address'], config['objectsCount'])
                except asyncio.exceptions.TimeoutError:
                    self.__log.error("Timeout error for device %s function code %s address %s, it may be caused by wrong data in server register.",  # noqa
                                     slave.device_name, config['functionCode'], config[ADDRESS_PARAMETER])
                    continue

                result[config_section][config['tag']] = response

        return result

    def __manage_device_connectivity_to_platform(self, slave: Slave):
        if slave.master.connected() and slave.device_name not in self.__gateway.get_devices():
            self.__add_device_to_platform(slave)

    def __delete_device_from_platform(self, slave: Slave):
        if slave.device_name in self.__gateway.get_devices():
            self.__gateway.del_device(slave.device_name)
            slave.last_connect_time = 0

    def __add_device_to_platform(self, slave: Slave):
        """
        Add device to platform
        """

        if slave.master.connected():
            device_connected = self.__gateway.add_device(slave.device_name,
                                                         {CONNECTOR_PARAMETER: self},
                                                         device_type=slave.device_type)

            slave.last_connect_time = monotonic() if device_connected else 0

    async def __convert_data(self):
        while not self.__stopped:
            try:
                batch_to_convert = {}
                if not self.__data_to_convert.empty():
                    batch_forming_start = monotonic()
                    while not self.__stopped and monotonic() - batch_forming_start < 0.1:
                        try:
                            slave, data = self.__data_to_convert.get_nowait()
                        except Empty:
                            break

                        batch_key = (slave.device_name, slave.uplink_converter)

                        if batch_key not in batch_to_convert:
                            batch_to_convert[batch_key] = []

                        batch_to_convert[batch_key].append(data)

                    for (device_name, uplink_converter), data in batch_to_convert.items():
                        converted_data: ConvertedData = uplink_converter.convert({}, data)
                        self.__log.trace("Converted data: %r", converted_data)
                        if len(converted_data['attributes']) or len(converted_data['telemetry']):
                            self.__data_to_save.put_nowait(converted_data)
                else:
                    await asyncio.sleep(.1)
            except Exception as e:
                self.__log.error('Exception in conversion data loop: %s', e)

    async def __save_data(self):
        while not self.__stopped:
            if not self.__data_to_save.empty():
                try:
                    converted_data = self.__data_to_save.get_nowait()
                    StatisticsService.count_connector_message(self.get_name(), stat_parameter_name='storageMsgPushed')
                    self.__gateway.send_to_storage(self.get_name(), self.get_id(), converted_data)
                    self.statistics[STATISTIC_MESSAGE_SENT_PARAMETER] += 1
                except Empty:
                    await asyncio.sleep(.001)
            else:
                await asyncio.sleep(.001)

    def get_device_shared_attributes_keys(self, device_name):
        device = self.__get_device_by_name(device_name)
        if device is not None:
            return device.shared_attributes_keys
        return []

    def on_attributes_update(self, content):
        self.__log.debug('Got attributes update: %s', content)

        try:
            device = self.__get_device_by_name(content[DEVICE_SECTION_PARAMETER])
            if device is None:
                self.__log.error('Device %s not found in connector %s',
                                 content[DEVICE_SECTION_PARAMETER],
                                 self.get_name())
                return

            for attribute_updates_command_config in device.attributes_updates_config:
                for attribute_updated in content[DATA_PARAMETER]:
                    if attribute_updates_command_config[TAG_PARAMETER] == attribute_updated:
                        to_process = {
                            DEVICE_SECTION_PARAMETER: content[DEVICE_SECTION_PARAMETER],
                            DATA_PARAMETER: {
                                RPC_METHOD_PARAMETER: attribute_updated,
                                RPC_PARAMS_PARAMETER: content[DATA_PARAMETER][attribute_updated]
                            }
                        }

                        self.__create_task(self.__process_attribute_request,
                                           (device, to_process,
                                            attribute_updates_command_config),
                                           {})
        except Exception as e:
            self.__log.exception('Failed to update attributes: %s', e)

    async def __process_attribute_request(self, device: Slave, data, config):
        if config is not None:
            converted_data = None

            config = BytesDownlinkConverterConfig(
                device_name=device.device_name,
                byte_order=device.byte_order,
                word_order=device.word_order,
                repack=device.repack,
                objects_count=config.get("objectsCount", config.get("registersCount", config.get("registerCount", 1))),
                function_code=config.get(FUNCTION_CODE_PARAMETER),
                lower_type=config.get("type", config.get("tag", "error")),
                address=config.get(ADDRESS_PARAMETER)
            )

            if config.function_code in (5, 6):
                converted_data = device.downlink_converter.convert(config, data)

                try:
                    converted_data = converted_data[0]
                except IndexError and TypeError:
                    pass
            elif config.function_code in (15, 16):
                converted_data = device.downlink_converter.convert(config, data)

            if converted_data is not None:
                try:
                    connected = await device.connect()
                    if connected:
                        await device.write(config.function_code, config.address, converted_data)
                    else:
                        self.__log.error(
                            'Failed to process attribute update: Socket is closed, connection is lost, for device %s',
                            device)
                except Exception as e:
                    self.__log.exception('Failed to process attribute update: ', e)

    def server_side_rpc_handler(self, content):
        self.__log.info('Received server side rpc request: %r', content)

        try:
            rpc_request = RPCRequest(content)

            response = None
            if rpc_request.rpc_type == RPCType.CONNECTOR:
                response = self.__process_connector_rpc_request(rpc_request)
            elif rpc_request.rpc_type == RPCType.RESERVED:
                response = self.__process_reserved_rpc_request(rpc_request)
            elif rpc_request.rpc_type == RPCType.DEVICE:
                response = self.__process_device_rpc_request(rpc_request)

            return response
        except Exception as e:
            self.__log.error('Failed to process server side rpc request: %s', e)
            return {'error': '%r' % e, 'success': False}

    def __process_connector_rpc_request(self, rpc_request: RPCRequest):
        self.__log.debug("Received RPC to connector: %r", rpc_request)

        if rpc_request.for_existing_device():
            results = []
            for device in self.__slaves:
                rpc_request.device_name = device.device_name
                result = {}
                self.__create_task(self.__process_rpc_request,
                                   (device, rpc_request.params, rpc_request),
                                   {'with_response': True, 'result': result},
                                   timeout=rpc_request.timeout)
                results.append(result['response'])

            return results
        else:
            slave = Slave(self, self.__log, rpc_request.params)
            master = self.__get_master(slave)
            slave.master = master
            rpc_request.device_name = slave.device_name

            connected_to_master_task = self.loop.create_task(slave.connect())
            connect_started = monotonic()
            while not connected_to_master_task.done() and not self.__stopped and \
                    monotonic() - connect_started < rpc_request.timeout:
                sleep(.02)

            if connected_to_master_task.result():
                result = {}
                self.__create_task(self.__process_rpc_request,
                                   (slave, rpc_request.params, rpc_request),
                                   {'with_response': True, 'result': result},
                                   timeout=rpc_request.timeout)
                return result['response']
            else:
                self.__log.error('Failed to connect to device %s', slave.device_name)
                return {'error': 'Failed to connect to device %s' % slave.device_name, 'success': False}

    def __process_reserved_rpc_request(self, rpc_request: RPCRequest):
        device = self.__get_device_by_name(rpc_request.device_name)
        if device is None:
            self.__log.error('Device %s not found in connector %s', rpc_request.device_name, self.get_name())
            return {'error': 'Device %s not found' % rpc_request.device_name, 'success': False}

        result = {}
        self.__create_task(self.__process_rpc_request,
                           (device, rpc_request.params, rpc_request),
                           {'result': result},
                           timeout=rpc_request.timeout)

        self.__log.debug("Result: %r", result)

        return result['response']

    def __process_device_rpc_request(self, rpc_request: RPCRequest):
        device = self.__get_device_by_name(rpc_request.device_name)
        if device is None:
            self.__log.error('Device %s not found in connector %s', rpc_request.device_name, self.get_name())
            return {'error': 'Device %s not found' % rpc_request.device_name, 'success': False}

        device_rpc_config = device.get_device_rpc_config(rpc_request.method)
        if device_rpc_config is None:
            self.__log.error("Received rpc request, but method %s not found in config for %s.",
                             rpc_request.method,
                             self.get_name())
            self.__gateway.send_rpc_reply(rpc_request.device_name,
                                          rpc_request.id,
                                          {rpc_request.method: "METHOD NOT FOUND!"})
            return

        result = {}
        self.__create_task(self.__process_rpc_request,
                           (device, device_rpc_config, rpc_request),
                           {'result': result},
                           timeout=rpc_request.timeout)

        self.__log.debug("Result: %r", result)

        return result['response']

    def __create_task(self, func, args, kwargs, timeout=5.0):
        task = self.loop.create_task(func(*args, **kwargs))
        task_started = monotonic()

        while not task.done() and not self.__stopped and monotonic() - task_started < timeout:
            sleep(.02)

    async def __process_rpc_request(self, device: Slave, config, data, with_response=False, result={}):
        try:
            if config is not None:
                if config['functionCode'] in (5, 6, 15, 16):
                    response = await self.__write_rpc_data(device, config, data)
                elif config['functionCode'] in (1, 2, 3, 4):
                    response = await self.__read_rpc_data(device, config)
                else:
                    response = 'Unsupported function code in RPC request.'

                if data.can_return_response():
                    result['response'] = self.__send_rpc_response(data, response, with_response)
        except Exception as e:
            self.__log.error('Failed to process rpc request: %r', e)
            result['response'] = {'error': e.__repr__()}

    async def __read_rpc_data(self, device: Slave, config):
        response = None

        try:
            connected = await device.connect()
            if connected:
                response = await device.read(config['functionCode'], config['address'], config['objectsCount'])
        except Exception as e:
            self.__log.error('Failed to process rpc request: %s', e)
            response['response'] = {'error': e.__repr__()}

        if isinstance(response, ModbusPDU):
            endian_order = Endian.BIG if device.byte_order.upper() == "BIG" else Endian.LITTLE
            word_endian_order = Endian.BIG if device.word_order.upper() == "BIG" else Endian.LITTLE
            response = device.uplink_converter.decode_data(response, config, endian_order, word_endian_order)

        return response

    async def __write_rpc_data(self, device, config, data):
        response = None

        config = BytesDownlinkConverterConfig(
            device_name=device.device_name,
            byte_order=device.byte_order,
            word_order=device.word_order,
            repack=device.repack,
            objects_count=config.get("objectsCount", config.get("registersCount", config.get("registerCount", 1))),
            function_code=config.get(FUNCTION_CODE_PARAMETER),
            lower_type=config.get("type", config.get("tag", "error")),
            address=config.get(ADDRESS_PARAMETER)
        )

        converted_data = device.downlink_converter.convert(config, data.value)

        if config.function_code in (5, 6):
            try:
                converted_data = converted_data[0]
            except IndexError and TypeError:
                pass

        if converted_data is not None:
            try:
                connected = await device.connect()
                if connected:
                    response = await device.write(config.function_code, config.address, converted_data)
            except Exception as e:
                self.__log.error('Failed to process rpc request for device: %s', device, exc_info=e)
                response = e

        if isinstance(response, (WriteMultipleRegistersResponse,
                                 WriteMultipleCoilsResponse,
                                 WriteSingleCoilResponse,
                                 WriteSingleRegisterResponse)):
            self.__log.debug("Write %r", str(response))
            response = {"success": True}

        return response

    def __send_rpc_response(self, content, response, with_response=False):
        method = content.method
        if isinstance(response, Exception) or isinstance(response, ExceptionResponse):
            if not with_response:
                self.__gateway.send_rpc_reply(device=content.device_name,
                                              req_id=content.id,
                                              content={
                                                  method: response.__repr__()
                                              },
                                              success_sent=False)
            else:
                return {
                    'device': content.device_name,
                    'req_id': content.id,
                    'content': {
                        method: str(response)
                    },
                    'success_sent': False
                }
        else:
            if not with_response:
                self.__gateway.send_rpc_reply(device=content.device_name,
                                              req_id=content.id,
                                              content={'result': response})
            else:
                response = {'device': content.device_name, 'content': response}

                if content.id is not None:
                    response['req_id'] = content.id

                return response

    def __get_device_by_name(self, device_name) -> Slave:
        devices = tuple(filter(lambda slave: slave.device_name == device_name, self.__slaves))

        if len(devices) > 0:
            return devices[0]

        return None

    @property
    def connector_type(self):
        return self._connector_type

    def get_id(self):
        return self.__id

    def get_name(self):
        return self.name

    def get_type(self):
        return self._connector_type

    def get_config(self):
        return self.__config

    def is_connected(self):
        return self.__connected

    def is_stopped(self):
        return self.__stopped
