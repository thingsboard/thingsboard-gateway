#     Copyright 2026. ThingsBoard
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
from concurrent.futures import TimeoutError as FutureTimeoutError
from random import choice
from re import fullmatch
from socket import gethostbyname
from string import ascii_lowercase
from threading import Event, Thread
from time import monotonic, time

from packaging import version

from thingsboard_gateway.connectors.connector import Connector
from thingsboard_gateway.connectors.snmp.constants import (
    RESERVED_GET_RPC_SCHEMA,
    RESERVED_SET_RPC_SCHEMA,
    RESERVED_GET_RPC_PATTERN,
    RESERVED_SET_RPC_PATTERN,
    V3_CLIENT_MAX_AGE_SECONDS,
    DEFAULT_TRAP_LISTENER_ADDRESS,
    DEFAULT_TRAP_LISTENER_PORT,
    TRAP_LISTENER_CLOSE_TIMEOUT_SECONDS
)
from thingsboard_gateway.connectors.snmp.utils import format_snmp_error, to_json_safe
from thingsboard_gateway.gateway.constants import RPC_DEFAULT_TIMEOUT
from thingsboard_gateway.gateway.entities.converted_data import ConvertedData
from thingsboard_gateway.gateway.entities.rpc_request import RPCType
from thingsboard_gateway.gateway.entities.rpc_response import RPCResponse
from thingsboard_gateway.gateway.statistics.statistics_service import StatisticsService
from thingsboard_gateway.tb_utility.tb_loader import TBModuleLoader
from thingsboard_gateway.tb_utility.tb_utility import TBUtility
from thingsboard_gateway.tb_utility.tb_logger import init_logger

# Try import puresnmp library or install it and import
installation_required = False
# 2.0.2 rejects SNMPv3 responses without the requested authentication/privacy
required_version = '2.0.2'
required_crypto_version = '1.0.1.post1'
force_install = False

try:
    from puresnmp import __version__ as puresnmp_version

    if version.parse(puresnmp_version) != version.parse(required_version):
        installation_required = True
        force_install = True
except ImportError:
    installation_required = True

if installation_required:
    print("puresnmp library not found or has a different version - installing...")
    TBUtility.install_package("puresnmp", required_version, force_install=force_install)

from puresnmp import Client, credentials, PyWrapper  # noqa: E402
from puresnmp.exc import ErrorResponse, SnmpError, Timeout as SNMPTimeoutException, UnknownPrivacyModel  # noqa: E402
from puresnmp.plugins import priv as privacy_plugins  # noqa: E402
from thingsboard_gateway.connectors.snmp.trap_receiver import SNMPTrapReceiver  # noqa: E402
from thingsboard_gateway.connectors.snmp.v3_security import (  # noqa: E402
    SNMPv3MessageProcessingModel,
    parse_v3_security
)

DEFAULT_SNMP_VERSION = 'v2c'
SNMP_V3_VERSION = 'v3'
SNMP_CREDENTIALS_BY_VERSION = {
    'v1': credentials.V1,
    'v2c': credentials.V2C,
}


class SNMPConnector(Connector, Thread):
    def __init__(self, gateway, config, connector_type):
        super().__init__()
        self.daemon = True
        self.__gateway = gateway
        self._connected = False
        self.__stopped = False
        self._connector_type = connector_type
        self.__config = config
        self.__id = self.__config.get('id')
        self.name = config.get("name", 'SNMP Connector ' + ''.join(choice(ascii_lowercase) for _ in range(5)))
        self._log = init_logger(self.__gateway, self.name, self.__config.get('logLevel', 'INFO'),
                                enable_remote_logging=self.__config.get('enableRemoteLogging', False),
                                is_connector_logger=True)
        self._converter_log = init_logger(self.__gateway, self.name + "_converter",
                                          self.__config.get('logLevel', 'INFO'),
                                          enable_remote_logging=self.__config.get('enableRemoteLogging', False),
                                          is_connector_logger=True, attr_name=self.name)
        self.__devices = self.__config["devices"]
        self.__privacy_dependencies_checked = False
        self.statistics = {'MessagesReceived': 0,
                           'MessagesSent': 0}
        self._default_converters = {
            "uplink": "SNMPUplinkConverter",
            "downlink": "SNMPDownlinkConverter"
        }
        self.__methods = ["get", "multiget", "getnext", "walk", "multiwalk", "set", "multiset",
                          "bulkget", "bulkwalk", "table", "bulktable"]
        self.__datatypes = ('attributes', 'telemetry')
        self.__trap_transport = None
        self.__trap_listener_closed = Event()
        self.__trap_listener_closed.set()

        self.__loop = asyncio.new_event_loop()

    def open(self):
        self.__stopped = False
        self.__devices = self.__validate_devices(self.__devices)
        self.__fill_converters()
        self.__trap_listener_closed.clear()
        self.start()

    def __validate_devices(self, devices):
        valid_devices = []
        for device in devices:
            try:
                if self.__get_version(device) == SNMP_V3_VERSION:
                    security = parse_v3_security(device.get("security"))
                    if security.has_privacy:
                        self.__check_privacy_support(security.credentials.priv.method)
                    device["v3_security"] = security
                elif device.get("community") is None:
                    raise ValueError('"community" is required for SNMP v1/v2c')
                valid_devices.append(device)
            except ValueError as e:
                self._log.error("Device \"%s\" skipped: %s", device.get("deviceName"), e)
        return valid_devices

    def __check_privacy_support(self, privacy_method):
        if not self.__privacy_dependencies_checked:
            self.__privacy_dependencies_checked = True
            self.__install_privacy_dependencies()

        try:
            privacy_plugins.create(privacy_method)
        except UnknownPrivacyModel:
            raise ValueError(f'SNMPv3 privacy protocol "{privacy_method.upper()}" requires the puresnmp-crypto '
                             f'library, which is not available') from None

    def __install_privacy_dependencies(self):
        crypto_version = TBUtility.get_package_version("puresnmp-crypto")
        if crypto_version is not None and version.parse(crypto_version) == version.parse(required_crypto_version):
            return

        self._log.info("puresnmp-crypto library not found or has a different version - installing...")
        try:
            TBUtility.install_package("puresnmp-crypto", required_crypto_version,
                                      force_install=crypto_version is not None)
        except Exception as e:
            self._log.error("Failed to install puresnmp-crypto, SNMPv3 privacy will not work: %s", e)

    def run(self):
        self._connected = True
        try:
            self.__loop.run_until_complete(self._run())
        except Exception as e:
            self._log.exception(e)

    async def _run(self):
        try:
            self.__trap_transport = await self.__start_trap_listener()
            if self.__trap_transport is None:
                self.__trap_listener_closed.set()
            polling_devices = [device for device in self.__devices if self.__has_polling_entries(device)]
            while not self.__stopped:
                current_time = time() * 1000
                for device in polling_devices:
                    try:
                        if device.get("previous_poll_time", 0) + device.get("pollPeriod", 10000) < current_time:
                            await self.__process_data(device)
                            device["previous_poll_time"] = current_time
                    except Exception as e:
                        self._log.exception(e)
                if self.__stopped:
                    break
                else:
                    await asyncio.sleep(.2)
        finally:
            await self.__close_trap_listener()

    @staticmethod
    def __has_polling_entries(device):
        entries = device.get("attributes", []) + device.get("telemetry", [])
        return any(str(entry.get("method", "")).lower() != "trap" for entry in entries)

    async def __start_trap_listener(self):
        receiver = SNMPTrapReceiver(self.__devices, self._log, self.__process_trap)
        if not receiver.has_devices:
            return None

        listener_config = self.__config.get("trapListener", {})
        address = listener_config.get("address", DEFAULT_TRAP_LISTENER_ADDRESS)
        port = listener_config.get("port", DEFAULT_TRAP_LISTENER_PORT)
        try:
            transport, _ = await self.__loop.create_datagram_endpoint(lambda: receiver, local_addr=(address, port))
        except OSError as e:
            self._log.error("Failed to start SNMP trap listener on %s:%s: %s", address, port, e)
            return None

        self._log.info("SNMP trap listener started on %s:%s", address, port)
        return transport

    async def __close_trap_listener(self):
        if self.__trap_transport is not None:
            self.__trap_transport.close()
            self.__trap_transport = None
            await asyncio.sleep(0)
        self.__trap_listener_closed.set()

    def __process_trap(self, device, trap_data):
        converted_data: ConvertedData = device["uplink_converter"].convert(device, trap_data)
        if (converted_data is not None and
                (converted_data.attributes_datapoints_count > 0 or
                 converted_data.telemetry_datapoints_count > 0)):
            # TODO: update trap statistics collection after statistics refactoring
            self.collect_statistic_and_send(self.get_name(), self.get_id(), converted_data)

    def close(self):
        self.__stopped = True
        self._connected = False
        if self.__trap_listener_closed.is_set():
            return

        asyncio.run_coroutine_threadsafe(self.__close_trap_listener(), self.__loop)
        if not self.__trap_listener_closed.wait(TRAP_LISTENER_CLOSE_TIMEOUT_SECONDS):
            self._log.error("SNMP trap listener was not stopped in %s seconds", TRAP_LISTENER_CLOSE_TIMEOUT_SECONDS)

    def get_id(self):
        return self.__id

    def get_name(self):
        return self.name

    def get_type(self):
        return self._connector_type

    def is_connected(self):
        return self._connected

    def is_stopped(self):
        return self.__stopped

    def get_config(self):
        return self.__config

    def collect_statistic_and_send(self, connector_name, connector_id, data):
        self.statistics["MessagesReceived"] = self.statistics["MessagesReceived"] + 1
        self.__gateway.send_to_storage(connector_name, connector_id, data)
        self.statistics["MessagesSent"] = self.statistics["MessagesSent"] + 1

    async def __process_data(self, device):
        common_parameters = self.__get_common_parameters(device)
        v3_client = None
        v3_client_created_at = 0
        device_responses = {}
        for datatype in self.__datatypes:
            for datatype_config in device.get(datatype, []):
                try:
                    method = datatype_config.get("method")
                    if method is None:
                        self._log.error("Method not found in configuration: %r", datatype_config)
                        continue
                    else:
                        method = method.lower()
                    if method == "trap":
                        continue
                    if method not in self.__methods:
                        self._log.error("Unknown method: %s, configuration is: %r", method, datatype_config)
                    v3_client, v3_client_created_at = self.__recreate_client_if_needed(
                        v3_client, v3_client_created_at, common_parameters, datatype_config)
                    response = await self.__process_methods(method, common_parameters, datatype_config, v3_client)
                    device_responses[datatype_config['key']] = response

                    StatisticsService.count_connector_message(self.name, stat_parameter_name='connectorMsgsReceived')
                    StatisticsService.count_connector_bytes(self.name, response,
                                                            stat_parameter_name='connectorBytesReceived')
                except SNMPTimeoutException:
                    self._log.error("Timeout exception on connection to device \"%s\" with ip: \"%s\"",
                                    device["deviceName"],
                                    device["ip"])
                    return
                except SnmpError as e:
                    if common_parameters["version"] == SNMP_V3_VERSION and not isinstance(e, ErrorResponse):
                        self._log.error("SNMPv3 security error on device \"%s\" with ip: \"%s\": %s",
                                        device["deviceName"],
                                        device["ip"],
                                        format_snmp_error(e))
                        return
                    self._log.error("SNMP error on device \"%s\" for key \"%s\": %s",
                                    device["deviceName"],
                                    datatype_config.get("key"),
                                    format_snmp_error(e))
                except Exception as e:
                    self._log.exception(e)

        if device_responses:
            converted_data: ConvertedData = device["uplink_converter"].convert(device, device_responses)

            if (converted_data is not None and
                    (converted_data.attributes_datapoints_count > 0 or
                     converted_data.telemetry_datapoints_count > 0)):
                self.collect_statistic_and_send(self.get_name(), self.get_id(), converted_data)

    def __recreate_client_if_needed(self, client, client_created_at, common_parameters, datatype_config):
        if common_parameters["version"] != SNMP_V3_VERSION:
            return None, 0
        if client is not None and monotonic() - client_created_at <= V3_CLIENT_MAX_AGE_SECONDS:
            return client, client_created_at
        return self.__create_client(common_parameters, datatype_config), monotonic()

    def __create_client(self, common_parameters, datatype_config):
        client = Client(ip=common_parameters['ip'],
                        port=common_parameters['port'],
                        credentials=self.__get_credentials(common_parameters, datatype_config),
                        context_name=common_parameters['context_name'],
                        engine_id=common_parameters['context_engine_id'])
        if common_parameters["version"] == SNMP_V3_VERSION:
            client.mpm = SNMPv3MessageProcessingModel(client.transport_handler, client.config.lcd)
        client.configure(timeout=common_parameters['timeout'])
        return PyWrapper(client)

    async def __process_methods(self, method, common_parameters, datatype_config, client=None):
        if client is None:
            client = self.__create_client(common_parameters, datatype_config)

        response = None

        if method == "get":
            oid = datatype_config["oid"]
            response = await client.get(oid=oid)
        elif method == "multiget":
            oids = datatype_config["oid"]
            oids = oids if isinstance(oids, list) else list(oids)
            response = await client.multiget(oids=oids)
        elif method == "getnext":
            oid = datatype_config["oid"]
            master_response = await client.getnext(oid=oid)
            response = {master_response.oid: master_response.value}
        elif method == "walk":
            oid = datatype_config["oid"]
            response = {}
            async for binded_var in client.walk(oid=oid):
                response[binded_var.oid] = binded_var.value
        elif method == "multiwalk":
            oids = datatype_config["oid"]
            oids = oids if isinstance(oids, list) else list(oids)
            response = {}
            async for binded_var in client.multiwalk(oids=oids):
                response[binded_var.oid] = binded_var.value
        elif method == "set":
            oid = datatype_config["oid"]
            value = datatype_config["value"]
            response = await client.set(oid=oid, value=value)
        elif method == "multiset":
            mappings = datatype_config["mappings"]
            response = await client.multiset(mappings=mappings)
        elif method == "bulkget":
            scalar_oids = datatype_config.get("scalarOid", [])
            scalar_oids = scalar_oids if isinstance(scalar_oids, list) else list(scalar_oids)
            repeating_oids = datatype_config.get("repeatingOid", [])
            repeating_oids = repeating_oids if isinstance(repeating_oids, list) else list(repeating_oids)
            max_list_size = datatype_config.get("maxListSize", 1)
            response = await client.bulkget(scalar_oids=scalar_oids, repeating_oids=repeating_oids,
                                            max_list_size=max_list_size)
            response = response.scalars
        elif method == "bulkwalk":
            oids = datatype_config["oid"]
            oids = oids if isinstance(oids, list) else list(oids)
            bulk_size = datatype_config.get("bulkSize", 10)
            response = {}
            async for binded_var in client.bulkwalk(bulk_size=bulk_size, oids=oids):
                response[binded_var.oid] = binded_var.value
        elif method == "table":
            oid = datatype_config["oid"]
            num_base_nodes = datatype_config.get("numBaseNodes", 0)
            response = await client.table(oid=oid)
        elif method == "bulktable":
            oid = datatype_config["oid"]
            num_base_nodes = datatype_config.get("numBaseNodes", 0)
            bulk_size = datatype_config.get("bulkSize", 10)
            response = await client.bulktable(oid=oid, bulk_size=bulk_size)
        else:
            self._log.error("Method \"%s\" - Not found", str(method))
        return response

    def __fill_converters(self):
        for device in self.__devices:
            try:
                uplink_converter_class_name = device.get('converter', self._default_converters["uplink"])
                downlink_converter_class_name = device.get('converter', self._default_converters["downlink"])
                device["uplink_converter"] = TBModuleLoader.import_module("snmp", uplink_converter_class_name)(
                    device, self._converter_log)
                device["downlink_converter"] = TBModuleLoader.import_module("snmp", downlink_converter_class_name)(
                    device)
            except Exception as e:
                self._log.error("Failed to load converter for device \"%s\": %s", device.get("deviceName"), e)

    def __get_credentials(self, common_parameters, datatype_config):
        version = common_parameters.get("version", DEFAULT_SNMP_VERSION)
        if version == SNMP_V3_VERSION:
            return common_parameters["v3_security"].credentials

        credentials_type = SNMP_CREDENTIALS_BY_VERSION.get(version)
        if credentials_type is None:
            self._log.warning("Unknown SNMP version: %r, falling back to %r. Supported versions: %s",
                              version, DEFAULT_SNMP_VERSION, ", ".join([*SNMP_CREDENTIALS_BY_VERSION, SNMP_V3_VERSION]))
            credentials_type = SNMP_CREDENTIALS_BY_VERSION[DEFAULT_SNMP_VERSION]

        community = common_parameters["community"]
        if datatype_config.get("community") is not None:
            community = datatype_config["community"]
        return credentials_type(community)

    @staticmethod
    def __get_common_parameters(device):
        v3_security = device.get("v3_security")
        return {"ip": gethostbyname(device["ip"]),
                "port": device.get("port", 161),
                "timeout": device.get("timeout", 6),
                "community": device.get("community"),
                "version": SNMPConnector.__get_version(device),
                "v3_security": v3_security,
                "context_name": v3_security.context_name if v3_security is not None else b"",
                "context_engine_id": v3_security.context_engine_id if v3_security is not None else b"",
                }

    @staticmethod
    def __get_version(device):
        return str(device.get("version", DEFAULT_SNMP_VERSION)).lower()

    def on_attributes_update(self, content):
        try:
            device = self.__find_device_by_name(content["device"])
            if device is None:
                self._log.error("Device \"%s\" not found", content["device"])
                return

            for attribute_request_config in device.get("attributeUpdateRequests", []):
                for attribute, value in content["data"].items():
                    if fullmatch(attribute_request_config["attributeFilter"], attribute):
                        common_parameters = self.__get_common_parameters(device)
                        result_key = "mappings" if "mappings" in attribute_request_config else "value"
                        converted_value = device["downlink_converter"].convert(
                            attribute_request_config, {"params": value, "attribute": value})
                        downlink_config = {**attribute_request_config, result_key: converted_value}
                        result = self.__run_coroutine(
                            self.__process_methods(attribute_request_config["method"], common_parameters,
                                                   downlink_config),
                            int(attribute_request_config.get("timeout", 5)))
                        self._log.debug(
                            "Received attribute update request for device \"%s\" "
                            "with attribute \"%s\" and value \"%s\"",
                            content["device"],
                            attribute,
                            value)
                        self._log.debug(result)
                        self._log.debug(content)
        except SnmpError as e:
            self._log.error("SNMP error on attribute update for device \"%s\": %s",
                            content.get("device"), format_snmp_error(e))
        except Exception as e:
            self._log.exception(e)

    def __find_device_by_name(self, device_name):
        device_filter = tuple(filter(lambda device: device["deviceName"] == device_name, self.__devices))
        if len(device_filter):
            return device_filter[0]

    def __run_coroutine(self, coroutine, timeout):
        future = asyncio.run_coroutine_threadsafe(coroutine, self.__loop)
        try:
            return future.result(timeout)
        except FutureTimeoutError:
            future.cancel()
            raise

    def server_side_rpc_handler(self, rpc_request) -> RPCResponse:
        self._log.debug('Received RPC request: %s', rpc_request)

        try:
            if rpc_request.rpc_type == RPCType.DEVICE:
                return self._process_rpc_to_device(rpc_request)
            elif rpc_request.rpc_type == RPCType.RESERVED:
                return self._process_reserved_rpc(rpc_request)
            elif rpc_request.rpc_type == RPCType.CONNECTOR:
                return self.__rpc_error_response(rpc_request, 'Connector RPC is not supported by SNMP connector')
            else:
                return self.__rpc_error_response(rpc_request, f'Invalid RPC type request: {rpc_request}')
        except Exception as e:
            return self.__rpc_error_response(rpc_request,
                                             f'Error processing RPC request {rpc_request}: {format_snmp_error(e)}')

    def __rpc_error_response(self, rpc_request, error_msg) -> RPCResponse:
        self._log.error(error_msg)
        response = RPCResponse(rpc_request.id, device=rpc_request.device_name)
        response.set_error_msg(error_msg)
        return response

    def _process_rpc_to_device(self, rpc_request) -> RPCResponse:
        device = self.__find_device_by_name(rpc_request.device_name)
        if device is None:
            return self.__rpc_error_response(rpc_request, f'Device {rpc_request.device_name} not found')

        rpc_config = next((rpc_config for rpc_config in device.get("serverSideRpcRequests", [])
                           if fullmatch(rpc_config['requestFilter'], rpc_request.method_name)), None)
        if rpc_config is None:
            return self.__rpc_error_response(
                rpc_request, f'Neither of configured device rpc methods match with {rpc_request.method_name}')

        return self._process_rpc(rpc_request, device, rpc_config, value=rpc_request.params)

    def _process_reserved_rpc(self, rpc_request) -> RPCResponse:
        device = self.__find_device_by_name(rpc_request.device_name)
        if device is None:
            return self.__rpc_error_response(rpc_request, f'Device {rpc_request.device_name} not found')

        if not rpc_request.params:
            return self.__rpc_error_response(
                rpc_request, f"No 'params' found in reserved RPC request '{rpc_request.method_name}'")

        is_set = rpc_request.method_name.lower() == 'set'
        pattern = RESERVED_SET_RPC_PATTERN if is_set else RESERVED_GET_RPC_PATTERN
        match = fullmatch(pattern, rpc_request.params)
        if match is None:
            expected_schema = RESERVED_SET_RPC_SCHEMA if is_set else RESERVED_GET_RPC_SCHEMA
            return self.__rpc_error_response(
                rpc_request, f"The requested RPC does not match with the schema: {expected_schema}")

        rpc_config = {key: value for key, value in match.groupdict().items() if value is not None}
        rpc_config['method'] = 'set' if is_set else 'get'
        value = rpc_config.pop('value', None)
        return self._process_rpc(rpc_request, device, rpc_config, value=value)

    def _process_rpc(self, rpc_request, device, rpc_config, value) -> RPCResponse:
        try:
            converted_value = device["downlink_converter"].convert(rpc_config, {"params": value})
        except ValueError as e:
            return self.__rpc_error_response(rpc_request, f"Failed to convert RPC value: {e}")

        result_key = "mappings" if "mappings" in rpc_config else "value"
        downlink_config = {**rpc_config, result_key: converted_value}
        common_parameters = self.__get_common_parameters(device)
        timeout = min(float(rpc_config.get("timeout", RPC_DEFAULT_TIMEOUT)), float(rpc_request.timeout))

        try:
            result = self.__run_coroutine(self.__process_methods(rpc_config["method"],
                                                                 common_parameters,
                                                                 downlink_config),
                                          timeout)
        except FutureTimeoutError:
            return self.__rpc_error_response(
                rpc_request, f'Timeout ({timeout}s) waiting for RPC {rpc_request.method_name} response '
                             f'from device {device["deviceName"]}')
        except SNMPTimeoutException:
            return self.__rpc_error_response(
                rpc_request, f'Timeout exception on connection to device {device["deviceName"]}')

        result = to_json_safe(result)
        self._log.trace('RPC result: %s', result)

        response = RPCResponse(rpc_request.id, device=rpc_request.device_name)
        response.set_message(result)
        return response
