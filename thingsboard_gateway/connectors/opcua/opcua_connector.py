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
import logging
import re
from asyncio.exceptions import CancelledError
from concurrent.futures import ThreadPoolExecutor
from queue import Queue, Empty
from random import choice
from string import ascii_lowercase
from threading import Thread
from time import sleep, monotonic, time
from typing import List, Dict, Union
from typing import Tuple, Any
from cachetools import TTLCache

from thingsboard_gateway.connectors.connector import Connector
from thingsboard_gateway.gateway.constants import CONNECTOR_PARAMETER, RECEIVED_TS_PARAMETER, CONVERTED_TS_PARAMETER, \
    DATA_RETRIEVING_STARTED, REPORT_STRATEGY_PARAMETER, ON_ATTRIBUTE_UPDATE_DEFAULT_TIMEOUT
from thingsboard_gateway.gateway.entities.converted_data import ConvertedData
from thingsboard_gateway.gateway.entities.report_strategy_config import ReportStrategyConfig
from thingsboard_gateway.gateway.statistics.statistics_service import StatisticsService
from thingsboard_gateway.gateway.tb_gateway_service import TBGatewayService
from thingsboard_gateway.tb_utility.tb_loader import TBModuleLoader
from thingsboard_gateway.tb_utility.tb_logger import init_logger
from thingsboard_gateway.tb_utility.tb_utility import TBUtility

# Try import asyncua library or install it and import
installation_required = False
required_version = '1.1.5'
force_install = False

from packaging import version

try:
    from asyncua import __version__ as asyncua_version

    if version.parse(asyncua_version) != version.parse(required_version):
        installation_required = True
        force_install = True

except ImportError:
    installation_required = True

if installation_required:
    print("OPC-UA library not found")
    TBUtility.install_package("asyncua", required_version, force_install=force_install)

import asyncua
from asyncua import ua, Node
from asyncua.ua import NodeId, UaStringParsingError
from asyncua.common.ua_utils import value_to_datavalue
from asyncua.ua.uaerrors import BadWriteNotSupported, BadTypeMismatch, UaInvalidParameterError
from asyncua.crypto.security_policies import SecurityPolicyBasic256Sha256, SecurityPolicyBasic256, \
    SecurityPolicyBasic128Rsa15
from asyncua.ua.uaerrors import UaStatusCodeError, BadNodeIdUnknown, BadConnectionClosed, \
    BadInvalidState, BadSessionClosed, BadAttributeIdInvalid, BadCommunicationError, BadOutOfService, BadNoMatch, \
    BadUnexpectedError, UaStatusCodeErrors, BadWaitingForInitialData, BadSessionIdInvalid, BadSubscriptionIdInvalid
from thingsboard_gateway.connectors.opcua.entities.rpc_request import OpcUaRpcRequest, OpcUaRpcType
from thingsboard_gateway.connectors.opcua.device import Device
from thingsboard_gateway.connectors.opcua.backward_compatibility_adapter import BackwardCompatibilityAdapter

DEFAULT_UPLINK_CONVERTER = 'OpcUaUplinkConverter'

SECURITY_POLICIES = {
    "Basic128Rsa15": SecurityPolicyBasic128Rsa15,
    "Basic256": SecurityPolicyBasic256,
    "Basic256Sha256": SecurityPolicyBasic256Sha256,
}

MESSAGE_SECURITY_MODES = {
    "None": asyncua.ua.MessageSecurityMode.None_,
    "Sign": asyncua.ua.MessageSecurityMode.Sign,
    "SignAndEncrypt": asyncua.ua.MessageSecurityMode.SignAndEncrypt
}

RPC_SET_SPLIT_PATTERNS = ["; ", ";=", "=", " "]


class OpcUaConnector(Connector, Thread):
    def __init__(self, gateway: 'TBGatewayService', config, connector_type):
        self.statistics = {'MessagesReceived': 0,
                           'MessagesSent': 0}
        super().__init__()
        self.__scanning_nodes_cache = TTLCache(maxsize=100_000, ttl=3600)
        self._connector_type = connector_type
        self.__gateway: 'TBGatewayService' = gateway
        self.__config = config
        self.__id = self.__config.get('id')
        self.name = self.__config.get("name", 'OPC-UA Connector ' + ''.join(choice(ascii_lowercase) for _ in range(5)))
        self.__last_contact_time = 0

        # check if config is in old format and convert it to new format
        using_old_configuration_format = len(tuple(filter(lambda node_config: not node_config.get('deviceInfo'),
                                                          self.__config.get('server', {}).get('mapping', []))))
        self.__enable_remote_logging = self.__config.get('enableRemoteLogging', False)
        self.__log = init_logger(self.__gateway, self.name, self.__config.get('logLevel', 'INFO'),
                                 enable_remote_logging=self.__enable_remote_logging, is_connector_logger=True)
        self.__converter_log = init_logger(self.__gateway, self.name + '_converter',
                                           self.__config.get('logLevel', 'INFO'),
                                           enable_remote_logging=self.__enable_remote_logging,
                                           is_converter_logger=True, attr_name=self.name)
        self.__replace_loggers()
        if gateway.get_report_strategy_service() is not None:
            report_strategy = self.__config.get('reportStrategy')
            self.__connector_report_strategy_config = gateway.get_report_strategy_service().get_main_report_strategy()
            try:
                if report_strategy is not None:
                    self.__connector_report_strategy_config = ReportStrategyConfig(report_strategy)
            except ValueError as e:
                self.__log.warning(
                    'Error in report strategy configuration: %s, the gateway main strategy will be used.',
                    e)
        if using_old_configuration_format:
            backward_compatibility_adapter = BackwardCompatibilityAdapter(self.__config, self.__log)
            self.__config = backward_compatibility_adapter.convert()

        self.__server_conf = self.__config.get('server', {})
        self.__server_limits = {}
        self.__max_nodes_per_read = 100
        self.__max_nodes_per_subscribe = 100

        if using_old_configuration_format:
            self.__log.warning('Connector configuration has been updated to the new format.')

        if "opc.tcp" not in self.__server_conf.get("url"):
            self.__opcua_url = "opc.tcp://" + self.__server_conf.get("url")
        else:
            self.__opcua_url = self.__server_conf.get("url")

        self.__enable_subscriptions = self.__server_conf.get('enableSubscriptions', True)
        self.__sub_check_period_in_millis = max(self.__server_conf.get("subCheckPeriodInMillis", 100), 100)
        # Batch size for data change subscription, the gateway will process this amount of data, received from subscriptions, or less in one iteration
        self.__sub_data_max_batch_size = self.__server_conf.get("subDataMaxBatchSize", 1000)
        self.__sub_keep_alive_period = self.__server_conf.get("subKeepAlivePeriodInMillis", 0) / 1000
        self.__sub_data_min_batch_creation_time = max(self.__server_conf.get("subDataMinBatchCreationTimeMs", 200),
                                                      100) / 1000
        self.__subscription_batch_size = self.__server_conf.get('subscriptionProcessBatchSize', 2000)

        self.__reconnect_retries_count = self.__server_conf.get('reconnectRetriesCount', 8)
        self.__reconnect_backoff_initial_delay = self.__server_conf.get('reconnectBackoffInitialDelay', 1)
        self.__reconnect_backoff_factor = self.__server_conf.get('reconnectBackoffFactor', 2)

        self.__show_map = self.__server_conf.get('showMap', False)

        self.__sub_data_to_convert = Queue(-1)
        self.__data_to_convert = Queue(-1)

        try:
            self.__loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self.__loop)
        except RuntimeError:
            self.__loop = asyncio.get_event_loop()

        self.__client: Union[asyncua.Client, None] = None

        self.__connected = False
        self.__stopped = False
        self.daemon = True

        self.__thread_pool_executor = ThreadPoolExecutor(max_workers=8)
        self.__thread_pool_executor_processor_thread = Thread(name='Thread Pool Executor Processor',
                                                              target=self.__thread_pool_executor_processor,
                                                              daemon=True)
        self.__thread_pool_executor_processor_thread.start()

        self.__device_nodes: List[Device] = []
        self.__nodes_config_cache: Dict[NodeId, List[Device]] = {}
        self.__next_poll = 0
        self.__next_scan = 0
        self.__client_recreation_required = True

        self.__log.info("OPC-UA Connector has been initialized")

    def __replace_loggers(self):
        for logger_name in logging.root.manager.loggerDict.keys():
            if 'asyncua' in logger_name:
                init_logger(self.__gateway,
                            logger_name,
                            'ERROR',
                            enable_remote_logging=self.__enable_remote_logging,
                            is_connector_logger=True,
                            attr_name=self.name)

    def get_device_shared_attributes_keys(self, device_name):
        device = self.__get_device_by_name(device_name)
        if device is not None:
            return device.shared_attributes_keys
        return []

    def open(self):
        self.__stopped = False
        self.start()
        self.__log.info("Starting OPC-UA Connector (Async IO)")

    def get_type(self):
        return self._connector_type

    def close(self):
        self.__stopped = True
        self.__connected = False
        self.__log.info("Stopping OPC-UA Connector")

        self.__delete_devices_from_platform()
        asyncio.run_coroutine_threadsafe(self.__disconnect(), self.__loop)
        asyncio.run_coroutine_threadsafe(self.__cancel_all_tasks(), self.__loop)

        start_time = monotonic()

        while self.is_alive():
            if monotonic() - start_time > 10:
                self.__log.error("Failed to stop connector %s", self.get_name())
                break
            sleep(.1)

        self.__log.info('%s has been stopped.', self.get_name())
        self.__log.stop()

    async def __cancel_all_tasks(self):
        await asyncio.sleep(5)
        for task in asyncio.all_tasks(self.__loop):
            task.cancel()

    async def __disconnect(self):
        try:
            if self.__connected:
                await self.__unsubscribe_from_nodes()
            await self.__client.disconnect()
            self.__log.info('%s has been disconnected from OPC-UA Server.', self.get_name())
        except Exception as e:
            self.__log.warning('%s could not be disconnected from OPC-UA Server: %s', self.name, e)

    async def __unsubscribe_from_node(self, device: Device, node):
        node.pop('valid', None)
        if self.__enable_subscriptions:
            node_obj = node.get("node") or node.get("qualified_path")
            if node_obj is None:
                return
            subscription_id = device.nodes_data_change_subscriptions.get(node_obj.nodeid, {}).get('subscription')
            if subscription_id is not None:
                try:
                    await device.subscription.unsubscribe(subscription_id)
                except Exception as e:
                    self.__log.exception('Error unsubscribing from on data change: %s', e)
                device.nodes_data_change_subscriptions[node_obj.nodeid]['subscription'] = None

    async def __unsubscribe_from_nodes(self, device_name=None):
        for device in self.__device_nodes:
            if device_name is None or device.name == device_name:
                for section in ('attributes', 'timeseries'):
                    for node in device.values.get(section, []):
                        await self.__unsubscribe_from_node(device, node)

            if (device_name is None and device.subscription is not None
                    and self.__client.uaclient.protocol is not None
                    and self.__client.uaclient.protocol.state == 'open'):
                try:
                    await device.subscription.delete()
                except AttributeError:
                    self.__log.debug('Subscription already deleted')
                except BadSubscriptionIdInvalid:
                    self.__log.debug('Subscription already deleted')
                except ConnectionError:
                    self.__log.info("Client is not connected, cannot delete subscription.")
                except Exception as e:
                    self.__log.exception('Error deleting subscription: %s', e)
                device.subscription = None
        self.__nodes_config_cache.clear()

    def get_id(self):
        return self.__id

    def get_name(self):
        return self.name

    def is_connected(self):
        return self.__connected

    def is_stopped(self):
        return self.__stopped

    def get_config(self):
        return self.__config

    def run(self):

        if self.__enable_subscriptions:
            sub_data_convert_thread = Thread(name='Sub Data Convert Thread', target=self.__convert_sub_data,
                                             daemon=True)
            sub_data_convert_thread.start()

        try:
            self.__loop.run_until_complete(self.start_client())
        except asyncio.CancelledError:
            try:
                self.__loop.run_until_complete(self.disconnect_if_connected())
            except Exception:
                pass
        except Exception as e:
            self.__log.exception("Error in main loop: %s", e)

    async def start_client(self):
        sleep_for_subscription_work_model = max(self.__sub_check_period_in_millis / 1000, .02)
        while not self.__stopped:
            try:
                reconnect_required = True
                if self.__client_recreation_required:
                    if (self.__client is not None
                            and self.__client.uaclient.protocol
                            and self.__client.uaclient.protocol.state == 'open'):
                        await self.disconnect_if_connected()
                    self.__client = asyncua.Client(url=self.__opcua_url,
                                                   timeout=self.__server_conf.get('timeoutInMillis', 4000) / 1000)
                    self.__log.debug("Recreating client after disconnection")
                    self.__client._monitor_server_loop = self._monitor_server_loop
                    self.__client._renew_channel_loop = self._renew_channel_loop
                    self.__client.session_timeout = self.__server_conf.get('sessionTimeoutInMillis', 120000)
                    self.__device_cleanup_after_reconnection()

                    if self.__server_conf.get('uri'):
                        self.__client.application_uri = self.__server_conf['uri']

                    if self.__is_certificate_based_auth():
                        await self.__set_auth_settings_by_cert()
                    if self.__server_conf["identity"].get("username"):
                        self.__set_auth_settings_by_username()
                    if self.__stopped:
                        break
                    if self.__enable_subscriptions and self.__device_nodes:
                        self.__log.debug(
                            "Subscriptions are enabled, client will reconnect, unsubscribe old subscriptions and subscribe to new nodes.")
                        await self.retry_connect_with_backoff(self.__reconnect_retries_count,
                                                              self.__reconnect_backoff_initial_delay,
                                                              self.__reconnect_backoff_factor)
                        self.__last_contact_time = monotonic()
                        if not (self.__client.uaclient.protocol and self.__client.uaclient.protocol.state == 'open'):
                            self.__log.error("Failed to connect to server, retrying...")
                            await self.disconnect_if_connected()
                            continue
                        await self.__unsubscribe_from_nodes()
                        reconnect_required = False

                    self.__nodes_config_cache = {}
                    self.__delete_devices_from_platform()
                    self.__next_scan = 0
                    self.__next_poll = 0
                    self.__device_nodes = []
                    self.__client_recreation_required = False

                if reconnect_required:
                    await self.retry_connect_with_backoff(self.__reconnect_retries_count,
                                                          self.__reconnect_backoff_initial_delay,
                                                          self.__reconnect_backoff_factor)
                    self.__last_contact_time = monotonic()

                if not (self.__client.uaclient.protocol and self.__client.uaclient.protocol.state == 'open'):
                    self.__log.error("Failed to connect to server, retrying...")
                    await self.disconnect_if_connected()
                    continue
                self.__log.info("Connected to OPC-UA Server: %s", self.__opcua_url)
                self.__connected = True
                self.__update_scanning_cache()

                try:
                    await self.__client.load_data_type_definitions()
                except Exception as e:
                    self.__log.error("Error on loading type definitions:\n %s", e)

                try:
                    await self.__fetch_server_limitations()
                except Exception as e:
                    self.__log.error("Error on fetching server limitations:\n %s", e)

                poll_period = int(self.__server_conf.get('pollPeriodInMillis', 5000) / 1000)
                scan_period = int(self.__server_conf.get('scanPeriodInMillis', 3600000) / 1000)

                if self.__enable_subscriptions:
                    await self.__scan_device_nodes()
                    self.__next_scan = monotonic() + scan_period
                    # await self.__poll_nodes()

                while not self.__stopped:
                    if self.__client_recreation_required:
                        await self.disconnect_if_connected()
                        break
                    if monotonic() >= self.__next_scan:
                        self.__next_scan = monotonic() + scan_period
                        await self.__scan_device_nodes()

                    if not self.__enable_subscriptions and monotonic() >= self.__next_poll:
                        self.__next_poll = monotonic() + poll_period
                        await self.__poll_nodes()

                    current_time = monotonic()
                    time_to_sleep = min(self.__next_poll - current_time, self.__next_scan - current_time) \
                        if not self.__enable_subscriptions else sleep_for_subscription_work_model
                    if time_to_sleep > 0:
                        await asyncio.sleep(time_to_sleep)
                    if not self.__connected:
                        if self.__stopped:
                            self.__log.debug("Connection closed due to stopping.")
                        else:
                            self.__log.debug("Detected connection lost, OPC-UA client will reconnect to server.")
                            await self.disconnect_if_connected()
                        break

            except (ConnectionError, BadSessionClosed, BadSessionIdInvalid):
                self.__log.warning('Connection lost for %s, will try to reconnect...', self.get_name())
                await self.disconnect_if_connected()
                self.__client_recreation_required = True
            except asyncio.exceptions.TimeoutError:
                self.__log.warning('Failed to connect %s', self.get_name())
                self.__client_recreation_required = True
                await self.disconnect_if_connected()
                await asyncio.sleep(5)
            except asyncio.CancelledError as e:
                if self.__stopped:
                    self.__log.debug('Task was cancelled due to connector stop: %s', e.__str__())
                else:
                    self.__log.exception('Task was cancelled: %s', e.__str__())
            except UaStatusCodeError as e:
                self.__log.error('Error in main loop, trying to reconnect: %s', exc_info=e)
                await self.disconnect_if_connected()
            except Exception as e:
                self.__log.exception("Error in main loop: %s", e)
            finally:
                if self.__stopped:
                    try:
                        await self.disconnect_if_connected()
                    except Exception:
                        pass
                    self.__connected = False
                await asyncio.sleep(.5)

    def __is_certificate_based_auth(self):
        auth_type = self.__server_conf["identity"].get("type")
        return auth_type == "cert.PEM" or auth_type == "certificates"

    async def disconnect_if_connected(self):
        if self.__connected:
            try:
                if self.__enable_subscriptions:
                    self.__client_recreation_required = True
                await self.__client.disconnect()
            except Exception:
                pass  # ignore

    def __delete_devices_from_platform(self):
        for device in self.__device_nodes:
            self.__gateway.del_device(device.name)

    async def retry_connect_with_backoff(self, max_retries=8, initial_delay=1, backoff_factor=2):
        last_contact_delta = monotonic() - self.__last_contact_time
        if last_contact_delta < self.__client.session_timeout / 1000 and self.__last_contact_time > 0:
            time_to_wait = self.__client.session_timeout / 1000 - last_contact_delta
            time_to_wait = min(self.__server_conf.get('sessionTimeoutInMillis', 120000) / 1000 * 0.8, time_to_wait)
            self.__log.info('Last contact was %.2f seconds ago, next connection try in %.2f seconds...',
                            last_contact_delta, time_to_wait)
            await asyncio.sleep(time_to_wait)
        delay = initial_delay
        for attempt in range(max_retries):
            if self.__stopped:
                return None
            try:
                self.__log.info("Trying to reconnect to the server, attempt %d of %d", attempt + 1, max_retries)
                return await self.__client.connect()

            except Exception as e:
                base_time = self.__client.session_timeout / 1000 if (
                        last_contact_delta > 0 and last_contact_delta < self.__client.session_timeout / 1000) else 0
                time_to_wait = base_time + delay
                time_to_wait = min(self.__server_conf.get('sessionTimeoutInMillis', 120000) / 1000 * 0.8, time_to_wait)
                self.__log.error('Encountered error: %r. Next connection try in %i second(s)...', e, time_to_wait)
                await asyncio.sleep(time_to_wait)
                delay *= backoff_factor
        self.__log.error('Max retries reached. Connection failed.')
        return None

    async def __set_auth_settings_by_cert(self):
        try:
            auth_config = self.__server_conf["identity"]

            ca_cert = auth_config.get("caCert", auth_config.get('pathToCACert'))
            private_key = auth_config.get("privateKey", auth_config.get('pathToPrivateKey'))
            cert = auth_config.get("cert", auth_config.get('pathToClientCert'))
            policy = self.__server_conf["security"]
            mode = auth_config.get("mode", "SignAndEncrypt")

            if cert is None or private_key is None:
                self.__log.exception("Error in ssl configuration - cert or privateKey parameter not found")
                raise RuntimeError("Error in ssl configuration - cert or privateKey parameter not found")

            await self.__client.set_security(
                SECURITY_POLICIES[policy],
                certificate=cert,
                private_key=private_key,
                server_certificate=ca_cert,
                mode=MESSAGE_SECURITY_MODES[mode]
            )
        except Exception as e:
            self.__log.exception(e)

    def __set_auth_settings_by_username(self):
        self.__client.set_user(self.__server_conf["identity"].get("username"))
        if self.__server_conf["identity"].get("password"):
            self.__client.set_password(self.__server_conf["identity"].get("password"))

    async def _monitor_server_loop(self):
        """
        Checks if the server is alive
        """
        timeout = min(self.__client.session_timeout / 1000 / 2, self.__client._watchdog_intervall)
        try:
            while not self.__client._closing and not self.__stopped:
                await asyncio.sleep(timeout)
                _ = await self.__client.nodes.server_state.read_value()
                self.__last_contact_time = monotonic()
        except ConnectionError as e:
            await self.__client._lost_connection(e)
            await self.__client.uaclient.inform_subscriptions(ua.StatusCode(ua.StatusCodes.BadShutdown))
            self.__connected = False
        except Exception as e:
            await self.__client._lost_connection(e)
            await self.__client.uaclient.inform_subscriptions(ua.StatusCode(ua.StatusCodes.BadShutdown))
            self.__connected = False
        finally:
            if not self.__stopped:
                self.__client_recreation_required = True

    async def _renew_channel_loop(self):
        """
        Renew the SecureChannel before the SecureChannelTimeout will happen.
        In theory, we could do that only if no session activity,
        but it does not cost much.
        """
        try:
            duration = self.__client.secure_channel_timeout * 0.75 / 1000
            while not self.__client._closing and not self.__stopped:
                await asyncio.sleep(duration)
                await self.__client.open_secure_channel(renew=True)
                val = await self.__client.nodes.server_state.read_value()
        except CancelledError:
            self.__log.debug("Renew channel loop task cancelled")
        except ConnectionError as e:
            self.__log.error("Connection error in renew_channel loop %s", e)
        except TimeoutError:
            self.__log.error("Timeout error in renew_channel loop")
        except Exception as e:
            self.__log.exception("Error in renew_channel loop %s", e)
        finally:
            if not self.__stopped:
                self.__client_recreation_required = True

    def __device_cleanup_after_reconnection(self):
        client_session = self.__client.uaclient
        for device in self.__device_nodes:
            try:
                device.device_node.session = client_session
                for node_list in device.nodes:
                    try:
                        node_list['node'].session = client_session

                    except IndexError as e:
                        self.__log.warning("The requested node does not exist")
                        continue

                    except Exception as e:
                        self.__log.warning("Failed to assign node for device %s", device)
                        continue

            except Exception as e:
                self.__log.warning("Could not process node sync process for client recreation: %s ", e)
                continue

    def __update_scanning_cache(self):
        start_time = monotonic()
        for key, value in dict(self.__scanning_nodes_cache).items():
            try:
                for index, element in enumerate(value):
                    node_obj = element.get('node')
                    if isinstance(node_obj, Node) and node_obj and node_obj.session is not self.__client.uaclient:
                        node_obj.session = self.__client.uaclient
                        element['node'] = node_obj
                        value[index] = element
                    else:
                        continue
                self.__scanning_nodes_cache[key] = value

            except AttributeError as e:
                self.__log.trace("Missing update due to type mismatch for key %s", key)
                continue

            except Exception as e:
                self.__log.error("Could not update scanning cache for key %s: %s", key, e)
                continue

        end_time = monotonic()
        total_time_taken = end_time - start_time
        self.__log.trace("The function for updating scanning cache took %.2f seconds", total_time_taken)

    def __load_converter(self, device):
        converter_class_name = device.get('converter')
        if not converter_class_name:
            self.__log.debug("No custom converter found for device %s, using default converter", device.get('name'))
            converter_class_name = DEFAULT_UPLINK_CONVERTER
        module = TBModuleLoader.import_module(self._connector_type, converter_class_name)

        if module:
            self.__log.debug('Converter %s for device %s - found!', converter_class_name, self.name)
            return module

        self.__log.error("Cannot find converter for %s device", self.name)
        return None

    @staticmethod
    def is_regex_pattern(pattern):
        return not re.fullmatch(pattern, pattern)

    def __find_foreign_node_in_cache(self, name_of_device_node: str):
        self.__log.debug("Searching cache for foreign node with name '%s' ")
        candidates = []
        for key, chain in self.__scanning_nodes_cache.items():
            if not key or not isinstance(key, str):
                continue
            if key.split(".")[-1] != name_of_device_node:
                continue

            for elem in chain:
                try:
                    if isinstance(elem, dict) and elem["path"].split(":")[-1] == name_of_device_node:
                        candidates.append(elem)
                        self.__log.debug("Matched cached node for %s with path=%s", name_of_device_node, elem["path"])
                except KeyError:
                    self.__log.debug("Cached element under key '%s' is missing 'path' field: %r", key, elem)

                except IndexError:
                    self.__log.debug("Unexpected path format in cached element under key '%s': %r", key, elem)

        self.__log.debug("Search in cache for '%s' finished, %d candidate found", name_of_device_node, len(candidates))

        return candidates

    async def __find_nodes(self, node_list_to_search, current_parent_node, nodes, path="Root", find_foreign_nodes=False):
        assert len(node_list_to_search) > 0
        final = []

        target_node_path = None
        if len(node_list_to_search) == 1:
            node_paths = [node['path'] if isinstance(node, dict) else node for node in nodes]
            target_node_path = '.'.join(node_path.split(':')[-1] for node_path in node_paths) + '.' + \
                               node_list_to_search[0]

            if target_node_path in self.__scanning_nodes_cache:
                if self.__show_map:
                    self.__log.debug('Found node in cache: %s', node_list_to_search[0])
                final.append(self.__scanning_nodes_cache[target_node_path])
                return final

            elif find_foreign_nodes and target_node_path:
                try:
                    name_of_device_node = target_node_path.split('.')[-1]
                    node_in_cache = self.__find_foreign_node_in_cache(name_of_device_node)
                    if node_in_cache:
                        final.append(node_in_cache)
                        return final
                except Exception as e:
                    self.__log.info("Could not find foreign node in cache for path %s - %s", target_node_path, e)

        children = await current_parent_node.get_children()
        children_nodes_count = len(children)
        counter = 0
        if self.__show_map:
            self.__log.debug('Found %s children for %s', children_nodes_count, current_parent_node)
        for node in children:
            counter += 1
            child_node = await node.read_browse_name()
            if len(node_list_to_search) == 1:
                node_paths = [node['path'] if isinstance(node, dict) else node for node in nodes]
                current_node_path = '.'.join(
                    node_path.split(':')[-1] for node_path in node_paths) + '.' + child_node.Name
                if current_node_path not in self.__scanning_nodes_cache:
                    self.__scanning_nodes_cache[current_node_path] = [*nodes, {
                        'path': f'{child_node.NamespaceIndex}:{child_node.Name}', 'node': node}]
            if self.__show_map and path:
                if children_nodes_count < 1000 or counter % 1000 == 0:
                    self.__log.info('Checking path: %s', path + '.' + f'{child_node.Name}')

            if re.fullmatch(re.escape(node_list_to_search[0]), child_node.Name) or node_list_to_search[0].split(':')[-1] == child_node.Name:
                if self.__show_map:
                    self.__log.info('Found node: %s', child_node.Name)
                new_nodes = [*nodes, {'path': f'{child_node.NamespaceIndex}:{child_node.Name}', 'node': node}]
                if len(node_list_to_search) == 1:
                    final.append(new_nodes)
                    if self.__show_map:
                        self.__log.debug('Found node: %s', child_node.Name)
                    return final
                else:
                    final.extend(
                        await self.__find_nodes(node_list_to_search[1:], current_parent_node=node, nodes=new_nodes,
                                                path=path + '.' + f'{child_node.Name}'))

        return final

    async def find_nodes(self, node_pattern, current_parent_node=None, nodes=None):
        if nodes is None:
            nodes = []
        node_list = node_pattern.split('\\.')

        if current_parent_node is None:
            current_parent_node = self.__client.nodes.root

            if len(node_list) > 0 and node_list[0].lower() == 'root':
                node_list = node_list[1:]

        return await self.__find_nodes(node_list, current_parent_node, nodes, 'Root')

    async def find_node_name_space_index(self, path):
        if isinstance(path, str):
            path = path.split('\\.')

        # find unresolved nodes
        u_node_count = len(tuple(filter(lambda u_node: len(u_node.split(':')) < 2, path)))

        resolved = path[:-u_node_count]
        resolved_level = len(path) - u_node_count
        if resolved_level < 1:
            path = path[1:]
            parent_node = self.__client.get_root_node()
        else:
            parent_node = await self.__client.nodes.root.get_child(resolved)

        unresolved = path[resolved_level:]
        return await self.__find_nodes(unresolved, current_parent_node=parent_node, nodes=resolved)

    async def _get_device_info_by_pattern(self, pattern, get_first=False, parent_node=None):
        """
        Method used to retrieve device name/profile by pattern.
        Device name/profile can consist of path expressions or NodeId expressions but not both.
        It will search for device name in the following order:
        1. By path expressions (e.g. "My Device ${Root\\.Objects\\.Device\\.serialNumber}}")
        2. By NodeId expressions (e.g. "My Device ${ns=2;i=1003} - ${ns=2;i=1008}")
        3. If no matches found, returns the pattern as is.
        """

        search_results = await self.__get_device_expression_value_by_path(pattern, parent_node=parent_node)
        if len(search_results) > 0:
            return search_results[0] if get_first else search_results

        search_results = await self.__get_device_expression_value_by_identifier(pattern)
        if len(search_results) > 0:
            return search_results[0] if get_first else search_results

        return [pattern]

    async def __get_device_expression_value_by_path(self, pattern: str, parent_node=None) -> List[str]:
        results = []

        search_result = re.search(r"\${([A-Za-z.:_\-\\\d\[\]]+)}", pattern)
        if search_result:
            try:
                group = search_result.group(0)
                node_path = search_result.group(1)
            except IndexError:
                self.__log.error('Invalid pattern: %s', pattern)
                return results

            nodes = await self.find_nodes(node_path, current_parent_node=parent_node)
            self.__log.debug('Found device name nodes: %s', nodes)

            for node in nodes:
                try:
                    var = node[-1]['node']
                    value = await var.read_value()
                    results.append(pattern.replace(group, str(value)))
                except Exception as e:
                    self.__log.exception(e)
                    continue

        return results

    async def __get_device_expression_value_by_identifier(self, pattern: str) -> List[str]:
        result = pattern
        node_ids_search_result = re.findall(r"\${(ns=\d+;[isgb]=[^}]+)}", pattern)
        if node_ids_search_result:
            for group in node_ids_search_result:
                try:
                    node_id = NodeId.from_string(group)
                    value = await self.__client.get_node(node_id).read_value()
                    result = result.replace(f'${{{group}}}', str(value))
                except Exception as e:
                    self.__log.error('Invalid NodeId format %s: %s', group, e)

        return [result]

    def __convert_sub_data(self):
        device_converted_data_map = {}
        sleep_period_after_empty_batch = max(self.__sub_check_period_in_millis / 1000, .02)

        while not self.__stopped:
            batch = []
            batch_start_forming_time = time()
            while (not self.__sub_data_to_convert.empty()
                   and len(batch) < self.__sub_data_max_batch_size
                   and time() - batch_start_forming_time < self.__sub_data_min_batch_creation_time):
                try:
                    batch.append(self.__sub_data_to_convert.get_nowait())
                    if (time() - batch_start_forming_time) >= self.__sub_data_min_batch_creation_time:
                        break
                except Empty:
                    break

            if not batch and self.__sub_data_to_convert.empty():
                sleep(sleep_period_after_empty_batch)
                continue
            if self.__log.isEnabledFor(5):
                self.__log.debug('Batch created in %i milliseconds', (time() - batch_start_forming_time) * 1000)
                self.__log.debug('Batch size: %s', len(batch))
                self.__log.debug('Data left in queue: %s', self.__sub_data_to_convert.qsize())

            for sub_node, data, received_ts in batch:
                node_configs = self.__nodes_config_cache.get(sub_node.nodeid, [])
                for device in node_configs:
                    try:
                        if sub_node.nodeid not in device.nodes_data_change_subscriptions:
                            continue
                        if device not in device_converted_data_map:
                            device_converted_data_map[device] = ConvertedData(device_name=device.name,
                                                                              device_type=device.device_profile)

                        nodes_configs = device.nodes_data_change_subscriptions[sub_node.nodeid]['nodes_configs']
                        nodes_values = [data.monitored_item.Value for _ in range(len(nodes_configs))]

                        converted_data = device.converter_for_sub.convert(nodes_configs, nodes_values)

                        if converted_data:
                            converted_data.add_to_metadata({
                                CONNECTOR_PARAMETER: self.get_name(),
                                RECEIVED_TS_PARAMETER: received_ts,
                                CONVERTED_TS_PARAMETER: int(time() * 1000)
                            })

                            for node_config in nodes_configs:
                                if node_config['section'] == 'attributes':
                                    device_converted_data_map[device].add_to_attributes(converted_data.attributes)
                                else:
                                    device_converted_data_map[device].add_to_telemetry(converted_data.telemetry)
                    except Exception as e:
                        self.__log.exception("Error converting data: %s", e)

            for device, converted_data in device_converted_data_map.items():
                self.__gateway.send_to_storage(self.get_name(), self.get_id(), converted_data)
                self.__log.debug('Converted data from %r notifications from server for device %s, using converter: %r',
                                 len(batch), device.name, device.converter_for_sub.__class__.__name__)

            device_converted_data_map.clear()

    async def __build_parent_node_descriptors(self, parent_nodes: List[Node]) -> List[Dict[str, Node]]:
        parent_descriptors: List[Dict[str, Node]] = []

        for node in parent_nodes:
            try:
                browse_name = await node.read_browse_name()
                path = f"{browse_name.NamespaceIndex}:{browse_name.Name}"

                parent_descriptors.append({
                    "path": path,
                    "node": node,
                })

            except UaInvalidParameterError as exc:
                self.__log.warning("Failed to read browse name for parent node %s while building "
                                   "device base path descriptors: %s", node, exc)

            except Exception as exc:
                self.__log.error("Unexpected error while building parent descriptor for node %s: %s", node, exc)

        return parent_descriptors

    async def __scan_device_nodes(self):
        await self._create_new_devices()
        await self._load_devices_nodes()

    async def __get_device_base_nodes(self, device_node_pattern):
        try:
            if self.__is_node_identifier(device_node_pattern):
                self.__log.debug("Resolving device base nodes from NodeId pattern '%s'", device_node_pattern)

                node_id = NodeId.from_string(device_node_pattern)
                node = self.__client.get_node(node_id)

                parent_nodes = await node.get_path()

                parent_nodes_without_root = parent_nodes[1:]

                if not parent_nodes_without_root:
                    self.__log.debug("No parents nodes found for node %s", node)
                    return []

                parent_descriptors = await self.__build_parent_node_descriptors(parent_nodes=parent_nodes_without_root)

                if not parent_descriptors:
                    self.__log.debug("No parent descriptors were built for node %s", node)
                return [parent_descriptors] if parent_descriptors else []

            elif self.__is_absolute_path(device_node_pattern):
                nodes = await self.find_nodes(device_node_pattern)
                return nodes
            else:
                return []

        except Exception as e:
            self.__log.error('Error finding device base nodes by pattern %s: %s', device_node_pattern, e)
            return []

    def __compute_subscription_death_threshold(self, device):
        death_threshold = self.__sub_keep_alive_period
        _keep_alive_time = device.subscription.parameters.RequestedLifetimeCount * device.subscription.parameters.RequestedPublishingInterval / 1000
        self.__log.trace("Defined death threshold is %s while by specification it is %s", death_threshold,
                         _keep_alive_time)
        return death_threshold

    async def __subscription_watchlog(self, device):
        check_interval = 10
        death_interval = self.__compute_subscription_death_threshold(device)
        while not self.__stopped and device.subscription and not device.subscription_has_expired:
            await asyncio.sleep(check_interval)
            if not self.__enable_subscriptions or device.subscription is None:
                continue
            last_activity_timme = getattr(device, 'last_subscription_activity', None)
            if last_activity_timme is None:
                continue
            silent_interval = monotonic() - last_activity_timme
            if silent_interval >= death_interval:
                self.__log.warning("No activity from subscription for device %s in the last %.2f seconds. "
                                   "Recreating subscription.", device.name, silent_interval)
                device.subscription_has_expired = True

    async def _create_new_devices(self):
        self.__log.debug('Scanning for new devices from configuration...')
        existing_devices = list(map(lambda dev: dev.name, self.__device_nodes))

        scanned_devices = []
        for device_config in self.__config.get('mapping', []):
            nodes = await self.__get_device_base_nodes(device_config['deviceNodePattern'])
            if not nodes:
                self.__log.error(
                    "No nodes found for such device name '%s' "
                    "Skipping device creation.",
                    device_config.get('deviceInfo', {}).get('deviceNameExpression', "")
                )
                continue

            self.__log.debug('Found devices: %s', nodes)

            for node in nodes:
                device_name_expression = device_config.get('deviceInfo', {}).get('deviceNameExpression')

                is_abs_path = self.__is_absolute_path(device_name_expression)
                device_names = await self._get_device_info_by_pattern(device_name_expression,
                                                                      parent_node=node[-1]['node'] if not is_abs_path else None)

                for device_name in device_names:
                    scanned_devices.append(device_name)

                    if device_name not in existing_devices:
                        converter = self.__load_converter(device_config)

                        device_profile_expression = device_config.get('deviceInfo', {}).get('deviceProfileExpression',
                                                                                            'default')
                        is_abs_path = self.__is_absolute_path(device_profile_expression)
                        device_profile = await self._get_device_info_by_pattern(device_profile_expression,
                                                                                get_first=True,
                                                                                parent_node=node[-1]['node'] if not is_abs_path else None)

                        device_config = {**device_config, 'device_name': device_name, 'device_type': device_profile}
                        device_path = [node_path_node_object['path'] for node_path_node_object in node]
                        self.__device_nodes.append(
                            Device(path=device_path, name=device_name, device_profile=device_profile,
                                   config=device_config,
                                   converter=converter(
                                       device_config, self.__converter_log),
                                   converter_for_sub=converter(device_config,
                                                               self.__converter_log) if self.__enable_subscriptions else None,
                                   device_node=node[-1]['node'],
                                   logger=self.__log))
                        self.__log.info('Added device node: %s', device_name)
        self.__log.debug('Device nodes: %s', self.__device_nodes)

    async def _load_devices_nodes(self):
        for device in self.__device_nodes:
            device.nodes = []
            for section in ('attributes', 'timeseries'):
                self.__log.info('Loading nodes for device: %s, section: %s, nodes count: %s', device.name, section,
                                len(device.values.get(section, [])))
                for node in device.values.get(section, []):
                    try:
                        path = node.get('qualified_path', node['path'])
                        if self.__is_node_identifier(path):
                            found_node = self.__client.get_node(path)
                            try:
                                node_path = node['path']
                                node_browse_name = await found_node.read_browse_name()
                                current_node_path = '.'.join([
                                    node_path.split(':')[-1],
                                    node_browse_name.Name
                                ])
                                self.__scanning_nodes_cache[current_node_path] = [{
                                    "path": f"{node_browse_name.NamespaceIndex}:{node_browse_name.Name}",
                                    "node": found_node,
                                }]
                            except (KeyError, IndexError) as e:
                                self.__log.debug('Failed to cache node %s, - %s', found_node, str(e))

                            except Exception as e:
                                self.__log.debug("Unexpected error occurred caching node %s, - %s ", found_node, str(e))
                        else:
                            if not isinstance(path, Node):
                                if len(path[-1].split(':')) != 2:
                                    qualified_path = await self.find_node_name_space_index(path)
                                    if len(qualified_path) == 0:
                                        if node.get('valid', True):
                                            self.__log.warning('Node not found; device: %s, key: %s, path: %s',
                                                               device.name,
                                                               node['key'], node['path'])
                                            if node.get('node') is not None and self.__enable_subscriptions:
                                                await self.__unsubscribe_from_node(device, node)
                                        continue
                                    elif len(qualified_path) > 1:
                                        self.__log.warning(
                                            'Multiple matching nodes found; device: %s, key: %s, path: %s; %s',
                                            device.name,
                                            node['key'], node['path'], qualified_path)
                                    node['qualified_path'] = qualified_path[0][-1]['node']
                                    path = qualified_path[0][-1]['node']

                            if isinstance(path, Node):
                                found_node = path
                            else:
                                found_node = await self.__client.nodes.root.get_child(path)

                        node_report_strategy = node.get(REPORT_STRATEGY_PARAMETER)
                        if self.__gateway.get_report_strategy_service() is not None:
                            if node_report_strategy is not None:
                                try:
                                    node_report_strategy = ReportStrategyConfig(node_report_strategy)
                                except ValueError as e:
                                    self.__log.error(
                                        'Error in report strategy configuration: %s, for key %s the device or connector report strategy will be used.',
                                        e, node['key'])
                                    node_report_strategy = self.__connector_report_strategy_config if device.report_strategy is None else device.report_strategy
                            elif device.report_strategy is not None:
                                node_report_strategy = device.report_strategy

                        node_config = {"node": found_node, "key": node['key'],
                                       "section": section,
                                       'timestampLocation': node.get('timestampLocation', 'gateway')}
                        if self.__gateway.get_report_strategy_service() is not None and node_report_strategy is not None:
                            node_config[REPORT_STRATEGY_PARAMETER] = node_report_strategy
                            node_report_strategy = None  # Cleaning for next iteration

                        device.nodes.append(node_config)

                        if self.__enable_subscriptions and device.subscription_has_expired and self.__sub_keep_alive_period > 0 and not self.__client_recreation_required:
                            self.__log.warning("Processing subscription expired with defined interval %d",
                                               self.__sub_keep_alive_period)
                            await self.__unsubscribe_from_nodes()
                            device.subscription_has_expired = False

                        subscription_exists = (device.subscription is not None
                                               and found_node.nodeid in device.nodes_data_change_subscriptions
                                               and device.nodes_data_change_subscriptions[found_node.nodeid][
                                                   'subscription'] is not None)

                        if node.get('valid') is None or (node.get('valid') and not self.__enable_subscriptions):
                            if self.__enable_subscriptions and not subscription_exists and not self.__stopped:
                                self.__nodes_config_cache.setdefault(found_node.nodeid, [])
                                if device not in self.__nodes_config_cache[found_node.nodeid]:
                                    self.__nodes_config_cache[found_node.nodeid].append(device)
                                if found_node.nodeid not in device.nodes_data_change_subscriptions:
                                    if found_node.nodeid not in self.__nodes_config_cache:
                                        self.__nodes_config_cache[found_node.nodeid] = []
                                    self.__nodes_config_cache[found_node.nodeid].append(device)

                                    device_node_config = {
                                        'subscription': None,
                                        'node': found_node,
                                        'nodes_configs': []
                                    }
                                    device.nodes_data_change_subscriptions[found_node.nodeid] = device_node_config

                                device.nodes_data_change_subscriptions[found_node.nodeid]['nodes_configs'].append(
                                    node_config)

                                if device.subscription is None:
                                    device.subscription = await self.__client.create_subscription(
                                        self.__sub_check_period_in_millis, SubHandler(
                                            self.__sub_data_to_convert, device, self.__log, self.status_change_callback))
                                    if self.__sub_keep_alive_period > 0:
                                        self.__loop.create_task(self.__subscription_watchlog(device))
                                        self.__log.debug("Started subscription watchlog task for device and subscription %s", device.name, device.subscription)

                                node['id'] = found_node.nodeid.to_string()

                            node['valid'] = True
                    except ConnectionError as e:
                        raise e
                    except (BadNodeIdUnknown, BadConnectionClosed, BadInvalidState, BadAttributeIdInvalid,
                            BadCommunicationError, BadOutOfService, BadNoMatch, BadUnexpectedError,
                            UaStatusCodeErrors,
                            BadWaitingForInitialData):
                        if node.get('valid', True):
                            self.__log.warning('Node not found (2); device: %s, key: %s, path: %s',
                                               device.name,
                                               node['key'], node['path'])
                            await self.__unsubscribe_from_node(device, node)
                    except UaStatusCodeError as uae:
                        if node.get('valid', True):
                            self.__log.exception('Node status code error: %s', uae)
                            await self.__unsubscribe_from_node(device, node)
                    except Exception as e:
                        if node.get('valid', True):
                            self.__log.exception(e)
                            await self.__unsubscribe_from_node(device, node)
            try:
                if self.__enable_subscriptions and device.subscription is not None:
                    to_subscribe = zip(*list(map(lambda n: (n[1]['node'], n),
                                                 filter(lambda n: n[1]['subscription'] is None,
                                                        device.nodes_data_change_subscriptions.items()))))
                    nodes_to_subscribe = []
                    conf = []
                    if to_subscribe:
                        try:
                            nodes_to_subscribe, conf = to_subscribe
                        except ValueError:
                            pass

                    if nodes_to_subscribe:
                        nodes_data_change_subscriptions = await self._subscribe_for_node_updates_in_batches(
                            device, nodes_to_subscribe,
                            self.__max_nodes_per_subscribe or self.__subscription_batch_size)
                        subs = []
                        for subs_batch in nodes_data_change_subscriptions:
                            subs.extend(subs_batch)
                        for node, conf, subscription_id in zip(nodes_to_subscribe, conf, subs):
                            device.nodes_data_change_subscriptions[node.nodeid]['subscription'] = subscription_id
                        self.__log.info('Subscribed to %i nodes for device %s', len(nodes_to_subscribe), device.name)
                    else:
                        self.__log.debug('No new nodes to subscribe for device %s', device.name)
                else:
                    self.__log.debug('Subscriptions are disabled for device %s or device subscription is None',
                                     device.name)
            except Exception as e:
                self.__log.exception("Error loading nodes: %s", e)
                raise e

    async def _subscribe_for_node_updates_in_batches(self, device, nodes, batch_size=500) -> List:
        total_nodes = len(nodes)
        total_successfully_subscribed = 0
        total_failed_to_subscribe = 0
        result = []
        for i in range(0, total_nodes, batch_size):
            batch = nodes[i:i + batch_size]
            batch_len = len(batch)
            try:
                self.__log.info("Subscribing to batch %i with %i nodes.", i // batch_size + 1, batch_len)
                subscription_result = await device.subscription.subscribe_data_change(batch)
                bad_results = list(filter(lambda r: not isinstance(r, int), subscription_result))
                if bad_results:
                    reasons = [r.doc for r in bad_results]
                    self.__log.error(
                        "Failed subscribing to nodes, server returned the following reasons: %r, nodes count with these problems - %r",
                        set(reasons), len(reasons))
                    self.__log.trace("Failed nodes: %r", bad_results)
                result.append(subscription_result)
                successfully_processed = batch_len - len(bad_results)
                total_successfully_subscribed += successfully_processed
                total_failed_to_subscribe += len(bad_results)
                if successfully_processed > 0:
                    self.__log.info("Succesfully subscribed to batch number %i with %i nodes.", i // batch_size + 1,
                                    successfully_processed)
                else:
                    self.__log.warning("Failed to subscribe to batch number %i with %i nodes.", i // batch_size + 1,
                                       batch_len)
            except Exception as e:
                self.__log.warning("Error subscribing to batch %i with %i : %r", i // batch_size + 1, batch_len, e)
                # self.__log.error("%r", batch) # Uncomment to see the nodes that failed to subscribe
                self.__log.exception("Error subscribing to nodes: ", exc_info=e)
                break
        self.__log.info("Expected subscription to %i nodes.", total_nodes)
        self.__log.info("Successfully subscribed to %i nodes.", total_successfully_subscribed)
        self.__log.info("Failed to subscribe to %i nodes.", total_failed_to_subscribe)
        return result

    def status_change_callback(self, status):
        self.__log.debug('Received status change event: %s', status.Status.doc)
        if status.Status.is_bad():
            self.__client_recreation_required = True

    async def __poll_nodes(self):
        data_retrieving_started = int(time() * 1000)
        all_nodes = [node_config['node'] for device in self.__device_nodes for node_config in device.nodes]

        if len(all_nodes) > 0:
            received_ts = int(time() * 1000)
            for i in range(0, len(all_nodes), self.__max_nodes_per_read):
                batch = all_nodes[i:i + self.__max_nodes_per_read]
                try:
                    values = await self.__client.read_attributes(batch)
                    self.__data_to_convert.put((values, received_ts, data_retrieving_started))
                except Exception as e:
                    self.__log.warning("Failed to read batch from %i to %i: %s", i, i + self.__max_nodes_per_read, e)

        else:
            self.__log.info('No nodes to poll')

    def __thread_pool_executor_processor(self):
        pack = 10
        futures = []
        while not self.__stopped:
            try:
                for future in futures:
                    if future.done():
                        futures.remove(future)
                    else:
                        sleep(.02)
                    if self.__stopped:
                        for future in futures:
                            future.cancel()
                        break
            except Exception as e:
                self.__log.exception("Error in thread pool executor: %s", e)

            try:
                values, received_ts, data_retrieving_started = self.__data_to_convert.get_nowait()
                futures.append(self.__thread_pool_executor.submit(self.__convert_retrieved_data, values, received_ts,
                                                                  data_retrieving_started))
                if len(futures) >= pack:
                    continue
            except Empty:
                sleep(.02)

    def __convert_retrieved_data(self, values, received_ts, data_retrieving_started):
        try:
            converted_nodes_count = 0
            for device in self.__device_nodes:
                nodes_count = len(device.nodes)
                device_values = values[converted_nodes_count:converted_nodes_count + nodes_count]
                converted_nodes_count += nodes_count
                converted_data: ConvertedData = self.__convert_device_data(device.converter, device.nodes,
                                                                           device_values)
                converted_data.add_to_metadata({
                    CONNECTOR_PARAMETER: self.get_name(),
                    RECEIVED_TS_PARAMETER: received_ts,
                    CONVERTED_TS_PARAMETER: int(time() * 1000),
                    DATA_RETRIEVING_STARTED: data_retrieving_started
                })
                if converted_data:
                    self.__gateway.send_to_storage(self.get_name(), self.get_id(), converted_data)

                    StatisticsService.count_connector_message(self.name, stat_parameter_name='connectorMsgsReceived')
                    # TODO: Should these counters be here, or on upper level?
                    StatisticsService.count_connector_bytes(self.name, converted_data,
                                                            stat_parameter_name='connectorBytesReceived')
            self.__log.info('Converted data from %s nodes', converted_nodes_count)
        except Exception as e:
            self.__log.exception("Error converting data: ", exc_info=e)

    @staticmethod
    def __convert_device_data(converter, device_nodes, values):
        return converter.convert(device_nodes, values)

    def __send_data_to_gateway_storage(self, data):
        data.metadata.update({'sendToStorageTs': int(time() * 1000)})

        self.statistics['MessagesReceived'] = self.statistics['MessagesReceived'] + 1
        self.__gateway.send_to_storage(self.get_name(), self.get_id(), data)
        self.statistics['MessagesSent'] = self.statistics['MessagesSent'] + 1
        self.__log.debug('Count data msg to storage: %s', self.statistics['MessagesSent'])

    @staticmethod
    def get_rpc_node_pattern_and_base_path(params, device, logger):
        try:
            if 'Root.' in params:
                current_path = params
                node_pattern = params.replace('.', '\\.')
                return node_pattern, current_path
            names = [v.split(':', 1)[1] for v in device.path]
            node_pattern = "Root\\." + r"\.".join(names) + r'\.' + params.split('\\.')[-1]
            current_path = 'Root.' + '.'.join(names)
            return node_pattern, current_path
        except Exception as e:
            logger.error("determine_rpc_income_data failed for params=%r: %s",
                         params, e)

    def find_full_node_path(self, params: str, device: Device) -> NodeId | None:
        try:
            node_pattern, current_path = self.get_rpc_node_pattern_and_base_path(params, device, logger=self.__log)
            node_list = node_pattern.split("\\.")[-1:]
            nodes = []
            find_task = self.__find_nodes(node_list, device.device_node, nodes, current_path, find_foreign_nodes=True)
            task = self.__loop.create_task(find_task)
            found_task_completed, found_nodes = self.__wait_task_with_timeout(task=task, timeout=10, poll_interval=0.2)
            if not found_task_completed:
                self.__log.error(
                    "Failed to process request for %s, timeout has been reached",
                )
                return None

            if found_nodes:
                full_path = found_nodes[-1][0]['node'].nodeid
                return full_path
            self.__log.error('Node not found! (%s)', found_nodes)

        except ConnectionError as e:
            self.__log.error("Connection error during node lookup for %r: %s", params, e)

        except Exception as e:
            self.__log.error("Error during node lookup for %r: %s", params, e)

    def __get_device_by_name(self, device_name: str):
        device_list = tuple(filter(lambda i: i.name == device_name, self.__device_nodes))
        if len(device_list) > 0:
            return device_list[0]

        return None

    def on_attributes_update(self, content: Dict):
        self.__log.debug(content)
        try:
            device = self.__get_device_by_name(content['device'])
            if device is None:
                self.__log.error('Device %s not found for attributes update', content['device'])
                return

            if not device.config.get("attributes_updates"):
                self.__log.error("No attribute mapping found for device %s", device.name)
                return

            for (key, value) in content["data"].items():
                if key not in device.shared_attributes_keys_value_pairs:
                    self.__log.warning("Attribute key %s not found in device attribute section %s", key, device.name)
                    continue

                raw_path = TBUtility.get_value(device.shared_attributes_keys_value_pairs.get(key), get_tag=True)
                node_id = self.__resolve_node_id(raw_path=raw_path, device=device)
                if not isinstance(node_id, NodeId):
                    self.__log.error('Could not find node for device (%s) for key (%s) with path (%s)', device.name,
                                     key, raw_path)

                    continue
                self.__write_node_value(node_id, value, timeout=ON_ATTRIBUTE_UPDATE_DEFAULT_TIMEOUT)
                self.__log.debug("Successfully proccesed attribute update for device %s with key %s", device.name, key)

        except Exception as e:
            self.__log.exception(e)

    def __resolve_node_id(self, raw_path: str, device: Device) -> NodeId | None:
        node_id = (
            NodeId.from_string(raw_path)
            if self.__is_node_identifier(raw_path)
            else self.find_full_node_path(raw_path, device)
        )
        return node_id

    def __write_node_value(self, node_id: NodeId, value, timeout: float) -> bool:
        try:
            write_task = self.__write_value(node_id, value)
            task = self.__loop.create_task(write_task)
            task_completed, result = self.__wait_task_with_timeout(task=task, timeout=timeout,
                                                                   poll_interval=0.2)
            if not task_completed:
                self.__log.error(
                    "Failed to process request for %s, timeout has been reached",
                )
                result = {"error": f"Timeout has been reached during write {value}"}

            if err := result.get("error"):
                self.__log.error("Write error on %s: %s", node_id, err)
                return False

            self.__log.debug("Successfully wrote %s to %s", value, node_id)
            return True

        except Exception as exc:
            self.__log.exception("Unexpected error during write: %s", exc)
            self.__log.debug("Unexpected error during write: %s", exc_info=exc)
            return False

    def server_side_rpc_handler(self, content: Dict):
        self.__log.info('Received server side rpc request: %r', content)

        try:
            response = None
            rpc_request = OpcUaRpcRequest(content=content)
            if rpc_request.rpc_type == OpcUaRpcType.CONNECTOR:
                response = self.__process_connector_rpc_request(rpc_request=rpc_request)
            elif rpc_request.rpc_type == OpcUaRpcType.DEVICE:
                response = self.__process_device_rpc_request(rpc_request=rpc_request)

            elif rpc_request.rpc_type == OpcUaRpcType.RESERVED:
                response = self.__process_reserved_rpc_request(rpc_request=rpc_request)
            return response

        except Exception as e:
            self.__log.error('Failed to process server side rpc request: %s', e)
            return {'error': '%r' % e, 'success': False}

    def __process_connector_rpc_request(self, rpc_request: OpcUaRpcRequest):
        self.__log.debug("Received RPC to connector: %r", rpc_request)
        results = []
        for device in self.__device_nodes:
            rpc_request.device = device.name
            try:
                task = self.__loop.create_task(
                    self.__call_method(device.path, rpc_request.rpc_method, rpc_request.arguments))
                task_completed, result = self.__wait_task_with_timeout(task=task, timeout=rpc_request.timeout,
                                                                       poll_interval=0.2)
                if not task_completed:
                    self.__log.error(
                        "Failed to process rpc request for %s, timeout has been reached",
                        device.name,
                    )
                    results.append({"error": f"Timeout rpc has been reached for {device.name}"})
                    continue
                result['device_name'] = device.name
                results.append(result)
                self.__log.debug("RPC with method %s execution result is: %s", rpc_request.rpc_method, result)
            except Exception as e:
                self.__log.exception(e)
                self.__gateway.send_rpc_reply(rpc_request.device, rpc_request.id,
                                              {"result": {"error": str(e)}})
        return results

    @staticmethod
    def __wait_task_with_timeout(task: asyncio.Task, timeout: float, poll_interval: float = 0.2) -> Tuple[bool, Any]:
        start_time = monotonic()
        while not task.done():
            sleep(poll_interval)
            current_time = monotonic()
            if current_time - start_time >= timeout:
                task.cancel()
                return False, None
        return True, task.result()

    def __process_device_rpc_request(self, rpc_request: OpcUaRpcRequest):
        device = self.__get_device_by_name(rpc_request.device_name)
        rpc_section = device.config.get('rpc_methods', [])
        if device is None:
            self.__log.error('Device %s not found for RPC request', rpc_request.device_name)
            self.__gateway.send_rpc_reply(device=rpc_request.device_name,
                                          req_id=rpc_request.id,
                                          content={"result": {"error": 'Device not found'}})
            return

        if not device.is_valid_rpc_method_name(rpc_device_section=rpc_section, rpc_request=rpc_request):
            self.__log.error('Requested rpc method is not found in config %s', rpc_request.device_name)
            self.__gateway.send_rpc_reply(device=rpc_request.device_name,
                                          req_id=rpc_request.id,
                                          content={"result": {"error": 'Requested rpc method is not found in config'}})
            return

        rpc_request.arguments = device.get_device_rpc_arguments(rpc_device_section=rpc_section, rpc_request=rpc_request)
        if isinstance(rpc_request.arguments, dict):
            error_msg = rpc_request.arguments.get('error')
            self.__log.error(f'{error_msg} for device {rpc_request.device_name}')
            self.__gateway.send_rpc_reply(device=rpc_request.device_name,
                                          req_id=rpc_request.id,
                                          content={"result": error_msg})
            return

        try:
            task = self.__loop.create_task(
                self.__call_method(device.path, rpc_request.rpc_method, rpc_request.arguments))
            task_completed, result = self.__wait_task_with_timeout(task=task, timeout=rpc_request.timeout,
                                                                   poll_interval=0.2)
            if not task_completed:
                self.__log.error(
                    "Failed to process rpc request for %s, timeout has been reached",
                    device.name,
                )
                result = {"error": f"Timeout rpc has been reached for {device.name}"}
            elif task_completed:
                self.__log.debug("RPC with method %s execution result is: %s", rpc_request.rpc_method, result)

            self.__gateway.send_rpc_reply(rpc_request.device_name,
                                          rpc_request.id,
                                          {"result": result})
            return result

        except Exception as e:
            self.__log.exception(e)
            self.__gateway.send_rpc_reply(rpc_request.device_name, rpc_request.id,
                                          {"result": {"error": str(e)}})

    def __process_reserved_rpc_request(self, rpc_request: OpcUaRpcRequest):
        identifier = ''
        device = self.__get_device_by_name(rpc_request.device_name)
        if device is None:
            self.__log.error('Device %s not found for RPC request', rpc_request.device_name)
            self.__gateway.send_rpc_reply(device=rpc_request.device_name,
                                          req_id=rpc_request.id,
                                          content={"result": {"error": 'Device not found'}})
            return
        is_node_id = self.__is_node_identifier(rpc_request.params)

        if is_node_id:
            identifier = rpc_request.params
            if not identifier:
                self.__log.error("Could not find node for requested rpc request %s", rpc_request.params)
                self.__gateway.send_rpc_reply(device=rpc_request.device_name,
                                              req_id=rpc_request.id,
                                              content={
                                                  "result": {"error": 'Could not find node for requested rpc request'}})

        elif not is_node_id:

            identifier = device.get_node_by_key(rpc_request.params)
            if not identifier:
                identifier = self.find_full_node_path(params=rpc_request.params, device=device)
            rpc_request.received_identifier = identifier

        try:
            task = self.__loop.create_task(self.__process_rpc_request(identifier=identifier, rpc_request=rpc_request))
            task_completed, result = self.__wait_task_with_timeout(task=task, timeout=rpc_request.timeout,
                                                                   poll_interval=0.2)
            if not task_completed:
                self.__log.error(
                    "Failed to process rpc request for %s, timeout has been reached",
                    device.name,
                )
                result = {"error": f"Timeout rpc has been reached for {device.name}"}
            elif task_completed:
                self.__log.debug("RPC with method %s execution result is: %s", rpc_request.rpc_method, result)
            self.__gateway.send_rpc_reply(rpc_request.device_name,
                                          rpc_request.id,
                                          {"result": result})
            return result

        except Exception as e:
            self.__log.exception(e)
            self.__gateway.send_rpc_reply(rpc_request.device_name, rpc_request.id,
                                          {"result": {"error": str(e)}})

    async def __process_rpc_request(self, identifier: Node | str, rpc_request: OpcUaRpcRequest):
        result = {}
        try:
            if rpc_request.rpc_method == 'get':
                result = await self.__read_value(identifier)
                return result
            elif rpc_request.rpc_method == 'set':
                result = await self.__write_value(identifier, rpc_request.arguments)
                return result
            else:
                result['response'] = 'Unsupported function code in RPC request.'
                return result
        except Exception as e:
            self.__log.error('Failed to process rpc request: %r', e)
            result['response'] = {'error': e.__repr__()}
            return result

    async def __write_value(self, path, value):
        result = {}

        try:
            var = path
            if isinstance(path, str):
                var = self.__client.get_node(path.replace('\\.', '.'))
            elif isinstance(path, NodeId):
                var = self.__client.get_node(path)

            try:
                await var.write_value(value)
            except (BadWriteNotSupported, BadTypeMismatch):
                value = self.__guess_type_and_cast(value)
                data_value = ua.DataValue(ua.Variant(value))
                await var.write_value(data_value)

            result['value'] = value
            return result
        except UaStringParsingError:
            error_response = f"Could not find identifier in string {path}"
            result['error'] = error_response
            self.__log.error(error_response)
            return result
        except Exception as e:
            result['error'] = e.__str__()
            self.__log.error("Can not find node for provided path %s ", path)
            return result

    @staticmethod
    def __guess_type_and_cast(value):
        if isinstance(value, str):
            if value.lower() in ['true', 'false']:
                return value.lower() == 'true'

            try:
                if '.' in value:
                    return float(value)

                return int(value)
            except ValueError:
                return value

        return value

    async def __read_value(self, path):
        result = {}

        try:
            var = self.__client.get_node(path)
            result['value'] = await var.read_value()
            return result
        except UaStringParsingError:
            error_response = f"Could not find identifier in string {path}"
            result['error'] = error_response
            self.__log.error(error_response)
            return result
        except Exception as e:
            result['error'] = e.__str__()
            self.__log.error("Can not find node for provided path %s ", path)
            return result

    async def __call_method(self, path, method_name, arguments):

        result = {}
        try:
            var = await self.__client.nodes.root.get_child(path)
            method_id = '{}:{}'.format(var.nodeid.NamespaceIndex, method_name)
            result['result'] = await var.call_method(method_id, *arguments)
            return result
        except Exception as e:
            result['error'] = e.__str__()
            self.__log.error("Failed to execute rpc for %s Error: %s", method_name, e)
            self.__log.debug("Error", exc_info=e)
            return result

    async def __fetch_server_limitations(self):
        """Fetch and apply limitations from OPC-UA server capabilities."""
        try:
            server_caps = await self.__client.nodes.server.get_child("0:ServerCapabilities")
            if server_caps is None:
                self.__log.warning("ServerCapabilities node not found.")
                return
            nodes = await server_caps.get_children()

            for node in nodes:
                browse_name = await node.read_browse_name()
                name = browse_name.Name
                if name in {"MaxSessions", "MaxSubscriptions", "MaxBrowseContinuationPoints"}:
                    try:
                        val = await node.read_value()
                        self.__server_limits[name] = val
                    except Exception as e:
                        self.__log.warning("Failed to read %s: %s", name, e)

                elif name == "OperationLimits":
                    try:
                        op_limit_nodes = await node.get_children()
                        for op_node in op_limit_nodes:
                            op_name = (await op_node.read_browse_name()).Name
                            try:
                                val = await op_node.read_value()
                                self.__server_limits[f"OperationLimits.{op_name}"] = val
                            except Exception as e:
                                self.__log.warning("Failed to read OperationLimit %s: %s", op_name, e)
                    except Exception as e:
                        self.__log.warning("Failed to read OperationLimits node children: %s", e)

            self.__max_nodes_per_read = self.__server_limits.get("OperationLimits.MaxNodesPerRead", 100)
            self.__max_nodes_per_subscribe = self.__server_limits.get("OperationLimits.MaxNodesPerSubscribe", 100)

            self.__log.info("Applied server limitations: Read=%s, Subscribe=%s",
                            self.__max_nodes_per_read,
                            self.__max_nodes_per_subscribe)

        except Exception as e:
            self.__log.exception("Failed to fetch server limitations: %s", e)

    @staticmethod
    def __is_node_identifier(path):
        return isinstance(path, str) and re.match(r"(ns=\d+;[isgb]=[^}]+)", path)

    @staticmethod
    def __is_absolute_path(path: str) -> bool:
        pattern = re.compile(r'^\s*.*Root\\.[^}]+.*$')
        try:
            return bool(pattern.match(path))
        except Exception:
            return False


class SubHandler:
    def __init__(self, queue, device, logger, status_change_callback):
        self.__log = logger
        self.device = device
        self.__queue = queue
        self.__status_change_callback = status_change_callback

    def status_change_notification(self, status):
        self.__status_change_callback(status)

    def event_notification(self, event):
        self.__log.debug("New event %s", event)

    def datachange_notification(self, node, _, data):
        self.device.last_subscription_activity = monotonic()
        self.__log.trace("New data change event %s %s", node, data)
        self.__queue.put((node, data, int(time() * 1000)))
