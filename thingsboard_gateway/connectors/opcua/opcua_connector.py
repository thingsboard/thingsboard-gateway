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

import logging
import asyncio
import re
from asyncio.exceptions import CancelledError
from concurrent.futures import ThreadPoolExecutor
from queue import Queue, Empty
from random import choice
from string import ascii_lowercase
from threading import Thread
from time import sleep, monotonic, time
from typing import List, Dict, Union

from thingsboard_gateway.connectors.connector import Connector
from thingsboard_gateway.gateway.constants import CONNECTOR_PARAMETER, RECEIVED_TS_PARAMETER, CONVERTED_TS_PARAMETER, \
    DATA_RETRIEVING_STARTED, REPORT_STRATEGY_PARAMETER
from thingsboard_gateway.gateway.entities.converted_data import ConvertedData
from thingsboard_gateway.gateway.entities.report_strategy_config import ReportStrategyConfig
from thingsboard_gateway.gateway.statistics.statistics_service import StatisticsService
from thingsboard_gateway.gateway.tb_gateway_service import TBGatewayService
from thingsboard_gateway.tb_utility.tb_loader import TBModuleLoader
from thingsboard_gateway.tb_utility.tb_logger import init_logger
from thingsboard_gateway.tb_utility.tb_utility import TBUtility

try:
    import asyncua
except (ImportError, ModuleNotFoundError):
    print("OPC-UA library not found")
    TBUtility.install_package("asyncua")
    import asyncua

from asyncua import ua
from asyncua.ua import NodeId
from asyncua.crypto.security_policies import SecurityPolicyBasic256Sha256, SecurityPolicyBasic256, \
    SecurityPolicyBasic128Rsa15
from asyncua.ua.uaerrors import UaStatusCodeError, BadNodeIdUnknown, BadConnectionClosed, \
    BadInvalidState, BadSessionClosed, BadAttributeIdInvalid, BadCommunicationError, BadOutOfService, BadNoMatch, \
    BadUnexpectedError, UaStatusCodeErrors, BadWaitingForInitialData, BadSessionIdInvalid, BadSubscriptionIdInvalid
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


class OpcUaConnector(Connector, Thread):
    def __init__(self, gateway: 'TBGatewayService', config, connector_type):
        self.statistics = {'MessagesReceived': 0,
                           'MessagesSent': 0}
        super().__init__()
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
        report_strategy = self.__config.get('reportStrategy')
        self.__connector_report_strategy_config = gateway.get_report_strategy_service().get_main_report_strategy()
        try:
            if report_strategy is not None:
                self.__connector_report_strategy_config = ReportStrategyConfig(report_strategy)
        except ValueError as e:
            self.__log.error('Error in report strategy configuration: %s, the gateway main strategy will be used.', e)
        if using_old_configuration_format:
            backward_compatibility_adapter = BackwardCompatibilityAdapter(self.__config, self.__log)
            self.__config = backward_compatibility_adapter.convert()

        self.__server_conf = self.__config.get('server', {})

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
        self.__sub_data_min_batch_creation_time = max(self.__server_conf.get("subDataMinBatchCreationTimeMs", 200), 100) / 1000
        self.__subscription_batch_size = self.__server_conf.get('subscriptionProcessBatchSize', 2000)

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
        if node.get('node') is not None and self.__enable_subscriptions:
            subscription_id = device.nodes_data_change_subscriptions.get(node['node'].nodeid, {}).get('subscription')
            if subscription_id is not None:
                try:
                    await device.subscription.unsubscribe(subscription_id)
                except Exception as e:
                    self.__log.exception('Error unsubscribing from on data change: %s', e)
                device.nodes_data_change_subscriptions[node['node'].nodeid]['subscription'] = None

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
                    self.__client._monitor_server_loop = self._monitor_server_loop
                    self.__client._renew_channel_loop = self._renew_channel_loop
                    self.__client.session_timeout = self.__server_conf.get('sessionTimeoutInMillis', 120000)
                    if self.__server_conf["identity"].get("type") == "cert.PEM":
                        await self.__set_auth_settings_by_cert()
                    if self.__server_conf["identity"].get("username"):
                        self.__set_auth_settings_by_username()
                    if self.__stopped:
                        break
                    if self.__enable_subscriptions and self.__device_nodes:
                        self.__log.debug("Subscriptions are enabled, client will reconnect, unsubscribe old subscriptions and subscribe to new nodes.")
                        await self.retry_connect_with_backoff()
                        self.__last_contact_time = monotonic()
                        if not (self.__client.uaclient.protocol and self.__client.uaclient.protocol.state == 'open'):
                            self.__log.error("Failed to connect to server, retrying...")
                            await self.disconnect_if_connected()
                            continue
                        await self.__unsubscribe_from_nodes()
                        reconnect_required = False

                    self.__nodes_config_cache = {}
                    self.__device_nodes = []
                    self.__next_scan = 0
                    self.__next_poll = 0
                    self.__client_recreation_required = False

                if reconnect_required:
                    await self.retry_connect_with_backoff()
                    self.__last_contact_time = monotonic()

                if not (self.__client.uaclient.protocol and self.__client.uaclient.protocol.state == 'open'):
                    self.__log.error("Failed to connect to server, retrying...")
                    await self.disconnect_if_connected()
                    continue
                self.__log.info("Connected to OPC-UA Server: %s", self.__opcua_url)
                self.__connected = True

                try:
                    await self.__client.load_data_type_definitions()
                except Exception as e:
                    self.__log.error("Error on loading type definitions:\n %s", e)

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

    async def disconnect_if_connected(self):
        if self.__connected:
            try:
                if self.__enable_subscriptions:
                    self.__client_recreation_required = True
                await self.__client.disconnect()
            except Exception:
                pass  # ignore

    async def retry_connect_with_backoff(self, max_retries=8, initial_delay=1, backoff_factor=2):
        last_contact_delta = monotonic() - self.__last_contact_time
        if last_contact_delta < self.__client.session_timeout / 1000 and self.__last_contact_time > 0:
            time_to_wait = self.__client.session_timeout / 1000 - last_contact_delta
            self.__log.info('Last contact was %.2f seconds ago, next connection try in %.2f seconds...',
                             last_contact_delta, time_to_wait)
            await asyncio.sleep(time_to_wait)
        delay = initial_delay
        for attempt in range(max_retries):
            if self.__stopped:
                return None
            try:
                return await self.__client.connect()
            except Exception as e:
                base_time = self.__client.session_timeout / 1000 if (last_contact_delta > 0 and last_contact_delta < self.__client.session_timeout / 1000) else 0
                time_to_wait = base_time / 1000 + delay
                self.__log.error('Encountered error: %r. Next connection try in %i second(s)...', e, time_to_wait)
                await asyncio.sleep(time_to_wait)
                delay *= backoff_factor
        self.__log.error('Max retries reached. Connection failed.')
        return None

    async def __set_auth_settings_by_cert(self):
        try:
            ca_cert = self.__server_conf["identity"].get("caCert")
            private_key = self.__server_conf["identity"].get("privateKey")
            cert = self.__server_conf["identity"].get("cert")
            policy = self.__server_conf["security"]
            mode = self.__server_conf["identity"].get("mode", "SignAndEncrypt")

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

    async def __find_nodes(self, node_list, current_parent_node, nodes, path="Root"):
        assert len(node_list) > 0
        final = []

        children = await current_parent_node.get_children()
        for node in children:
            child_node = await node.read_browse_name()
            if self.__show_map and path:
                self.__log.info('Checking path: %s', path + '.' + f'{child_node.Name}')

            if re.fullmatch(re.escape(node_list[0]), child_node.Name) or node_list[0].split(':')[-1] == child_node.Name:
                if self.__show_map:
                    self.__log.info('Found node: %s', child_node.Name)
                new_nodes = [*nodes, f'{child_node.NamespaceIndex}:{child_node.Name}']
                if len(node_list) == 1:
                    final.append(new_nodes)
                else:
                    final.extend(await self.__find_nodes(node_list[1:], current_parent_node=node, nodes=new_nodes,
                                 path=path + '.' + f'{child_node.Name}'))

        return final

    async def find_nodes(self, node_pattern, current_parent_node=None, nodes=[]):
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
        parent_node = await self.__client.nodes.root.get_child(resolved)

        unresolved = path[resolved_level:]
        return await self.__find_nodes(unresolved, current_parent_node=parent_node, nodes=resolved)

    async def _get_device_info_by_pattern(self, pattern, get_first=False):
        result = []

        search_result = re.search(r"\${([A-Za-z.:\\\d\[\]]+)}", pattern)
        if search_result:
            try:
                group = search_result.group(0)
                node_path = search_result.group(1)
            except IndexError:
                self.__log.error('Invalid pattern: %s', pattern)
                return result

            nodes = await self.find_nodes(node_path)
            self.__log.debug('Found device name nodes: %s', nodes)

            for node in nodes:
                try:
                    var = await self.__client.nodes.root.get_child(node)
                    value = await var.read_value()
                    result.append(pattern.replace(group, str(value)))
                except Exception as e:
                    self.__log.exception(e)
                    continue
        else:
            result.append(pattern)

        return result[0] if len(result) > 0 and get_first else result

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
                if self.__log.isEnabledFor(5):
                    self.__log.debug("Sleeping due to empty batch and converting queue %.3f (seconds)...", sleep_period_after_empty_batch)
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
                            device_converted_data_map[device] = ConvertedData(device_name=device.name)

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
                self.__log.info('Converted data from %r notifications from server for device %s, using converter: %r', len(batch), device.name, device.converter_for_sub.__class__.__name__)

            device_converted_data_map.clear()

    async def __scan_device_nodes(self):
        await self._create_new_devices()
        await self._load_devices_nodes()

    async def _create_new_devices(self):
        self.__log.debug('Scanning for new devices from configuration...')
        existing_devices = list(map(lambda dev: dev.name, self.__device_nodes))

        scanned_devices = []
        for device_config in self.__config.get('mapping', []):
            nodes = await self.find_nodes(device_config['deviceNodePattern'])
            self.__log.debug('Found devices: %s', nodes)

            device_names = await self._get_device_info_by_pattern(
                device_config.get('deviceInfo', {}).get('deviceNameExpression'))

            for device_name in device_names:
                scanned_devices.append(device_name)
                if device_name not in existing_devices:
                    for node in nodes:
                        converter = self.__load_converter(device_config)
                        device_type = await self._get_device_info_by_pattern(
                            device_config.get('deviceInfo', {}).get('deviceProfileExpression', 'default'),
                            get_first=True)
                        device_config = {**device_config, 'device_name': device_name, 'device_type': device_type}
                        self.__device_nodes.append(
                            Device(path=node, name=device_name, config=device_config,
                                   converter=converter(device_config, self.__converter_log),
                                   converter_for_sub=converter(device_config,
                                                               self.__converter_log) if self.__enable_subscriptions else None,
                                   logger=self.__log))
                        self.__log.info('Added device node: %s', device_name)
        self.__log.debug('Device nodes: %s', self.__device_nodes)

    async def _load_devices_nodes(self):
        for device in self.__device_nodes:
            device.nodes = []
            for section in ('attributes', 'timeseries'):
                for node in device.values.get(section, []):
                    try:
                        path = node.get('qualified_path', node['path'])
                        if isinstance(path, str) and re.match(r"(ns=\d+;[isgb]=[^}]+)", path):
                            found_node = self.__client.get_node(path)
                        else:
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
                                node['qualified_path'] = qualified_path[0]
                                path = qualified_path[0]

                            found_node = await self.__client.nodes.root.get_child(path)

                        node_report_strategy = node.get(REPORT_STRATEGY_PARAMETER)
                        if node_report_strategy is not None:
                            try:
                                node_report_strategy = ReportStrategyConfig(node_report_strategy)
                            except ValueError as e:
                                self.__log.error('Error in report strategy configuration: %s, for key %s the device or connector report strategy will be used.', e, node['key'])
                                node_report_strategy = self.__connector_report_strategy_config if device.report_strategy is None else device.report_strategy
                        elif device.report_strategy is not None:
                            node_report_strategy = device.report_strategy

                        node_config = {"node": found_node, "key": node['key'],
                                       "section": section, 'timestampLocation': node.get('timestampLocation', 'gateway')}
                        if node_report_strategy is not None:
                            node_config[REPORT_STRATEGY_PARAMETER] = node_report_strategy
                            node_report_strategy = None # Cleaning for next iteration

                        device.nodes.append(node_config)

                        subscription_exists = (device.subscription is not None
                                               and found_node.nodeid in device.nodes_data_change_subscriptions
                                               and device.nodes_data_change_subscriptions[found_node.nodeid][
                                                   'subscription'] is not None)

                        if node.get('valid') is None or (node.get('valid') and not self.__enable_subscriptions):
                            if self.__enable_subscriptions and not subscription_exists and not self.__stopped:
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

                                device.nodes_data_change_subscriptions[found_node.nodeid]['nodes_configs'].append(node_config)

                                if device.subscription is None:
                                    device.subscription = await self.__client.create_subscription(
                                        self.__sub_check_period_in_millis, SubHandler(
                                            self.__sub_data_to_convert, self.__log, self.status_change_callback))

                                node['id'] = found_node.nodeid.to_string()

                            node['valid'] = True
                    except ConnectionError as e:
                        raise e
                    except (BadNodeIdUnknown, BadConnectionClosed, BadInvalidState, BadAttributeIdInvalid,
                            BadCommunicationError, BadOutOfService, BadNoMatch, BadUnexpectedError,
                            UaStatusCodeErrors,
                            BadWaitingForInitialData):
                        if node.get('valid', True):
                            self.__log.warning('Node not found (2); device: %s, key: %s, path: %s', device.name,
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
                            nodes_data_change_subscriptions = await self._subscribe_for_node_updates_in_batches(device,
                                                                                                         nodes_to_subscribe,
                                                                                                         self.__subscription_batch_size)
                            subs = []
                            for subs_batch in nodes_data_change_subscriptions:
                                subs.extend(subs_batch)
                            for node, conf, subscription_id in zip(nodes_to_subscribe, conf, subs):
                                device.nodes_data_change_subscriptions[node.nodeid]['subscription'] = subscription_id
                            self.__log.info('Subscribed to %i nodes for device %s', len(nodes_to_subscribe), device.name)
                    else:
                        self.__log.debug('No nodes to subscribe for device %s', device.name)
                else:
                    self.__log.debug('Subscriptions are disabled for device %s or device subscription is None', device.name)
            except Exception as e:
                self.__log.exception("Error loading nodes: %s", e)
                raise e

    async def _subscribe_for_node_updates_in_batches(self, device, nodes, batch_size=500):
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
                    self.__log.error("Failed subscribing to nodes, server returned the following reasons: %r, nodes count with these problems - %r", set(reasons), len(reasons))
                    self.__log.trace("Failed nodes: %r", bad_results)
                result.append(subscription_result)
                successfully_processed = batch_len - len(bad_results)
                total_successfully_subscribed += successfully_processed
                total_failed_to_subscribe += len(bad_results)
                if successfully_processed > 0:
                    self.__log.info("Succesfully subscribed to batch number %i with %i nodes.", i // batch_size + 1, successfully_processed)
                else:
                    self.__log.warning("Failed to subscribe to batch number %i with %i nodes.", i // batch_size + 1, batch_len)
            except Exception as e:
                self.__log.warn("Error subscribing to batch %i with %i : %r", i // batch_size + 1, batch_len, e)
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
            values = await self.__client.read_attributes(all_nodes)
            received_ts = int(time() * 1000)

            self.__data_to_convert.put((values, received_ts, data_retrieving_started))

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
                converted_data: ConvertedData = self.__convert_device_data(device.converter, device.nodes, device_values)
                converted_data.add_to_metadata({
                    CONNECTOR_PARAMETER: self.get_name(),
                    RECEIVED_TS_PARAMETER: received_ts,
                    CONVERTED_TS_PARAMETER: int(time() * 1000),
                    DATA_RETRIEVING_STARTED: data_retrieving_started
                })
                if converted_data:
                    self.__gateway.send_to_storage(self.get_name(), self.get_id(), converted_data)

                    StatisticsService.count_connector_message(self.name, stat_parameter_name='connectorMsgsReceived')
                    #TODO: Should these counters be here, or on upper level?
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

    async def get_shared_attr_node_id(self, path, result={}):
        try:
            q_path = await self.find_node_name_space_index(path)
            result['result'] = await self.__client.nodes.root.get_child(q_path[0])
        except Exception as e:
            result['error'] = e.__str__()

    def on_attributes_update(self, content):
        self.__log.debug(content)
        try:
            device = tuple(filter(lambda i: i.name == content['device'], self.__device_nodes))[0]

            for (key, value) in content['data'].items():
                for attr_update in device.config['attributes_updates']:
                    if attr_update['key'] == key:
                        result = {}
                        task = self.__loop.create_task(
                            self.get_shared_attr_node_id(
                                device.path + attr_update['value'].replace('\\', '').split('.'), result))

                        while not task.done():
                            sleep(.1)

                        if result.get('error'):
                            self.__log.error('Node not found! (%s)', result['error'])
                            return

                        node_id = result['result']
                        self.__loop.create_task(self.__write_value(node_id, value))
                        return
        except Exception as e:
            self.__log.exception(e)

    def server_side_rpc_handler(self, content):
        try:
            if content.get('data') is None:
                content['data'] = {'params': content['params'], 'method': content['method'], 'id': content['id']}

            rpc_method = content["data"].get("method")

            # check if RPC type is connector RPC (can be only 'get' or 'set')
            try:
                (connector_type, rpc_method_name) = rpc_method.split('_')
                if connector_type == self._connector_type:
                    rpc_method = rpc_method_name
                    content['device'] = content['params'].split(' ')[0].split('=')[-1]
            except (ValueError, IndexError):
                self.__log.error('Invalid RPC method name: %s', rpc_method)
            except AttributeError:
                pass

            if content.get('device'):
                # firstly check if a method is not service
                if rpc_method == 'set' or rpc_method == 'get':
                    full_path = ''
                    args_list = []
                    device = content.get('device')

                    try:
                        args_list = content['data']['params'].split(';')

                        if 'ns' in content['data']['params']:
                            full_path = ';'.join(
                                [item for item in (args_list[0:-1] if rpc_method == 'set' else args_list)])
                        else:
                            full_path = args_list[0].split('=')[-1]
                    except IndexError:
                        self.__log.error('Not enough arguments. Expected min 2.')
                        self.__gateway.send_rpc_reply(device=device,
                                                      req_id=content['data'].get('id'),
                                                      content={content['data'][
                                                                   'method']: 'Not enough arguments. Expected min 2.',
                                                               'code': 400})

                    result = {}
                    if rpc_method == 'get':
                        task = self.__loop.create_task(self.__read_value(full_path, result))

                        while not task.done():
                            sleep(.2)
                    elif rpc_method == 'set':
                        value = args_list[2].split('=')[-1]
                        task = self.__loop.create_task(self.__write_value(full_path, value, result))

                        while not task.done():
                            sleep(.2)

                    self.__gateway.send_rpc_reply(device=device,
                                                  req_id=content['data'].get('id'),
                                                  content={'result': result})
                else:
                    device = tuple(filter(lambda i: i.name == content['device'], self.__device_nodes))[0]

                    for rpc in device.config['rpc_methods']:
                        if rpc['method'] == content["data"]['method']:
                            arguments_from_config = rpc["arguments"]
                            arguments = content["data"].get("params") if content["data"].get(
                                "params") is not None else arguments_from_config
                            method_name = content['data']['method']

                            try:
                                result = {}
                                task = self.__loop.create_task(
                                    self.__call_method(device.path, method_name, arguments, result))

                                while not task.done():
                                    sleep(.2)

                                self.__gateway.send_rpc_reply(content["device"],
                                                              content["data"]["id"],
                                                              {content["data"]["method"]: result, "code": 200})
                                self.__log.debug("method %s result is: %s", rpc['method'], result)
                            except Exception as e:
                                self.__log.exception(e)
                                self.__gateway.send_rpc_reply(content["device"], content["data"]["id"],
                                                              {"error": str(e), "code": 500})
                        else:
                            self.__log.error("Method %s not found for device %s", rpc_method, content["device"])
                            self.__gateway.send_rpc_reply(content["device"], content["data"]["id"],
                                                          {"error": "%s - Method not found" % rpc_method,
                                                           "code": 404})
            else:
                results = []
                for device in self.__device_nodes:
                    content['device'] = device.name

                    arguments = content['data']['params']["arguments"]

                    try:
                        result = {}
                        task = self.__loop.create_task(
                            self.__call_method(device.path, rpc_method, arguments, result))

                        while not task.done():
                            sleep(.2)

                        results.append(result)
                        self.__log.debug("method %s result is: %s", rpc_method, result)
                    except Exception as e:
                        self.__log.exception(e)
                        self.__gateway.send_rpc_reply(content["device"], content["data"]["id"],
                                                      {"error": str(e), "code": 500})

                return results
        except Exception as e:
            self.__log.exception("Error during RPC handling: ", exc_info=e)

    async def __write_value(self, path, value, result={}):
        try:
            var = path
            if isinstance(path, str):
                var = self.__client.get_node(path.replace('\\.', '.'))

            await var.write_value(value)
        except Exception as e:
            result['error'] = e.__str__()

    async def __read_value(self, path, result={}):
        try:
            var = self.__client.get_node(path)
            result['value'] = await var.read_value()
        except Exception as e:
            result['error'] = e.__str__()

    async def __call_method(self, path, method_name, arguments, result={}):
        try:
            var = await self.__client.nodes.root.get_child(path)
            method_id = '{}:{}'.format(var.nodeid.NamespaceIndex, method_name)
            result['result'] = await var.call_method(method_id, *arguments)
        except Exception as e:
            result['error'] = e.__str__()

    def update_converter_config(self, converter_name, config):
        self.__log.debug('Received remote converter configuration update for %s with configuration %s', converter_name,
                         config)
        for device in self.__device_nodes:
            if device.converter.__class__.__name__ == converter_name:
                device.config.update(config)
                device.load_values()
                self.__log.info('Updated converter configuration for: %s with configuration %s',
                                converter_name, device.config)

                for node_config in self.__config['mapping']:
                    if node_config['deviceNodePattern'] == device.config['deviceNodePattern']:
                        node_config.update(config)

                self.__gateway.update_connector_config_file(self.name, {'server': self.__server_conf,
                                                                        'mapping': self.__config.get('mapping', [])})


class SubHandler:
    def __init__(self, queue, logger, status_change_callback):
        self.__log = logger
        self.__queue = queue
        self.__status_change_callback = status_change_callback

    def status_change_notification(self, status):
        self.__status_change_callback(status)

    def event_notification(self, event):
        self.__log.debug("New event %s", event)

    def datachange_notification(self, node, _, data):
        self.__log.trace("New data change event %s %s", node, data)
        self.__queue.put((node, data, int(time() * 1000)))
