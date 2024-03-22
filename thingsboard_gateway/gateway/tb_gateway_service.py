#     Copyright 2024. ThingsBoard
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
import logging.config
import logging.handlers
import multiprocessing.managers
import os.path
import subprocess
from os import execv, listdir, path, pathsep, stat, system, environ
from platform import system as platform_system
from queue import SimpleQueue
from random import choice
from signal import signal, SIGINT
from string import ascii_lowercase, hexdigits
from sys import argv, executable, getsizeof
from threading import RLock, Thread, main_thread, current_thread
from time import sleep, time

from simplejson import JSONDecodeError, dumps, load, loads
from yaml import safe_load

from thingsboard_gateway.connectors.connector import Connector
from thingsboard_gateway.gateway.constant_enums import DeviceActions, Status
from thingsboard_gateway.gateway.constants import CONNECTED_DEVICES_FILENAME, CONNECTOR_PARAMETER, \
    PERSISTENT_GRPC_CONNECTORS_KEY_FILENAME, RENAMING_PARAMETER, CONNECTOR_NAME_PARAMETER, DEVICE_TYPE_PARAMETER, \
    CONNECTOR_ID_PARAMETER, ATTRIBUTES_FOR_REQUEST
from thingsboard_gateway.gateway.device_filter import DeviceFilter
from thingsboard_gateway.gateway.duplicate_detector import DuplicateDetector
from thingsboard_gateway.gateway.shell.proxy import AutoProxy
from thingsboard_gateway.gateway.statistics_service import StatisticsService
from thingsboard_gateway.gateway.tb_client import TBClient
from thingsboard_gateway.storage.file.file_event_storage import FileEventStorage
from thingsboard_gateway.storage.memory.memory_event_storage import MemoryEventStorage
from thingsboard_gateway.storage.sqlite.sqlite_event_storage import SQLiteEventStorage
from thingsboard_gateway.tb_utility.tb_gateway_remote_configurator import RemoteConfigurator
from thingsboard_gateway.tb_utility.tb_handler import TBLoggerHandler
from thingsboard_gateway.tb_utility.tb_loader import TBModuleLoader
from thingsboard_gateway.tb_utility.tb_logger import TbLogger
from thingsboard_gateway.tb_utility.tb_remote_shell import RemoteShell
from thingsboard_gateway.tb_utility.tb_updater import TBUpdater
from thingsboard_gateway.tb_utility.tb_utility import TBUtility

GRPC_LOADED = False
try:
    from thingsboard_gateway.gateway.grpc_service.grpc_connector import GrpcConnector
    from thingsboard_gateway.gateway.grpc_service.tb_grpc_manager import TBGRPCServerManager

    GRPC_LOADED = True
except ImportError:
    print("Cannot load GRPC connector!")

log: TbLogger = None
main_handler = logging.handlers.MemoryHandler(-1)

DEFAULT_CONNECTORS = {
    "mqtt": "MqttConnector",
    "modbus": "ModbusConnector",
    "opcua": "OpcUaConnectorAsyncIO",
    "opcua_asyncio": "OpcUaConnectorAsyncIO",
    "ble": "BLEConnector",
    "request": "RequestConnector",
    "can": "CanConnector",
    "bacnet": "BACnetConnector",
    "odbc": "OdbcConnector",
    "rest": "RESTConnector",
    "snmp": "SNMPConnector",
    "ftp": "FTPConnector",
    "socket": "SocketConnector",
    "xmpp": "XMPPConnector",
    "ocpp": "OcppConnector",
}

DEFAULT_STATISTIC = {
    'enable': True,
    'statsSendPeriodInSeconds': 3600
}

DEFAULT_DEVICE_FILTER = {
    'enable': False
}

SECURITY_VAR = ('accessToken', 'caCert', 'privateKey', 'cert', 'clientId', 'username', 'password')


def load_file(path_to_file):
    content = None
    with open(path_to_file, 'r') as target_file:
        content = load(target_file)
    return content


def get_env_variables():
    env_variables = {
        'host': environ.get('host'),
        'port': int(environ.get('port')) if environ.get('port') else None,
        'accessToken': environ.get('accessToken'),
        'caCert': environ.get('caCert'),
        'privateKey': environ.get('privateKey'),
        'cert': environ.get('cert'),
        'clientId': environ.get('clientId'),
        'username': environ.get('username'),
        'password': environ.get('password')
    }

    if environ.get('TB_GW_HOST'):
        env_variables['host'] = environ.get('TB_GW_HOST')
    if environ.get('TB_GW_PORT'):
        env_variables['port'] = int(environ.get('TB_GW_PORT'))
    if environ.get('TB_GW_ACCESS_TOKEN'):
        env_variables['accessToken'] = environ.get('TB_GW_ACCESS_TOKEN')
    if environ.get('TB_GW_CA_CERT'):
        env_variables['caCert'] = environ.get('TB_GW_CA_CERT')
    if environ.get('TB_GW_PRIVATE_KEY'):
        env_variables['privateKey'] = environ.get('TB_GW_PRIVATE_KEY')
    if environ.get('TB_GW_CERT'):
        env_variables['cert'] = environ.get('TB_GW_CERT')
    if environ.get('TB_GW_CLIENT_ID'):
        env_variables['clientId'] = environ.get('TB_GW_CLIENT_ID')
    if environ.get('TB_GW_USERNAME'):
        env_variables['username'] = environ.get('TB_GW_USERNAME')
    if environ.get('TB_GW_PASSWORD'):
        env_variables['password'] = environ.get('TB_GW_PASSWORD')
    if environ.get('TB_GW_RATE_LIMITS'):
        env_variables['rateLimits'] = environ.get('TB_GW_RATE_LIMITS')

    converted_env_variables = {}

    for (key, value) in env_variables.items():
        if value is not None:
            if key in SECURITY_VAR:
                if not converted_env_variables.get('security'):
                    converted_env_variables['security'] = {}

                converted_env_variables['security'][key] = value
            else:
                converted_env_variables[key] = value

    return converted_env_variables


class GatewayManager(multiprocessing.managers.BaseManager):
    def __init__(self, address=None, authkey=b''):
        super().__init__(address=address, authkey=authkey)
        self.gateway = None

    def has_gateway(self):
        return self.gateway is not None

    def add_gateway(self, gateway):
        self.gateway = gateway

    def shutdown(self) -> None:
        super().__exit__(None, None, None)


class TBGatewayService:
    EXPOSED_GETTERS = [
        'ping',
        'get_status',
        'get_storage_name',
        'get_storage_events_count',
        'get_available_connectors',
        'get_connector_status',
        'get_connector_config'
    ]

    def __init__(self, config_file=None):
        self.__init_variables()
        if current_thread() is main_thread():
            signal(SIGINT, lambda _, __: self.__stop_gateway())

        self.__lock = RLock()
        self.async_device_actions = {
            DeviceActions.CONNECT: self.add_device,
            DeviceActions.DISCONNECT: self.del_device
        }
        self.__async_device_actions_queue = SimpleQueue()
        self.__process_async_actions_thread = Thread(target=self.__process_async_device_actions,
                                                     name="Async device actions processing thread", daemon=True)

        self._config_dir = path.dirname(path.abspath(config_file)) + path.sep
        if config_file is None:
            config_file = (path.dirname(path.dirname(path.abspath(__file__))) +
                           '/config/tb_gateway.json'.replace('/', path.sep))

        logging_error = None
        try:
            with open(self._config_dir + 'logs.json', 'r') as file:
                log_config = load(file)
            logging.config.dictConfig(log_config)
        except Exception as e:
            logging_error = e

        global log
        log = TbLogger('service', gateway=self, level='INFO')
        global main_handler
        self.main_handler = main_handler
        log.addHandler(self.main_handler)

        # load general configuration YAML/JSON
        self.__config = self.__load_general_config(config_file)

        # change main config if Gateway running with docker env variables
        self.__modify_main_config()

        log.info("Gateway starting...")
        self.__updater = TBUpdater()
        self.__updates_check_period_ms = 300000
        self.__updates_check_time = 0
        self.version = self.__updater.get_version()
        log.info("ThingsBoard IoT gateway version: %s", self.version["current_version"])
        self.available_connectors_by_name: dict[str, Connector] = {}
        self.available_connectors_by_id: dict[str, Connector] = {}
        self.__connector_incoming_messages = {}
        self.__connected_devices = {}
        self.__renamed_devices = {}
        self.__saved_devices = {}
        self.__events = []
        self.name = ''.join(choice(ascii_lowercase) for _ in range(64))
        self.__rpc_register_queue = SimpleQueue()
        self.__rpc_requests_in_progress = {}
        connection_logger = logging.getLogger('tb_connection')
        self.tb_client = TBClient(self.__config["thingsboard"], self._config_dir, connection_logger)
        try:
            self.tb_client.disconnect()
        except Exception as e:
            log.exception(e)
        self.tb_client.register_service_subscription_callback(self.subscribe_to_required_topics)
        self.tb_client.connect()
        if logging_error is not None:
            self.tb_client.client.send_telemetry({"ts": time() * 1000, "values": {
                "LOGS": "Logging loading exception, logs.json is wrong: %s" % (str(logging_error),)}})
            TBLoggerHandler.set_default_handler()
        self.remote_handler = TBLoggerHandler(self)
        log.addHandler(self.remote_handler)
        # self.main_handler.setTarget(self.remote_handler)
        self._default_connectors = DEFAULT_CONNECTORS
        self.__converted_data_queue = SimpleQueue()
        self.__save_converted_data_thread = Thread(name="Storage fill thread", daemon=True,
                                                   target=self.__send_to_storage)
        self.__save_converted_data_thread.start()
        self._implemented_connectors = {}
        self._event_storage_types = {
            "memory": MemoryEventStorage,
            "file": FileEventStorage,
            "sqlite": SQLiteEventStorage,
        }
        self.__gateway_rpc_methods = {
            "ping": self.__rpc_ping,
            "stats": self.__form_statistics,
            "devices": self.__rpc_devices,
            "update": self.__rpc_update,
            "version": self.__rpc_version,
            "device_renamed": self.__process_renamed_gateway_devices,
            "device_deleted": self.__process_deleted_gateway_devices,
        }

        self.init_remote_shell(self.__config["thingsboard"].get("remoteShell"))

        self.__scheduled_rpc_calls = []
        self.__rpc_processing_queue = SimpleQueue()
        self.__rpc_scheduled_methods_functions = {
            "restart": {"function": execv, "arguments": (executable, [executable.split(pathsep)[-1]] + argv)},
            "reboot": {"function": subprocess.call, "arguments": (["shutdown", "-r", "-t", "0"],)},
        }
        self.__rpc_processing_thread = Thread(target=self.__send_rpc_reply_processing, daemon=True,
                                              name="RPC processing thread")
        self.__rpc_processing_thread.start()
        self._event_storage = self._event_storage_types[self.__config["storage"]["type"]](self.__config["storage"])
        self.connectors_configs = {}

        self.init_grpc_service(self.__config.get('grpc'))

        self.__devices_idle_checker = self.__config['thingsboard'].get('checkingDeviceActivity', {})
        self.__check_devices_idle = self.__devices_idle_checker.get('checkDeviceInactivity', False)
        if self.__check_devices_idle:
            thread = Thread(name='Checking devices idle time', target=self.__check_devices_idle_time, daemon=True)
            thread.start()
            log.info('Start checking devices idle time')

        self.init_statistics_service(self.__config['thingsboard'].get('statistics', DEFAULT_STATISTIC))

        self._published_events = SimpleQueue()

        self.__min_pack_send_delay_ms = self.__config['thingsboard'].get('minPackSendDelayMS', 200)
        self.__min_pack_send_delay_ms = self.__min_pack_send_delay_ms / 1000.0
        self.__min_pack_size_to_send = self.__config['thingsboard'].get('minPackSizeToSend', 50)

        self._send_thread = Thread(target=self.__read_data_from_storage, daemon=True,
                                   name="Send data to Thingsboard Thread")
        self._send_thread.start()

        self.init_device_filtering(self.__config['thingsboard'].get('deviceFiltering', DEFAULT_DEVICE_FILTER))

        self.__duplicate_detector = DuplicateDetector(self.available_connectors_by_name)

        log.info("Gateway started.")

        self._watchers_thread = Thread(target=self._watchers, name='Watchers', daemon=True)
        self._watchers_thread.start()

        self._load_connectors()
        self.__connect_with_connectors()
        self.__load_persistent_devices()

        self.__init_remote_configuration()

        if path.exists('/tmp/gateway'):
            try:
                # deleting old manager if it was closed incorrectly
                system('rm -rf /tmp/gateway')
            except OSError as e:
                log.exception(e)

        manager_address = '/tmp/gateway'
        if platform_system() == 'Windows':
            manager_address = ('127.0.0.1', 9999)
        self.manager = GatewayManager(address=manager_address, authkey=b'gateway')

        if current_thread() is main_thread():
            GatewayManager.register('get_gateway',
                                    self.get_gateway,
                                    proxytype=AutoProxy,
                                    exposed=self.EXPOSED_GETTERS,
                                    create_method=False)
            self.server = self.manager.get_server()
            self.server.serve_forever()

    def __init_variables(self):
        self.stopped = False
        self.__device_filter_config = None
        self.__device_filter = None
        self.__grpc_manager = None
        self.__remote_shell = None
        self.__statistics = None
        self.__statistics_service = None
        self.__grpc_config = None
        self.__grpc_connectors = None
        self.__grpc_manager = None
        self.__remote_configurator = None
        self.__request_config_after_connect = False
        self.__rpc_reply_sent = False
        self.__subscribed_to_rpc_topics = False
        self.__rpc_remote_shell_command_in_progress = None

    @staticmethod
    def __load_general_config(config_file):
        file_extension = config_file.split('.')[-1]
        if file_extension == 'json' and os.path.exists(config_file):
            with open(config_file) as general_config:
                try:
                    return load(general_config)
                except Exception as e:
                    log.exception('Failed to load configuration file:\n %s', e)
        else:
            log.warning('YAML configuration is deprecated. '
                        'Please, use JSON configuration instead.')
            log.warning(
                'See default configuration on '
                'https://thingsboard.io/docs/iot-gateway/configuration/')

            config = {}
            try:
                filename = ''.join(config_file.split('.')[:-1])
                with open(filename + '.yaml') as general_config:
                    config = safe_load(general_config)

                with open(filename + '.json', 'w') as file:
                    file.writelines(dumps(config, indent='  '))
            except Exception as e:
                log.exception('Failed to load configuration file:\n %s', e)

            return config

    def init_grpc_service(self, config):
        self.__grpc_config = config
        self.__grpc_connectors = {}
        if GRPC_LOADED and self.__grpc_config is not None and self.__grpc_config.get("enabled"):
            self.__process_async_actions_thread.start()
            self.__grpc_manager = TBGRPCServerManager(self, self.__grpc_config)
            self.__grpc_manager.set_gateway_read_callbacks(self.__register_connector, self.__unregister_connector)

    def init_statistics_service(self, config):
        self.__statistics = config
        if self.__statistics['enable']:
            if isinstance(self.__statistics_service, StatisticsService):
                self.__statistics_service.stop()
            self.__statistics_service = StatisticsService(self.__statistics['statsSendPeriodInSeconds'], self, log,
                                                          config_path=self._config_dir + self.__statistics[
                                                              'configuration'] if self.__statistics.get(
                                                              'configuration') else None)
        else:
            self.__statistics_service = None

    def init_device_filtering(self, config):
        self.__device_filter_config = config
        self.__device_filter = None
        if self.__device_filter_config['enable']:
            self.__device_filter = DeviceFilter(config_path=self._config_dir + self.__device_filter_config[
                'filterFile'] if self.__device_filter_config.get('filterFile') else None)
        else:
            self.__device_filter = None

    def init_remote_shell(self, enable):
        self.__remote_shell = None
        if enable:
            log.warning("Remote shell is enabled. Please be carefully with this feature.")
            self.__remote_shell = RemoteShell(platform=self.__updater.get_platform(),
                                              release=self.__updater.get_release())
        else:
            self.__remote_shell = None

    @property
    def event_storage_types(self):
        return self._event_storage_types

    @property
    def config(self):
        return self.__config

    @config.setter
    def config(self, config):
        self.__config.update(config)

    def get_gateway(self):
        if self.manager.has_gateway():
            return self.manager.gateway
        else:
            self.manager.add_gateway(self)
            self.manager.register('gateway', lambda: self, proxytype=AutoProxy)

    def _watchers(self):
        try:
            gateway_statistic_send = 0
            connectors_configuration_check_time = 0

            while not self.stopped:
                cur_time = time() * 1000

                if not self.tb_client.client.is_connected() and self.__subscribed_to_rpc_topics:
                    self.__subscribed_to_rpc_topics = False

                if self.tb_client.is_connected() and not self.tb_client.is_stopped() and not self.__subscribed_to_rpc_topics:
                    for device in list(self.__saved_devices.keys()):
                        self.add_device(device,
                                        {CONNECTOR_PARAMETER: self.__saved_devices[device][CONNECTOR_PARAMETER]},
                                        device_type=self.__saved_devices[device][DEVICE_TYPE_PARAMETER])
                    self.subscribe_to_required_topics()

                if self.__scheduled_rpc_calls:
                    for rpc_call_index in range(len(self.__scheduled_rpc_calls)):
                        rpc_call = self.__scheduled_rpc_calls[rpc_call_index]
                        if cur_time > rpc_call[0]:
                            rpc_call = self.__scheduled_rpc_calls.pop(rpc_call_index)
                            result = None
                            try:
                                result = rpc_call[1]["function"](*rpc_call[1]["arguments"])
                            except Exception as e:
                                log.exception(e)

                            if result == 256:
                                log.warning("Error on RPC command: 256. Permission denied.")

                if ((self.__rpc_requests_in_progress or not self.__rpc_register_queue.empty())
                        and self.tb_client.is_connected()):
                    new_rpc_request_in_progress = {}
                    if self.__rpc_requests_in_progress:
                        for rpc_in_progress, data in self.__rpc_requests_in_progress.items():
                            if cur_time >= data[1]:
                                data[2](rpc_in_progress)
                                self.cancel_rpc_request(rpc_in_progress)
                                self.__rpc_requests_in_progress[rpc_in_progress] = "del"
                        new_rpc_request_in_progress = {key: value for key, value in
                                                       self.__rpc_requests_in_progress.items() if value != 'del'}
                    if not self.__rpc_register_queue.empty():
                        new_rpc_request_in_progress = self.__rpc_requests_in_progress
                        rpc_request_from_queue = self.__rpc_register_queue.get(False)
                        topic = rpc_request_from_queue["topic"]
                        data = rpc_request_from_queue["data"]
                        new_rpc_request_in_progress[topic] = data
                    self.__rpc_requests_in_progress = new_rpc_request_in_progress
                else:
                    try:
                        sleep(0.2)
                    except Exception as e:
                        log.exception(e)
                        break

                if (not self.__request_config_after_connect and self.tb_client.is_connected()
                        and not self.tb_client.client.get_subscriptions_in_progress()):
                    self.__request_config_after_connect = True
                    self._check_shared_attributes()

                if (cur_time - gateway_statistic_send > self.__statistics['statsSendPeriodInSeconds'] * 1000
                        and self.tb_client.is_connected()):
                    summary_messages = self.__form_statistics()
                    # with self.__lock:
                    self.tb_client.client.send_telemetry(summary_messages)
                    gateway_statistic_send = time() * 1000
                    # self.__check_shared_attributes()

                if (cur_time - connectors_configuration_check_time > self.__config["thingsboard"].get("checkConnectorsConfigurationInSeconds", 60) * 1000
                        and not (self.__remote_configurator is not None and self.__remote_configurator.in_process)):
                    self.check_connector_configuration_updates()
                    connectors_configuration_check_time = time() * 1000

                if cur_time - self.__updates_check_time >= self.__updates_check_period_ms:
                    self.__updates_check_time = time() * 1000
                    self.version = self.__updater.get_version()
        except Exception as e:
            log.exception(e)
            self.__stop_gateway()
            self.__close_connectors()
            log.info("The gateway has been stopped.")
            self.tb_client.stop()

    def __modify_main_config(self):
        env_variables = get_env_variables()
        self.__config['thingsboard'] = {**self.__config['thingsboard'], **env_variables}

    def __close_connectors(self):
        for current_connector in self.available_connectors_by_id:
            try:
                self.available_connectors_by_id[current_connector].close()
                log.debug("Connector %s closed connection.", current_connector)
            except Exception as e:
                log.exception(e)

    def __stop_gateway(self):
        self.stopped = True
        self.__updater.stop()
        log.info("Stopping...")

        if self.__statistics_service is not None:
            self.__statistics_service.stop()

        if self.__grpc_manager is not None:
            self.__grpc_manager.stop()
        if os.path.exists("/tmp/gateway"):
            os.remove("/tmp/gateway")
        self.__close_connectors()
        if hasattr(self, "_event_storage"):
            self._event_storage.stop()
        log.info("The gateway has been stopped.")
        self.tb_client.disconnect()
        self.tb_client.stop()
        if hasattr(self, "manager"):
            self.manager.shutdown()

    def __init_remote_configuration(self, force=False):
        remote_configuration_enabled = self.__config["thingsboard"].get("remoteConfiguration")
        if not remote_configuration_enabled and force:
            log.info("Remote configuration is enabled forcibly!")
        if (remote_configuration_enabled or force) and self.__remote_configurator is None:
            try:
                self.__remote_configurator = RemoteConfigurator(self, self.__config)
                if self.tb_client.is_connected() and not self.tb_client.client.get_subscriptions_in_progress():
                    self._check_shared_attributes()
            except Exception as e:
                log.exception(e)
        if self.__remote_configurator is not None:
            self.__remote_configurator.send_current_configuration()

    def _attributes_parse(self, content, *args):
        try:
            log.debug("Received data: %s", content)
            if content is not None:
                shared_attributes = content.get("shared", {})
                client_attributes = content.get("client", {})
                if shared_attributes or client_attributes:
                    self.__process_attributes_response(shared_attributes, client_attributes)
                else:
                    self.__process_attribute_update(content)

                if shared_attributes:
                    log.debug("Shared attributes received (%s).",
                              ", ".join([attr for attr in shared_attributes.keys()]))
                if client_attributes:
                    log.debug("Client attributes received (%s).",
                              ", ".join([attr for attr in client_attributes.keys()]))
        except Exception as e:
            log.exception(e)

    def __process_attribute_update(self, content):
        self.__process_remote_logging_update(content.get("RemoteLoggingLevel"))
        self.__process_remote_configuration(content)
        self.__process_remote_converter_configuration_update(content)

    def __process_attributes_response(self, shared_attributes, client_attributes):
        self.__process_remote_logging_update(shared_attributes.get('RemoteLoggingLevel'))
        self.__process_remote_configuration(shared_attributes)

    def __process_remote_logging_update(self, remote_logging_level):
        if remote_logging_level == 'NONE':
            self.remote_handler.deactivate()
            log.info('Remote logging has being deactivated.')
        elif remote_logging_level is not None:
            if self.remote_handler.current_log_level != remote_logging_level or not self.remote_handler.activated:
                self.main_handler.setLevel(remote_logging_level)
                self.remote_handler.activate(remote_logging_level)
                log.info('Remote logging has being updated. Current logging level is: %s ',
                         remote_logging_level)

    def __process_remote_converter_configuration_update(self, content: dict):
        try:
            key = list(content.keys())[0]
            connector_name, converter_name = key.split(':')
            log.info('Got remote converter configuration update')
            if not self.available_connectors_by_name.get(connector_name):
                raise ValueError

            self.available_connectors_by_name[connector_name].update_converter_config(converter_name, content[key])
        except (ValueError, AttributeError, IndexError) as e:
            log.debug('Failed to process remote converter update: %s', e)

    def update_connector_config_file(self, connector_name, config):
        for connector in self.__config['connectors']:
            if connector['name'] == connector_name:
                with open(self._config_dir + connector['configuration'], 'w', encoding='UTF-8') as file:
                    file.writelines(dumps(config))

                log.info('Updated %s configuration file', connector_name)
                return

    def __process_deleted_gateway_devices(self, deleted_device_name: str):
        log.info("Received deleted gateway device notification: %s", deleted_device_name)
        if deleted_device_name in list(self.__renamed_devices.values()):
            first_device_name = TBUtility.get_dict_key_by_value(self.__renamed_devices, deleted_device_name)
            del self.__renamed_devices[first_device_name]
            deleted_device_name = first_device_name
            log.debug("Current renamed_devices dict: %s", self.__renamed_devices)
        if deleted_device_name in self.__connected_devices:
            del self.__connected_devices[deleted_device_name]
            log.debug("Device %s - was removed from __connected_devices", deleted_device_name)
        if deleted_device_name in self.__saved_devices:
            del self.__saved_devices[deleted_device_name]
            log.debug("Device %s - was removed from __saved_devices", deleted_device_name)
        self.__duplicate_detector.delete_device(deleted_device_name)
        self.__save_persistent_devices()
        self.__load_persistent_devices()

    def __process_renamed_gateway_devices(self, renamed_device: dict):
        if self.__config.get('handleDeviceRenaming', True):
            log.info("Received renamed gateway device notification: %s", renamed_device)
            old_device_name, new_device_name = renamed_device.popitem()
            if old_device_name in list(self.__renamed_devices.values()):
                device_name_key = TBUtility.get_dict_key_by_value(self.__renamed_devices, old_device_name)
            else:
                device_name_key = new_device_name
            self.__renamed_devices[device_name_key] = new_device_name
            self.__duplicate_detector.rename_device(old_device_name, new_device_name)

            self.__save_persistent_devices()
            self.__load_persistent_devices()
            log.debug("Current renamed_devices dict: %s", self.__renamed_devices)
        else:
            log.debug("Received renamed device notification %r, but device renaming handle is disabled",
                      renamed_device)

    def __process_remote_configuration(self, new_configuration):
        if new_configuration is not None and self.__remote_configurator is not None:
            try:
                self.__remote_configurator.process_config_request(new_configuration)
            except Exception as e:
                log.exception(e)

    def get_config_path(self):
        return self._config_dir

    def subscribe_to_required_topics(self):
        if not self.__subscribed_to_rpc_topics:
            self.tb_client.client.clean_device_sub_dict()
            self.tb_client.client.gw_set_server_side_rpc_request_handler(self._rpc_request_handler)
            self.tb_client.client.set_server_side_rpc_request_handler(self._rpc_request_handler)
            self.tb_client.client.subscribe_to_all_attributes(self._attribute_update_callback)
            self.tb_client.client.gw_subscribe_to_all_attributes(self._attribute_update_callback)
            self.__subscribed_to_rpc_topics = True

    def request_device_attributes(self, device_name, shared_keys, client_keys, callback):
        if client_keys is not None:
            self.tb_client.client.gw_request_client_attributes(device_name, client_keys, callback)
        if shared_keys is not None:
            self.tb_client.client.gw_request_shared_attributes(device_name, shared_keys, callback)

    def _check_shared_attributes(self, shared_keys=None, client_keys=None):
        if shared_keys is None:
            shared_keys = ATTRIBUTES_FOR_REQUEST
        self.tb_client.client.request_attributes(callback=self._attributes_parse, shared_keys=shared_keys, client_keys=client_keys)

    def __register_connector(self, session_id, connector_key):
        if (self.__grpc_connectors.get(connector_key) is not None
                and self.__grpc_connectors[connector_key]['id'] not in self.available_connectors_by_id):
            target_connector = self.__grpc_connectors.get(connector_key)
            connector = GrpcConnector(self, target_connector['config'], self.__grpc_manager, session_id)
            connector.setName(target_connector['name'])
            self.available_connectors_by_name[connector.get_name()] = connector
            self.available_connectors_by_id[connector.get_id()] = connector
            self.__grpc_manager.registration_finished(Status.SUCCESS, session_id, target_connector)
            log.info("[%r][%r] GRPC connector with key %s registered with name %s", session_id,
                     connector.get_id(), connector_key, connector.get_name())
        elif self.__grpc_connectors.get(connector_key) is not None:
            self.__grpc_manager.registration_finished(Status.FAILURE, session_id, None)
            log.error("[%r] GRPC connector with key: %s - already registered!", session_id, connector_key)
        else:
            self.__grpc_manager.registration_finished(Status.NOT_FOUND, session_id, None)
            log.error("[%r] GRPC configuration for connector with key: %s - not found", session_id,
                      connector_key)

    def __unregister_connector(self, session_id, connector_key):
        if (self.__grpc_connectors.get(connector_key) is not None
                and self.__grpc_connectors[connector_key]['id'] in self.available_connectors_by_id):
            connector_id = self.__grpc_connectors[connector_key]['id']
            target_connector: GrpcConnector = self.available_connectors_by_id.pop(connector_id)
            self.__grpc_manager.unregister(Status.SUCCESS, session_id, target_connector)
            log.info("[%r] GRPC connector with key %s and name %s - unregistered", session_id, connector_key,
                     target_connector.get_name())
        elif self.__grpc_connectors.get(connector_key) is not None:
            self.__grpc_manager.unregister(Status.NOT_FOUND, session_id, None)
            log.error("[%r] GRPC connector with key: %s - is not registered!", session_id, connector_key)
        else:
            self.__grpc_manager.unregister(Status.FAILURE, session_id, None)
            log.error("[%r] GRPC configuration for connector with key: %s - not found and not registered",
                session_id, connector_key)

    @staticmethod
    def _generate_persistent_key(connector, connectors_persistent_keys):
        if connectors_persistent_keys and connectors_persistent_keys.get(connector['name']) is not None:
            connector_persistent_key = connectors_persistent_keys[connector['name']]
        else:
            connector_persistent_key = "".join(choice(hexdigits) for _ in range(10))
            connectors_persistent_keys[connector['name']] = connector_persistent_key

        return connector_persistent_key

    def load_connectors(self, config=None):
        self._load_connectors(config=config)

    def _load_connectors(self, config=None):
        global log
        self.connectors_configs = {}
        connectors_persistent_keys = self.__load_persistent_connector_keys()

        if config:
            configuration = config.get('connectors')
        else:
            configuration = self.__config.get('connectors')

        if configuration:
            for connector in configuration:
                try:
                    connector_persistent_key = None
                    connector_type = connector["type"].lower() if connector.get("type") is not None else None

                    # can be removed in future releases
                    if connector_type == 'opcua':
                        connector_type = 'opcua_asyncio'

                    if connector_type is None:
                        log.error("Connector type is not defined!")
                        continue
                    if connector_type == "grpc" and self.__grpc_manager is None:
                        log.error("Cannot load connector with name: %s and type grpc. GRPC server is disabled!",
                                  connector['name'])
                        continue

                    if connector_type != "grpc":
                        connector_class = None
                        if connector.get('useGRPC', False):
                            module_name = f'Grpc{self._default_connectors.get(connector_type, connector.get("class"))}'
                            connector_class = TBModuleLoader.import_module(connector_type, module_name)

                        if self.__grpc_manager and self.__grpc_manager.is_alive() and connector_class:
                            connector_persistent_key = self._generate_persistent_key(connector,
                                                                                     connectors_persistent_keys)
                        else:
                            connector_class = TBModuleLoader.import_module(connector_type,
                                                                           self._default_connectors.get(
                                                                               connector_type,
                                                                               connector.get('class')))

                        if connector_class is None:
                            log.warning("Connector implementation not found for %s", connector['name'])
                        else:
                            self._implemented_connectors[connector_type] = connector_class
                    elif connector_type == "grpc":
                        if connector.get('key') == "auto":
                            self._generate_persistent_key(connector, connectors_persistent_keys)
                        else:
                            connector_persistent_key = connector['key']
                        log.info("Connector key for GRPC connector with name [%s] is: [%s]", connector['name'],
                                 connector_persistent_key)
                    config_file_path = self._config_dir + connector['configuration']
                    connector_conf_file_data = ''
                    if not path.exists(config_file_path):
                        log.error("Configuration file for connector with name: %s not found!", connector['name'])
                        continue
                    with open(config_file_path, 'r', encoding="UTF-8") as conf_file:
                        connector_conf_file_data = conf_file.read()

                    connector_conf = connector_conf_file_data
                    try:
                        connector_conf = loads(connector_conf_file_data)
                    except JSONDecodeError as e:
                        log.debug(e)
                        log.warning("Cannot parse connector configuration as a JSON, it will be passed as a string.")

                    connector_id = TBUtility.get_or_create_connector_id(connector_conf)

                    if isinstance(connector_conf, dict):
                        if connector_conf.get('id') is None:
                            connector_conf['id'] = connector_id
                            with open(config_file_path, 'w', encoding="UTF-8") as conf_file:
                                conf_file.write(dumps(connector_conf, indent=2))
                    elif isinstance(connector_conf, str) and not connector_conf:
                        raise ValueError("Connector configuration is empty!")
                    elif isinstance(connector_conf, str):
                        start_find = connector_conf.find("{id_var_start}")
                        end_find = connector_conf.find("{id_var_end}")
                        if not (start_find > -1 and end_find > -1):
                            connector_conf = "{id_var_start}" + str(connector_id) + "{id_var_end}" + connector_conf

                    if not self.connectors_configs.get(connector_type):
                        self.connectors_configs[connector_type] = []
                    if connector_type != 'grpc' and isinstance(connector_conf, dict):
                        connector_conf["name"] = connector['name']
                    connector_configuration = {connector['configuration']: connector_conf} if connector_type != 'grpc' else connector_conf
                    self.connectors_configs[connector_type].append({"name": connector['name'],
                                                                    "id": connector_id,
                                                                    "config": connector_configuration,
                                                                    "config_updated": stat(config_file_path),
                                                                    "config_file_path": config_file_path,
                                                                    "grpc_key": connector_persistent_key})
                except Exception as e:
                    log.exception("Error on loading connector: %r", e)
            if connectors_persistent_keys:
                self.__save_persistent_keys(connectors_persistent_keys)
        else:
            log.warning("Connectors - not found!")
            self.__init_remote_configuration(force=True)

    def connect_with_connectors(self):
        self.__connect_with_connectors()

    def __connect_with_connectors(self):
        global log
        for connector_type in self.connectors_configs:
            connector_type = connector_type.lower()
            for connector_config in self.connectors_configs[connector_type]:
                if self._implemented_connectors.get(connector_type) is not None:
                    if connector_type != 'grpc' and 'Grpc' not in self._implemented_connectors[connector_type].__name__:
                        for config in connector_config["config"]:
                            connector = None
                            connector_name = None
                            connector_id = None
                            try:
                                if connector_config["config"][config] is not None:
                                    if ("logLevel" in connector_config["config"][config]
                                        and len(connector_config["config"][config].keys()) > 3) or \
                                            ("logLevel" not in connector_config["config"][config]
                                             and len(connector_config["config"][config].keys()) >= 1):
                                        connector_name = connector_config["name"]
                                        connector_id = connector_config["id"]

                                        available_connector = self.available_connectors_by_id.get(connector_id)

                                        if available_connector is None or available_connector.is_stopped():
                                            connector = self._implemented_connectors[connector_type](self,
                                                                                                     connector_config["config"][config],
                                                                                                     connector_type)
                                            connector.name = connector_name
                                            self.available_connectors_by_id[connector_id] = connector
                                            self.available_connectors_by_name[connector_name] = connector
                                            connector.open()
                                        else:
                                            log.warning("[%r] Connector with name %s already exists and not stopped!",
                                                        connector_id, connector_name)
                                    else:
                                        log.warning("Config incorrect for %s", connector_type)
                                else:
                                    log.warning("Config is empty for %s", connector_type)
                            except Exception as e:
                                log.error("[%r] Error on loading connector %r: %r", connector_id, connector_name, e)
                                log.exception(e, attr_name=connector_name)
                                if connector is not None:
                                    connector.close()
                    else:
                        self.__grpc_connectors.update({connector_config['grpc_key']: connector_config})
                        if connector_type != 'grpc':
                            connector_dir_abs = "/".join(self._config_dir.split("/")[:-2])
                            connector_file_name = f'{connector_type}_connector.py'
                            connector_abs_path = f'{connector_dir_abs}/grpc_connectors/{connector_type}/{connector_file_name}'
                            connector_config_json = dumps({
                                **connector_config,
                                'gateway': {
                                    'host': 'localhost',
                                    'port': self.__config['grpc']['serverPort']
                                }
                            })

                            thread = Thread(target=self._run_connector,
                                            args=(connector_abs_path, connector_config_json,),
                                            daemon=True, name='Separated GRPC Connector')
                            thread.start()

    def _run_connector(self, connector_abs_path, connector_config_json):
        subprocess.run(['python3', connector_abs_path, connector_config_json, self._config_dir],
                       check=True,
                       universal_newlines=True)

    def check_connector_configuration_updates(self):
        configuration_changed = False
        for connector_type in self.connectors_configs:
            for connector_config in self.connectors_configs[connector_type]:
                if stat(connector_config["config_file_path"]) != connector_config["config_updated"]:
                    configuration_changed = True
                    break
            if configuration_changed:
                break
        if configuration_changed:
            self.__close_connectors()
            self._load_connectors()
            self.__connect_with_connectors()

            # Updating global self.__config['connectors'] configuration for states syncing
            for connector_type in self.connectors_configs:
                for connector_config in self.connectors_configs[connector_type]:
                    for (index, connector) in enumerate(self.__config['connectors']):
                        if connector_config['config'].get(connector['configuration']):
                            self.__config['connectors'][index]['configurationJson'] = connector_config['config'][
                                connector['configuration']]

            if self.__remote_configurator is not None:
                self.__remote_configurator.send_current_configuration()

    def send_to_storage(self, connector_name, connector_id, data=None):
        if data is None:
            log.error("[%r]Data is empty from connector %r!", connector_id, connector_name)
        try:
            device_valid = True
            if self.__device_filter:
                device_valid = self.__device_filter.validate_device(connector_name, data)

            if not device_valid:
                log.warning('Device %s forbidden', data['deviceName'])
                return Status.FORBIDDEN_DEVICE

            filtered_data = self.__duplicate_detector.filter_data(connector_name, data)
            if filtered_data:
                self.__converted_data_queue.put((connector_name, connector_id, filtered_data), True, 100)
                return Status.SUCCESS
            else:
                return Status.NO_NEW_DATA
        except Exception as e:
            log.exception("Cannot put converted data!", exc_info=e)
            return Status.FAILURE

    def __send_to_storage(self):
        while not self.stopped:
            try:
                if not self.__converted_data_queue.empty():
                    connector_name, connector_id, event = self.__converted_data_queue.get(True, 100)
                    data_array = event if isinstance(event, list) else [event]
                    for data in data_array:
                        if not connector_name == self.name:
                            if 'telemetry' not in data:
                                data['telemetry'] = []
                            if 'attributes' not in data:
                                data['attributes'] = []
                            if not TBUtility.validate_converted_data(data):
                                log.error("[%r] Data from %s connector is invalid.", connector_id, connector_name)
                                continue
                            if data.get('deviceType') is None:
                                device_name = data['deviceName']
                                if self.__connected_devices.get(device_name) is not None:
                                    data["deviceType"] = self.__connected_devices[device_name]['device_type']
                                elif self.__saved_devices.get(device_name) is not None:
                                    data["deviceType"] = self.__saved_devices[device_name]['device_type']
                                else:
                                    data["deviceType"] = "default"
                            if data["deviceName"] not in self.get_devices() and self.tb_client.is_connected():
                                self.add_device(data["deviceName"],
                                                {CONNECTOR_PARAMETER: self.available_connectors_by_id[connector_id]},
                                                device_type=data["deviceType"])
                            if not self.__connector_incoming_messages.get(connector_id):
                                self.__connector_incoming_messages[connector_id] = 0
                            else:
                                self.__connector_incoming_messages[connector_id] += 1
                        else:
                            data["deviceName"] = "currentThingsBoardGateway"
                            data['deviceType'] = "gateway"

                        if self.__check_devices_idle:
                            self.__connected_devices[data['deviceName']]['last_receiving_data'] = time()

                        data = self.__convert_telemetry_to_ts(data)

                        max_data_size = self.__config["thingsboard"].get("maxPayloadSizeBytes", 400)
                        if self.__get_data_size(data) >= max_data_size:
                            # Data is too large, so we will attempt to send in pieces
                            adopted_data = {"deviceName": data['deviceName'],
                                            "deviceType": data['deviceType'],
                                            "attributes": {},
                                            "telemetry": []}
                            empty_adopted_data_size = self.__get_data_size(adopted_data)
                            adopted_data_size = empty_adopted_data_size

                            # First, loop through the attributes
                            for attribute in data['attributes']:
                                adopted_data['attributes'].update(attribute)
                                adopted_data_size += self.__get_data_size(attribute)
                                if adopted_data_size >= max_data_size:
                                    # We have surpassed the max_data_size, so send what we have and clear attributes
                                    self.__send_data_pack_to_storage(adopted_data, connector_name, connector_id)
                                    adopted_data['attributes'] = {}
                                    adopted_data_size = empty_adopted_data_size

                            # Now, loop through telemetry. Possibly have some unsent attributes that have been adopted.
                            telemetry = data['telemetry'] if isinstance(data['telemetry'], list) else [
                                data['telemetry']]
                            ts_to_index = {}
                            for ts_kv_list in telemetry:
                                ts = ts_kv_list['ts']
                                for kv in ts_kv_list['values']:
                                    if ts in ts_to_index:
                                        kv_data = {kv: ts_kv_list['values'][kv]}
                                        adopted_data['telemetry'][ts_to_index[ts]]['values'].update(kv_data)
                                    else:
                                        kv_data = {'ts': ts, 'values': {kv: ts_kv_list['values'][kv]}}
                                        adopted_data['telemetry'].append(kv_data)
                                        ts_to_index[ts] = len(adopted_data['telemetry']) - 1

                                    adopted_data_size += self.__get_data_size(kv_data)
                                    if adopted_data_size >= max_data_size:
                                        # we have surpassed the max_data_size, so send what we have and clear attributes and telemetry
                                        self.__send_data_pack_to_storage(adopted_data, connector_name, connector_id)
                                        adopted_data['telemetry'] = []
                                        adopted_data['attributes'] = {}
                                        adopted_data_size = empty_adopted_data_size
                                        ts_to_index = {}

                            # It is possible that we get here and have some telemetry or attributes not yet sent, so check for that.
                            if len(adopted_data['telemetry']) > 0 or len(adopted_data['attributes']) > 0:
                                self.__send_data_pack_to_storage(adopted_data, connector_name, connector_id)
                                # technically unnecessary to clear here, but leaving for consistency.
                                adopted_data['telemetry'] = []
                                adopted_data['attributes'] = {}
                        else:
                            self.__send_data_pack_to_storage(data, connector_name, connector_id)

                else:
                    sleep(0.2)
            except Exception as e:
                log.error(e)

    @staticmethod
    def __get_data_size(data: dict):
        return getsizeof(str(data))

    @staticmethod
    def get_data_size(data):
        return TBGatewayService.__get_data_size(data)

    @staticmethod
    def __convert_telemetry_to_ts(data):
        telemetry = {}
        telemetry_with_ts = []
        for item in data["telemetry"]:
            if item.get("ts") is None:
                telemetry = {**telemetry, **item}
            else:
                if isinstance(item['ts'], int):
                    telemetry_with_ts.append({"ts": item["ts"], "values": {**item["values"]}})
                else:
                    log.warning('Data has invalid TS (timestamp) format! Using generated TS instead.')
                    telemetry_with_ts.append({"ts": int(time() * 1000), "values": {**item["values"]}})

        if telemetry_with_ts:
            data["telemetry"] = telemetry_with_ts
        elif len(data['telemetry']) > 0:
            data["telemetry"] = {"ts": int(time() * 1000), "values": telemetry}
        return data

    def __send_data_pack_to_storage(self, data, connector_name, connector_id=None):
        json_data = dumps(data)
        save_result = self._event_storage.put(json_data)
        if not save_result:
            log.error('%rData from the device "%s" cannot be saved, connector name is %s.',
                      "[" + connector_id + "] " if connector_id is not None else "",
                      data["deviceName"], connector_name)

    def check_size(self, devices_data_in_event_pack):
        if (self.__get_data_size(devices_data_in_event_pack)
                >= self.__config["thingsboard"].get("maxPayloadSizeBytes", 400)):
            self.__send_data(devices_data_in_event_pack)
            for device in devices_data_in_event_pack:
                devices_data_in_event_pack[device]["telemetry"] = []
                devices_data_in_event_pack[device]["attributes"] = {}

    def __read_data_from_storage(self):
        devices_data_in_event_pack = {}
        log.debug("Send data Thread has been started successfully.")
        log.debug("Maximal size of the client message queue is: %r",
                  self.tb_client.client._client._max_queued_messages)

        while not self.stopped:
            try:
                if self.tb_client.is_connected():
                    events = []

                    if self.__remote_configurator is None or not self.__remote_configurator.in_process:
                        events = self._event_storage.get_event_pack()

                    if events:
                        for event in events:
                            try:
                                current_event = loads(event)
                            except Exception as e:
                                log.exception(e)
                                continue

                            if not devices_data_in_event_pack.get(current_event["deviceName"]):
                                devices_data_in_event_pack[current_event["deviceName"]] = {"telemetry": [],
                                                                                           "attributes": {}}
                            if current_event.get("telemetry"):
                                if isinstance(current_event["telemetry"], list):
                                    for item in current_event["telemetry"]:
                                        self.check_size(devices_data_in_event_pack)
                                        devices_data_in_event_pack[current_event["deviceName"]]["telemetry"].append(item)
                                else:
                                    self.check_size(devices_data_in_event_pack)
                                    devices_data_in_event_pack[current_event["deviceName"]]["telemetry"].append(current_event["telemetry"])
                            if current_event.get("attributes"):
                                if isinstance(current_event["attributes"], list):
                                    for item in current_event["attributes"]:
                                        self.check_size(devices_data_in_event_pack)
                                        devices_data_in_event_pack[current_event["deviceName"]]["attributes"].update(item.items())
                                else:
                                    self.check_size(devices_data_in_event_pack)
                                    devices_data_in_event_pack[current_event["deviceName"]]["attributes"].update(current_event["attributes"].items())
                        if devices_data_in_event_pack:
                            if not self.tb_client.is_connected():
                                continue
                            while self.__rpc_reply_sent:
                                sleep(.01)
                            self.__send_data(devices_data_in_event_pack)

                        if self.tb_client.is_connected() and (
                                self.__remote_configurator is None or not self.__remote_configurator.in_process):
                            success = True
                            while not self._published_events.empty():
                                if (self.__remote_configurator is not None and self.__remote_configurator.in_process) or \
                                        not self.tb_client.is_connected() or \
                                        self._published_events.empty() or \
                                        self.__rpc_reply_sent:
                                    success = False
                                    break

                                events = [self._published_events.get(False, 10) for _ in
                                          range(min(self.__min_pack_size_to_send, self._published_events.qsize()))]
                                for event in events:
                                    try:
                                        if (self.tb_client.is_connected()
                                                and (self.__remote_configurator is None
                                                or not self.__remote_configurator.in_process)):
                                            if self.tb_client.client.quality_of_service == 1:
                                                success = event.get() == event.TB_ERR_SUCCESS
                                            else:
                                                success = True
                                        else:
                                            break
                                    except Exception as e:
                                        log.exception(e)
                                        success = False
                            if success and self.tb_client.is_connected():
                                self._event_storage.event_pack_processing_done()
                                del devices_data_in_event_pack
                                devices_data_in_event_pack = {}
                        else:
                            continue
                    else:
                        sleep(self.__min_pack_send_delay_ms)
                else:
                    sleep(1)
            except Exception as e:
                log.exception(e)
                sleep(1)

    @StatisticsService.CollectAllSentTBBytesStatistics(start_stat_type='allBytesSentToTB')
    def __send_data(self, devices_data_in_event_pack):
        try:
            for device in devices_data_in_event_pack:
                final_device_name = device if self.__renamed_devices.get(device) is None else self.__renamed_devices[
                    device]

                if devices_data_in_event_pack[device].get("attributes"):
                    if device == self.name or device == "currentThingsBoardGateway":
                        self._published_events.put(
                            self.tb_client.client.send_attributes(devices_data_in_event_pack[device]["attributes"]))
                    else:
                        self._published_events.put(self.tb_client.client.gw_send_attributes(final_device_name,
                                                                                            devices_data_in_event_pack[
                                                                                                device]["attributes"]))
                if devices_data_in_event_pack[device].get("telemetry"):
                    if device == self.name or device == "currentThingsBoardGateway":
                        self._published_events.put(
                            self.tb_client.client.send_telemetry(devices_data_in_event_pack[device]["telemetry"]))
                    else:
                        self._published_events.put(self.tb_client.client.gw_send_telemetry(final_device_name,
                                                                                           devices_data_in_event_pack[
                                                                                               device]["telemetry"]))
                devices_data_in_event_pack[device] = {"telemetry": [], "attributes": {}}
        except Exception as e:
            log.exception(e)

    def _rpc_request_handler(self, request_id, content):
        try:
            device = content.get("device")
            if device is not None:
                connector = self.get_devices()[device].get(CONNECTOR_PARAMETER)
                if connector is not None:
                    content['id'] = request_id
                    connector.server_side_rpc_handler(content)
                else:
                    log.error("Received RPC request but connector for the device %s not found. Request data: \n %s",
                              content["device"],
                              dumps(content))
            else:
                try:
                    method_split = content["method"].split('_')
                    module = None
                    if len(method_split) > 0:
                        module = method_split[0]
                    if module is not None:
                        result = None
                        if self.connectors_configs.get(module):
                            log.debug("Connector \"%s\" for RPC request \"%s\" found", module, content["method"])
                            for connector_name in self.available_connectors_by_name:
                                if self.available_connectors_by_name[connector_name]._connector_type == module:
                                    log.debug("Sending command RPC %s to connector %s", content["method"], connector_name)
                                    content['id'] = request_id
                                    result = self.available_connectors_by_name[connector_name].server_side_rpc_handler(content)
                        elif module == 'gateway' or module in self.__remote_shell.shell_commands:
                            result = self.__rpc_gateway_processing(request_id, content)
                        else:
                            log.error("Connector \"%s\" not found", module)
                            result = {"error": "%s - connector not found in available connectors." % module,
                                      "code": 404}
                        if result is None:
                            self.send_rpc_reply(None, request_id, success_sent=False)
                        elif "qos" in result:
                            self.send_rpc_reply(None, request_id,
                                                dumps({k: v for k, v in result.items() if k != "qos"}),
                                                quality_of_service=result["qos"])
                        else:
                            self.send_rpc_reply(None, request_id, dumps(result))
                except Exception as e:
                    self.send_rpc_reply(None, request_id, "{\"error\":\"%s\", \"code\": 500}" % str(e))
                    log.exception(e)
        except Exception as e:
            log.exception(e)

    def __rpc_gateway_processing(self, request_id, content):
        log.info("Received RPC request to the gateway, id: %s, method: %s", str(request_id), content["method"])
        arguments = content.get('params', {})
        method_to_call = content["method"].replace("gateway_", "")
        result = None
        if self.__remote_shell is not None:
            method_function = self.__remote_shell.shell_commands.get(method_to_call, self.__gateway_rpc_methods.get(method_to_call))
        else:
            log.info("Remote shell is disabled.")
            method_function = self.__gateway_rpc_methods.get(method_to_call)
        if method_function is None and method_to_call in self.__rpc_scheduled_methods_functions:
            seconds_to_restart = arguments * 1000 if arguments and arguments != '{}' else 0
            self.__scheduled_rpc_calls.append([time() * 1000 + seconds_to_restart, self.__rpc_scheduled_methods_functions[method_to_call]])
            log.info("Gateway %s scheduled in %i seconds", method_to_call, seconds_to_restart / 1000)
            result = {"success": True}
        elif method_function is None:
            log.error("RPC method %s - Not found", content["method"])
            return {"error": "Method not found", "code": 404}
        elif isinstance(arguments, list):
            result = method_function(*arguments)
        elif arguments:
            result = method_function(arguments)
        else:
            result = method_function()
        return result

    @staticmethod
    def __rpc_ping(*args):
        return {"code": 200, "resp": "pong"}

    def __rpc_devices(self, *args):
        data_to_send = {}
        for device in self.__connected_devices:
            if self.__connected_devices[device][CONNECTOR_PARAMETER] is not None:
                data_to_send[device] = self.__connected_devices[device][CONNECTOR_PARAMETER].get_name()
        return {"code": 200, "resp": data_to_send}

    def __rpc_update(self, *args):
        try:
            result = {"resp": self.__updater.update(),
                      "code": 200,
                      }
        except Exception as e:
            result = {"error": str(e),
                      "code": 500
                      }
        return result

    def __rpc_version(self, *args):
        try:
            result = {"resp": self.__updater.get_version(), "code": 200}
        except Exception as e:
            result = {"error": str(e), "code": 500}
        return result

    def is_rpc_in_progress(self, topic):
        return topic in self.__rpc_requests_in_progress

    def rpc_with_reply_processing(self, topic, content):
        req_id = self.__rpc_requests_in_progress[topic][0]["data"]["id"]
        device = self.__rpc_requests_in_progress[topic][0]["device"]
        log.info("Outgoing RPC. Device: %s, ID: %d", device, req_id)
        self.send_rpc_reply(device, req_id, content)

    @StatisticsService.CollectRPCReplyStatistics(start_stat_type='allBytesSentToTB')
    def send_rpc_reply(self, device=None, req_id=None, content=None, success_sent=None, wait_for_publish=None,
                       quality_of_service=0):
        self.__rpc_processing_queue.put((device, req_id, content, success_sent, wait_for_publish, quality_of_service))

    def __send_rpc_reply_processing(self):
        while not self.stopped:
            if not self.__rpc_processing_queue.empty():
                args = self.__rpc_processing_queue.get()
                self.__send_rpc_reply(*args)
            else:
                sleep(.1)

    def __send_rpc_reply(self, device=None, req_id=None, content=None, success_sent=None, wait_for_publish=None,
                         quality_of_service=0):
        try:
            self.__rpc_reply_sent = True
            rpc_response = {"success": False}
            if success_sent is not None:
                if success_sent:
                    rpc_response["success"] = True
            if device is not None and success_sent is not None:
                self.tb_client.client.gw_send_rpc_reply(device, req_id, dumps(rpc_response), quality_of_service=quality_of_service)
            elif device is not None and req_id is not None and content is not None:
                self.tb_client.client.gw_send_rpc_reply(device, req_id, content, quality_of_service=quality_of_service)
            elif device is None and success_sent is not None:
                self.tb_client.client.send_rpc_reply(req_id, dumps(rpc_response), quality_of_service=quality_of_service,
                                                     wait_for_publish=wait_for_publish)
            elif device is None and content is not None:
                self.tb_client.client.send_rpc_reply(req_id, content, quality_of_service=quality_of_service,
                                                     wait_for_publish=wait_for_publish)
            self.__rpc_reply_sent = False
        except Exception as e:
            log.exception(e)

    def register_rpc_request_timeout(self, content, timeout, topic, cancel_method):
        # Put request in outgoing RPC queue. It will be eventually dispatched.
        self.__rpc_register_queue.put({"topic": topic, "data": (content, timeout, cancel_method)}, False)

    def cancel_rpc_request(self, rpc_request):
        content = self.__rpc_requests_in_progress[rpc_request][0]
        self.send_rpc_reply(device=content["device"], req_id=content["data"]["id"], success_sent=False)

    def _attribute_update_callback(self, content, *args):
        log.debug("Attribute request received with content: \"%s\"", content)
        log.debug(args)
        if content.get('device') is not None:
            try:
                self.__connected_devices[content["device"]][CONNECTOR_PARAMETER].on_attributes_update(content)
            except Exception as e:
                log.exception(e)
        else:
            self._attributes_parse(content)

    def __form_statistics(self):
        summary_messages = {"eventsProduced": 0, "eventsSent": 0}
        for connector in self.available_connectors_by_name:
            connector_camel_case = connector.replace(' ', '')
            telemetry = {
                (connector_camel_case + ' EventsProduced').replace(' ', ''):
                    self.available_connectors_by_name[connector].statistics.get('MessagesReceived', 0),
                (connector_camel_case + ' EventsSent').replace(' ', ''):
                    self.available_connectors_by_name[connector].statistics.get('MessagesSent', 0)
            }
            summary_messages['eventsProduced'] += telemetry[
                str(connector_camel_case + ' EventsProduced').replace(' ', '')]
            summary_messages['eventsSent'] += telemetry[
                str(connector_camel_case + ' EventsSent').replace(' ', '')]
            summary_messages.update(telemetry)
        return summary_messages

    def add_device_async(self, data):
        if data['deviceName'] not in self.__saved_devices:
            self.__async_device_actions_queue.put((DeviceActions.CONNECT, data))
            return Status.SUCCESS
        else:
            return Status.FAILURE

    def add_device(self, device_name, content, device_type=None):
        device_type = device_type if device_type is not None else 'default'
        device_details = None
        if device_name in self.__saved_devices:
            device_details = {
                'connectorType': content['connector'].get_type(),
                'connectorName': content['connector'].get_name()
            }
        self.__connected_devices[device_name] = {**content, DEVICE_TYPE_PARAMETER: device_type}
        self.__saved_devices[device_name] = {**content, DEVICE_TYPE_PARAMETER: device_type}
        self.__save_persistent_devices()
        self.tb_client.client.gw_connect_device(device_name, device_type)
        if device_details is not None:
            self.tb_client.client.gw_send_attributes(device_name, device_details)

    def update_device(self, device_name, event, content):
        should_save = False
        if self.__connected_devices.get(device_name) is None:
            return
        if event == 'connector' and self.__connected_devices[device_name].get(event) != content:
            should_save = True
        self.__connected_devices[device_name][event] = content
        if should_save:
            self.__save_persistent_devices()

    def del_device_async(self, data):
        if data['deviceName'] in self.__saved_devices:
            self.__async_device_actions_queue.put((DeviceActions.DISCONNECT, data))
            return Status.SUCCESS
        else:
            return Status.FAILURE

    def del_device(self, device_name):
        if device_name in self.__connected_devices:
            self.tb_client.client.gw_disconnect_device(device_name)
            self.__connected_devices.pop(device_name)
            self.__saved_devices.pop(device_name)
            self.__save_persistent_devices()

    def get_devices(self, connector_id: str = None):
        return self.__connected_devices if connector_id is None else {
            device_name: self.__connected_devices[device_name][DEVICE_TYPE_PARAMETER] for device_name in
            self.__connected_devices.keys() if
            self.__connected_devices[device_name].get(CONNECTOR_PARAMETER) is not None and
            self.__connected_devices[device_name][CONNECTOR_PARAMETER].get_id() == connector_id}

    def __process_async_device_actions(self):
        while not self.stopped:
            if not self.__async_device_actions_queue.empty():
                action, data = self.__async_device_actions_queue.get()
                if action == DeviceActions.CONNECT:
                    self.add_device(data['deviceName'],
                                    {CONNECTOR_PARAMETER: self.available_connectors_by_name[data['name']]},
                                    data.get('deviceType'))
                elif action == DeviceActions.DISCONNECT:
                    self.del_device(data['deviceName'])
            else:
                sleep(.2)

    def __load_persistent_connector_keys(self):
        persistent_keys = {}
        if PERSISTENT_GRPC_CONNECTORS_KEY_FILENAME in listdir(self._config_dir) and \
                path.getsize(self._config_dir + PERSISTENT_GRPC_CONNECTORS_KEY_FILENAME) > 0:
            try:
                persistent_keys = load_file(self._config_dir + PERSISTENT_GRPC_CONNECTORS_KEY_FILENAME)
            except Exception as e:
                log.exception(e)
            log.debug("Loaded keys: %s", persistent_keys)
        else:
            log.debug("Persistent keys file not found")
        return persistent_keys

    def __save_persistent_keys(self, persistent_keys):
        try:
            with open(self._config_dir + PERSISTENT_GRPC_CONNECTORS_KEY_FILENAME, 'w') as persistent_keys_file:
                persistent_keys_file.write(dumps(persistent_keys, indent=2, sort_keys=True))
        except Exception as e:
            log.exception(e)

    def __load_persistent_devices(self):
        loaded_connected_devices = None
        if CONNECTED_DEVICES_FILENAME in listdir(self._config_dir) and \
                path.getsize(self._config_dir + CONNECTED_DEVICES_FILENAME) > 0:
            try:
                loaded_connected_devices = load_file(self._config_dir + CONNECTED_DEVICES_FILENAME)
            except Exception as e:
                log.exception(e)
        else:
            open(self._config_dir + CONNECTED_DEVICES_FILENAME, 'w').close()

        if loaded_connected_devices is not None:
            log.debug("Loaded devices:\n %s", loaded_connected_devices)
            for device_name in loaded_connected_devices:
                try:
                    if isinstance(loaded_connected_devices[device_name], str):
                        open(self._config_dir + CONNECTED_DEVICES_FILENAME, 'w').close()
                        log.debug("Old connected_devices file, new file will be created")
                        return
                    device_data_to_save = {}
                    if isinstance(loaded_connected_devices[device_name], list) \
                            and self.available_connectors_by_name.get(loaded_connected_devices[device_name][0]):
                        device_data_to_save = {
                            CONNECTOR_PARAMETER: self.available_connectors_by_name[loaded_connected_devices[device_name][0]],
                            DEVICE_TYPE_PARAMETER: loaded_connected_devices[device_name][1]}
                        if len(loaded_connected_devices[device_name]) > 2 and device_name not in self.__renamed_devices:
                            new_device_name = loaded_connected_devices[device_name][2]
                            self.__renamed_devices[device_name] = new_device_name
                    elif isinstance(loaded_connected_devices[device_name], dict):
                        connector = None
                        if not self.available_connectors_by_id.get(
                                loaded_connected_devices[device_name][CONNECTOR_ID_PARAMETER]):
                            log.warning("Connector with id %s not found, trying to use connector by name!",
                                        loaded_connected_devices[device_name][CONNECTOR_ID_PARAMETER])
                            connector = self.available_connectors_by_name.get(
                                loaded_connected_devices[device_name][CONNECTOR_NAME_PARAMETER])
                        else:
                            connector = self.available_connectors_by_id.get(
                                loaded_connected_devices[device_name][CONNECTOR_ID_PARAMETER])
                        if connector is None:
                            log.warning("Connector with name %s not found! probably it is disabled, device %s will be "
                                        "removed from the saved devices",
                                        loaded_connected_devices[device_name][CONNECTOR_NAME_PARAMETER], device_name)
                            continue
                        device_data_to_save = {
                            CONNECTOR_PARAMETER: connector,
                            DEVICE_TYPE_PARAMETER: loaded_connected_devices[device_name][DEVICE_TYPE_PARAMETER]}
                        if loaded_connected_devices[device_name].get(RENAMING_PARAMETER) is not None:
                            new_device_name = loaded_connected_devices[device_name][RENAMING_PARAMETER]
                            self.__renamed_devices[device_name] = new_device_name
                    self.__connected_devices[device_name] = device_data_to_save
                    for device in list(self.__connected_devices.keys()):
                        self.add_device(device, self.__connected_devices[device], self.__connected_devices[device][
                            DEVICE_TYPE_PARAMETER])
                    self.__saved_devices[device_name] = device_data_to_save

                except Exception as e:
                    log.exception(e)
                    continue
        else:
            log.debug("No device found in connected device file.")
            self.__connected_devices = {} if self.__connected_devices is None else self.__connected_devices

    def __save_persistent_devices(self):
        with self.__lock:
            data_to_save = {}
            for device in self.__connected_devices:
                if self.__connected_devices[device][CONNECTOR_PARAMETER] is not None:
                    data_to_save[device] = {
                        CONNECTOR_NAME_PARAMETER: self.__connected_devices[device][CONNECTOR_PARAMETER].get_name(),
                        DEVICE_TYPE_PARAMETER: self.__connected_devices[device][DEVICE_TYPE_PARAMETER],
                        CONNECTOR_ID_PARAMETER: self.__connected_devices[device][CONNECTOR_PARAMETER].get_id(),
                        RENAMING_PARAMETER: self.__renamed_devices.get(device)
                    }
            with open(self._config_dir + CONNECTED_DEVICES_FILENAME, 'w') as config_file:
                try:
                    config_file.write(dumps(data_to_save, indent=2, sort_keys=True))
                except Exception as e:
                    log.exception(e)

            log.debug("Saved connected devices.")

    def __check_devices_idle_time(self):
        check_devices_idle_every_sec = self.__devices_idle_checker.get('inactivityCheckPeriodSeconds', 1)
        disconnect_device_after_idle = self.__devices_idle_checker.get('inactivityTimeoutSeconds', 50)

        while not self.stopped:
            for_deleting = []
            for (device_name, device) in self.__connected_devices.items():
                ts = time()

                if not device.get('last_receiving_data'):
                    device['last_receiving_data'] = ts

                last_receiving_data = device['last_receiving_data']

                if ts - last_receiving_data >= disconnect_device_after_idle:
                    for_deleting.append(device_name)

            for device_name in for_deleting:
                self.del_device(device_name)

                log.debug('Delete device %s for the reason of idle time > %s.',
                          device_name,
                          disconnect_device_after_idle)

            sleep(check_devices_idle_every_sec)

    # GETTERS --------------------
    def ping(self):
        return self.name

    def get_max_payload_size_bytes(self):
        return self.__config["thingsboard"].get("maxPayloadSizeBytes", 400)

    # ----------------------------
    # Storage --------------------
    def get_storage_name(self):
        return self._event_storage.__class__.__name__

    def get_storage_events_count(self):
        return self._event_storage.len()

    # Connectors -----------------
    def get_available_connectors(self):
        return {num + 1: name for (num, name) in enumerate(self.available_connectors_by_name)}

    def get_connector_status(self, name):
        try:
            connector = self.available_connectors_by_name[name]
            return {'connected': connector.is_connected()}
        except KeyError:
            return f'Connector {name} not found!'

    def get_connector_config(self, name):
        try:
            connector = self.available_connectors_by_name[name]
            return connector.get_config()
        except KeyError:
            return f'Connector {name} not found!'

    # Gateway ----------------------
    def get_status(self):
        return {'connected': self.tb_client.is_connected()}


if __name__ == '__main__':
    TBGatewayService(
        path.dirname(path.dirname(path.abspath(__file__))) + '/config/tb_gateway.yaml'.replace('/', path.sep))
