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

import logging
import logging.config
import logging.handlers
from copy import deepcopy
from os import execv, listdir, path, pathsep, stat, system
from queue import SimpleQueue
from random import choice
from string import ascii_lowercase, hexdigits
from sys import argv, executable, getsizeof
from threading import RLock, Thread
from time import sleep, time

from simplejson import JSONDecodeError, dumps, load, loads
from yaml import safe_load

from thingsboard_gateway.gateway.constant_enums import DeviceActions
from thingsboard_gateway.gateway.constants import CONNECTED_DEVICES_FILENAME, CONNECTOR_PARAMETER, PERSISTENT_GRPC_CONNECTORS_KEY_FILENAME
from thingsboard_gateway.gateway.grpc_service.grpc_connector import GrpcConnector
from thingsboard_gateway.gateway.grpc_service.tb_grpc_manager import Status, TBGRPCServerManager
from thingsboard_gateway.gateway.tb_client import TBClient
from thingsboard_gateway.storage.file.file_event_storage import FileEventStorage
from thingsboard_gateway.storage.memory.memory_event_storage import MemoryEventStorage
from thingsboard_gateway.storage.sqlite.sqlite_event_storage import SQLiteEventStorage
from thingsboard_gateway.tb_utility.tb_gateway_remote_configurator import RemoteConfigurator
from thingsboard_gateway.tb_utility.tb_loader import TBModuleLoader
from thingsboard_gateway.tb_utility.tb_logger import TBLoggerHandler
from thingsboard_gateway.tb_utility.tb_remote_shell import RemoteShell
from thingsboard_gateway.tb_utility.tb_updater import TBUpdater
from thingsboard_gateway.tb_utility.tb_utility import TBUtility

log = logging.getLogger('service')
main_handler = logging.handlers.MemoryHandler(-1)

DEFAULT_CONNECTORS = {
    "mqtt": "MqttConnector",
    "modbus": "ModbusConnector",
    "opcua": "OpcUaConnector",
    "ble": "BLEConnector",
    "request": "RequestConnector",
    "can": "CanConnector",
    "bacnet": "BACnetConnector",
    "odbc": "OdbcConnector",
    "rest": "RESTConnector",
    "snmp": "SNMPConnector",
    "ftp": "FTPConnector"
    }


def load_file(path_to_file):
    content = None
    with open(path_to_file, 'r') as target_file:
        content = load(target_file)
    return content


class TBGatewayService:
    def __init__(self, config_file=None):
        self.stopped = False
        self.__lock = RLock()
        self.async_device_actions = {
            DeviceActions.CONNECT: self.add_device,
            DeviceActions.DISCONNECT: self.del_device
            }
        self.__async_device_actions_queue = SimpleQueue()
        self.__process_async_actions_thread = Thread(target=self.__process_async_device_actions, name="Async device actions processing thread", daemon=True)
        if config_file is None:
            config_file = path.dirname(path.dirname(path.abspath(__file__))) + '/config/tb_gateway.yaml'.replace('/',
                                                                                                                 path.sep)
        with open(config_file) as general_config:
            self.__config = safe_load(general_config)
        self._config_dir = path.dirname(path.abspath(config_file)) + path.sep
        logging_error = None
        try:
            logging.config.fileConfig(self._config_dir + "logs.conf", disable_existing_loggers=False)
        except Exception as e:
            logging_error = e
        global log
        log = logging.getLogger('service')
        log.info("Gateway starting...")
        self.__updater = TBUpdater()
        self.__updates_check_period_ms = 300000
        self.__updates_check_time = 0
        self.version = self.__updater.get_version()
        log.info("ThingsBoard IoT gateway version: %s", self.version["current_version"])
        self.available_connectors = {}
        self.__connector_incoming_messages = {}
        self.__connected_devices = {}
        self.__renamed_devices = {}
        self.__saved_devices = {}
        self.__events = []
        self.name = ''.join(choice(ascii_lowercase) for _ in range(64))
        self.__rpc_register_queue = SimpleQueue()
        self.__rpc_requests_in_progress = {}
        self.tb_client = TBClient(self.__config["thingsboard"], self._config_dir)
        try:
            self.tb_client.disconnect()
        except Exception as e:
            log.exception(e)
        self.tb_client.connect()
        self.subscribe_to_required_topics()
        self.__subscribed_to_rpc_topics = True
        if logging_error is not None:
            self.tb_client.client.send_telemetry({"ts": time() * 1000, "values": {
                "LOGS": "Logging loading exception, logs.conf is wrong: %s" % (str(logging_error),)}})
            TBLoggerHandler.set_default_handler()
        self.counter = 0
        self.__rpc_reply_sent = False
        global main_handler
        self.main_handler = main_handler
        self.remote_handler = TBLoggerHandler(self)
        self.main_handler.setTarget(self.remote_handler)
        self._default_connectors = DEFAULT_CONNECTORS
        self.__converted_data_queue = SimpleQueue()
        self.__save_converted_data_thread = Thread(name="Save converted data", daemon=True, target=self.__send_to_storage)
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
        self.__remote_shell = None
        if self.__config["thingsboard"].get("remoteShell"):
            log.warning("Remote shell is enabled. Please be carefully with this feature.")
            self.__remote_shell = RemoteShell(platform=self.__updater.get_platform(),
                                              release=self.__updater.get_release())
        self.__rpc_remote_shell_command_in_progress = None
        self.__scheduled_rpc_calls = []
        self.__rpc_processing_queue = SimpleQueue()
        self.__rpc_scheduled_methods_functions = {
            "restart": {"function": execv, "arguments": (executable, [executable.split(pathsep)[-1]] + argv)},
            "reboot": {"function": system, "arguments": ("reboot 0",)},
            }
        self.__rpc_processing_thread = Thread(target=self.__send_rpc_reply_processing, daemon=True, name="RPC processing thread")
        self.__rpc_processing_thread.start()
        self._event_storage = self._event_storage_types[self.__config["storage"]["type"]](self.__config["storage"])
        self.connectors_configs = {}
        self.__remote_configurator = None
        self.__request_config_after_connect = False
        self.__load_persistent_devices()
        self.__init_remote_configuration()
        self.__grpc_config = self.__config.get('grpc')
        self.__grpc_manager = None
        self.__grpc_connectors = {}
        if self.__grpc_config is not None and self.__grpc_config.get("enabled"):
            self.__process_async_actions_thread.start()
            self.__grpc_manager = TBGRPCServerManager(self, self.__grpc_config)
            self.__grpc_manager.set_gateway_read_callbacks(self.__register_connector, self.__unregister_connector)
        self._load_connectors()
        self._connect_with_connectors()
        self.__load_persistent_devices()
        self._published_events = SimpleQueue()
        self._send_thread = Thread(target=self.__read_data_from_storage, daemon=True,
                                   name="Send data to Thingsboard Thread")
        self._send_thread.start()
        self.__min_pack_send_delay_ms = self.__config['thingsboard'].get('minPackSendDelayMS', 500) / 1000.0
        log.info("Gateway started.")

        try:
            gateway_statistic_send = 0
            connectors_configuration_check_time = 0
            while not self.stopped:
                cur_time = time() * 1000
                if not self.tb_client.is_connected() and self.__subscribed_to_rpc_topics:
                    self.__subscribed_to_rpc_topics = False
                if self.tb_client.is_connected() and not self.__subscribed_to_rpc_topics:
                    for device in self.__saved_devices:
                        self.add_device(device, {"connector": self.__saved_devices[device]["connector"]},
                                        device_type=self.__saved_devices[device]["device_type"])
                    self.subscribe_to_required_topics()
                    self.__subscribed_to_rpc_topics = True
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
                if (self.__rpc_requests_in_progress or not self.__rpc_register_queue.empty()) and self.tb_client.is_connected():
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
                if not self.__request_config_after_connect and self.tb_client.is_connected() and not self.tb_client.client.get_subscriptions_in_progress():
                    self.__request_config_after_connect = True
                    self.__check_shared_attributes()

                if cur_time - gateway_statistic_send > self.__config["thingsboard"].get("statsSendPeriodInSeconds",
                                                                                        3600) * 1000 and self.tb_client.is_connected():
                    summary_messages = self.__form_statistics()
                    # with self.__lock:
                    self.tb_client.client.send_telemetry(summary_messages)
                    gateway_statistic_send = time() * 1000
                    # self.__check_shared_attributes()

                if cur_time - connectors_configuration_check_time > self.__config["thingsboard"].get(
                        "checkConnectorsConfigurationInSeconds", 60) * 1000:
                    self.check_connector_configuration_updates()
                    connectors_configuration_check_time = time() * 1000

                if cur_time - self.__updates_check_time >= self.__updates_check_period_ms:
                    self.__updates_check_time = time() * 1000
                    self.version = self.__updater.get_version()

        except KeyboardInterrupt:
            self.__stop_gateway()
        except Exception as e:
            log.exception(e)
            self.__stop_gateway()
            self.__close_connectors()
            log.info("The gateway has been stopped.")
            self.tb_client.stop()

    def __close_connectors(self):
        for current_connector in self.available_connectors:
            try:
                self.available_connectors[current_connector].close()
                log.debug("Connector %s closed connection.", current_connector)
            except Exception as e:
                log.exception(e)

    def __stop_gateway(self):
        self.stopped = True
        self.__updater.stop()
        log.info("Stopping...")
        self.__grpc_manager.stop()
        self.__close_connectors()
        self._event_storage.stop()
        log.info("The gateway has been stopped.")
        self.tb_client.disconnect()
        self.tb_client.stop()

    def __init_remote_configuration(self, force=False):
        if (self.__config["thingsboard"].get("remoteConfiguration") or force) and self.__remote_configurator is None:
            try:
                self.__remote_configurator = RemoteConfigurator(self, self.__config)
                if self.tb_client.is_connected() and not self.tb_client.client.get_subscriptions_in_progress():
                    self.__check_shared_attributes()
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
        self.__process_remote_configuration(content.get("configuration"))

    def __process_attributes_response(self, shared_attributes, client_attributes):
        self.__process_remote_logging_update(shared_attributes.get('RemoteLoggingLevel'))
        self.__process_remote_configuration(shared_attributes.get("configuration"))

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

    def __process_deleted_gateway_devices(self, deleted_device_name: str):
        log.info("Received deleted gateway device notification: %s", deleted_device_name)
        if deleted_device_name in list(self.__renamed_devices.values()):
            first_device_name = TBUtility.get_dict_key_by_value(self.__renamed_devices, deleted_device_name)
            del self.__renamed_devices[first_device_name]
            deleted_device_name = first_device_name
            log.debug("Current renamed_devices dict: %s", self.__renamed_devices)
        if deleted_device_name in self.__connected_devices:
            del self.__connected_devices[deleted_device_name]
            log.debug("Device %s - was removed", deleted_device_name)
        self.__save_persistent_devices()
        self.__load_persistent_devices()

    def __process_renamed_gateway_devices(self, renamed_device: dict):
        log.info("Received renamed gateway device notification: %s", renamed_device)
        old_device_name, new_device_name = renamed_device.popitem()
        if old_device_name in list(self.__renamed_devices.values()):
            device_name_key = TBUtility.get_dict_key_by_value(self.__renamed_devices, old_device_name)
        else:
            device_name_key = new_device_name
        self.__renamed_devices[device_name_key] = new_device_name

        self.__save_persistent_devices()
        self.__load_persistent_devices()
        log.debug("Current renamed_devices dict: %s", self.__renamed_devices)

    def __process_remote_configuration(self, new_configuration):
        if new_configuration is not None and self.__remote_configurator is not None:
            try:
                self.__remote_configurator.process_configuration(new_configuration)
                self.__remote_configurator.send_current_configuration()
            except Exception as e:
                log.exception(e)

    def get_config_path(self):
        return self._config_dir

    def subscribe_to_required_topics(self):
        self.tb_client.client.clean_device_sub_dict()
        self.tb_client.client.gw_set_server_side_rpc_request_handler(self._rpc_request_handler)
        self.tb_client.client.set_server_side_rpc_request_handler(self._rpc_request_handler)
        self.tb_client.client.subscribe_to_all_attributes(self._attribute_update_callback)
        self.tb_client.client.gw_subscribe_to_all_attributes(self._attribute_update_callback)

    def __check_shared_attributes(self):
        self.tb_client.client.request_attributes(callback=self._attributes_parse)

    def __register_connector(self, session_id, connector_key):
        if self.__grpc_connectors.get(connector_key) is not None and self.__grpc_connectors[connector_key]['name'] not in self.available_connectors:
            target_connector = self.__grpc_connectors.get(connector_key)
            connector = GrpcConnector(self, target_connector['config'], self.__grpc_manager, session_id)
            connector.setName(target_connector['name'])
            self.available_connectors[connector.get_name()] = connector
            self.__grpc_manager.registration_finished(Status.SUCCESS, session_id, target_connector)
            log.info("GRPC connector with key %s registered with name %s", connector_key, connector.get_name())
        elif self.__grpc_connectors.get(connector_key) is not None:
            self.__grpc_manager.registration_finished(Status.FAILURE, session_id, None)
            log.error("GRPC connector with key: %s - already registered!", connector_key)
        else:
            self.__grpc_manager.registration_finished(Status.NOT_FOUND, session_id, None)
            log.error("GRPC configuration for connector with key: %s - not found", connector_key)

    def __unregister_connector(self, session_id, connector_key):
        if self.__grpc_connectors.get(connector_key) is not None and self.__grpc_connectors[connector_key]['name'] in self.available_connectors:
            connector_name = self.__grpc_connectors[connector_key]['name']
            target_connector: GrpcConnector = self.available_connectors.pop(connector_name)
            self.__grpc_manager.unregister(Status.SUCCESS, session_id, target_connector)
            log.info("GRPC connector with key %s and name %s - unregistered", connector_key, target_connector.get_name())
        elif self.__grpc_connectors.get(connector_key) is not None:
            self.__grpc_manager.unregister(Status.NOT_FOUND, session_id, None)
            log.error("GRPC connector with key: %s - is not registered!", connector_key)
        else:
            self.__grpc_manager.unregister(Status.FAILURE, session_id, None)
            log.error("GRPC configuration for connector with key: %s - not found in configuration and not registered", connector_key)

    def _load_connectors(self):
        self.connectors_configs = {}
        connectors_persistent_keys = self.__load_persistent_connector_keys()
        if self.__config.get("connectors"):
            for connector in self.__config['connectors']:
                try:
                    connector_persistent_key = None
                    if connector['type'] == "grpc" and self.__grpc_manager is None:
                        log.error("Cannot load connector with name: %s and type grpc. GRPC server is disabled!", connector['name'])
                        continue
                    if connector['type'] != "grpc":
                        connector_class = TBModuleLoader.import_module(connector['type'],
                                                                       self._default_connectors.get(connector['type'],
                                                                                                    connector.get('class')))
                        self._implemented_connectors[connector['type']] = connector_class
                    elif connector['type'] == "grpc":
                        if connector.get('key') == "auto":
                            if connectors_persistent_keys and connectors_persistent_keys.get(connector['name']) is not None:
                                connector_persistent_key = connectors_persistent_keys[connector['name']]
                            else:
                                connector_persistent_key = "".join(choice(hexdigits) for _ in range(10))
                                connectors_persistent_keys[connector['name']] = connector_persistent_key
                        else:
                            connector_persistent_key = connector['key']
                        log.info("Connector key for GRPC connector with name [%s] is: [%s]", connector['name'], connector_persistent_key)
                    config_file_path = self._config_dir + connector['configuration']
                    connector_conf_file_data = ''
                    with open(config_file_path, 'r', encoding="UTF-8") as conf_file:
                        connector_conf_file_data = conf_file.read()

                    connector_conf = connector_conf_file_data
                    try:
                        connector_conf = loads(connector_conf_file_data)
                    except JSONDecodeError as e:
                        log.debug(e)
                        log.warning("Cannot parse connector configuration as a JSON, it will be passed as a string.")

                    if not self.connectors_configs.get(connector['type']):
                        self.connectors_configs[connector['type']] = []
                    if connector['type'] != 'grpc' and isinstance(connector_conf, dict):
                        connector_conf["name"] = connector['name']
                    self.connectors_configs[connector['type']].append({"name": connector['name'],
                                                                       "config": {connector['configuration']: connector_conf} if connector[
                                                                                                                                     'type'] != 'grpc' else connector_conf,
                                                                       "config_updated": stat(config_file_path),
                                                                       "config_file_path": config_file_path,
                                                                       "grpc_key": connector_persistent_key})
                except Exception as e:
                    log.exception("Error on loading connector: %r", e)
            if connectors_persistent_keys:
                self.__save_persistent_keys(connectors_persistent_keys)
        else:
            log.error("Connectors - not found! Check your configuration!")
            self.__init_remote_configuration(force=True)
            log.info("Remote configuration is enabled forcibly!")

    def _connect_with_connectors(self):
        for connector_type in self.connectors_configs:
            for connector_config in self.connectors_configs[connector_type]:
                if connector_type.lower() != 'grpc':
                    for config in connector_config["config"]:
                        connector = None
                        try:
                            if connector_config["config"][config] is not None:
                                if self._implemented_connectors[connector_type]:
                                    connector = self._implemented_connectors[connector_type](self, connector_config["config"][config], connector_type)
                                    connector.setName(connector_config["name"])
                                    self.available_connectors[connector.get_name()] = connector
                                    connector.open()
                                else:
                                    log.warning("Connector implementation not found for %s", connector_config["name"])
                            else:
                                log.info("Config not found for %s", connector_type)
                        except Exception as e:
                            log.exception(e)
                            if connector is not None:
                                connector.close()
                else:
                    self.__grpc_connectors.update({connector_config['grpc_key']: connector_config})

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
            self._connect_with_connectors()

    def send_to_storage(self, connector_name, data):
        try:
            self.__converted_data_queue.put((connector_name, data), True, 100)
            return Status.SUCCESS
        except Exception as e:
            log.exception("Cannot put converted data!", e)
            return Status.FAILURE

    def __send_to_storage(self):
        while True:
            try:
                if not self.__converted_data_queue.empty():
                    connector_name, event = self.__converted_data_queue.get(True, 100)
                    data_array = event if isinstance(event, list) else [event]
                    for data in data_array:
                        if not connector_name == self.name:
                            if 'telemetry' not in data:
                                data['telemetry'] = []
                            if 'attributes' not in data:
                                data['attributes'] = []
                            if not TBUtility.validate_converted_data(data):
                                log.error("Data from %s connector is invalid.", connector_name)
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
                                                {"connector": self.available_connectors[connector_name]},
                                                device_type=data["deviceType"])
                            if not self.__connector_incoming_messages.get(connector_name):
                                self.__connector_incoming_messages[connector_name] = 0
                            else:
                                self.__connector_incoming_messages[connector_name] += 1
                        else:
                            data["deviceName"] = "currentThingsBoardGateway"

                        data = self.__convert_telemetry_to_ts(data)

                        max_data_size = self.__config["thingsboard"].get("maxPayloadSizeBytes", 1024)
                        if self.__get_data_size(data) >= max_data_size:
                            adopted_data = {"deviceName": data['deviceName'],
                                            "deviceType": data['deviceType'],
                                            "attributes": {},
                                            "telemetry": []}
                            for attribute in data['attributes']:
                                adopted_data_size = self.__get_data_size(adopted_data)
                                if adopted_data_size >= max_data_size:
                                    self.__send_data_pack_to_storage(adopted_data, connector_name)
                                    adopted_data['attributes'] = {}
                                adopted_data['attributes'].update({attribute: data['attributes'][attribute]})
                            for ts_kv_list in data['telemetry']:
                                ts = ts_kv_list['ts']
                                for kv in ts_kv_list['values']:
                                    adopted_data_size = self.__get_data_size(adopted_data)
                                    if adopted_data_size >= max_data_size:
                                        self.__send_data_pack_to_storage(adopted_data, connector_name)
                                        adopted_data['telemetry'] = []
                                    if len(adopted_data['telemetry']) == 0:
                                        adopted_data['telemetry'] = [{'ts': ts, 'values': {kv: ts_kv_list['values'][kv]}}]
                                    else:
                                        for adopted_kv in adopted_data['telemetry']:
                                            if adopted_kv['ts'] == ts:
                                                adopted_kv['values'].update({kv: ts_kv_list['values'][kv]})

                        else:
                            self.__send_data_pack_to_storage(data, connector_name)

                else:
                    sleep(0.2)
            except Exception as e:
                log.error(e)

    @staticmethod
    def __get_data_size(data: dict):
        return getsizeof(str(data))

    @staticmethod
    def __convert_telemetry_to_ts(data):
        telemetry = {}
        telemetry_with_ts = []
        for item in data["telemetry"]:
            if item.get("ts") is None:
                telemetry = {**telemetry, **item}
            else:
                telemetry_with_ts.append({"ts": item["ts"], "values": {**item["values"]}})
        if telemetry_with_ts:
            data["telemetry"] = telemetry_with_ts
        elif len(data['telemetry']) > 0:
            data["telemetry"] = {"ts": int(time() * 1000), "values": telemetry}
        return data

    def __send_data_pack_to_storage(self, data, connector_name):
        json_data = dumps(data)
        save_result = self._event_storage.put(json_data)
        if not save_result:
            log.error('Data from the device "%s" cannot be saved, connector name is %s.',
                      data["deviceName"],
                      connector_name)

    def check_size(self, devices_data_in_event_pack):
        if self.__get_data_size(devices_data_in_event_pack) >= self.__config["thingsboard"].get("maxPayloadSizeBytes", 1024):
            self.__send_data(devices_data_in_event_pack)
            for device in devices_data_in_event_pack:
                devices_data_in_event_pack[device]["telemetry"] = []
                devices_data_in_event_pack[device]["attributes"] = {}

    def __read_data_from_storage(self):
        devices_data_in_event_pack = {}
        log.debug("Send data Thread has been started successfully.")

        while not self.stopped:
            try:
                if self.tb_client.is_connected():
                    size = self.__get_data_size(devices_data_in_event_pack) - 2
                    events = []

                    if self.__remote_configurator is None or not self.__remote_configurator.in_process:
                        events = self._event_storage.get_event_pack()

                    if events:
                        for event in events:
                            self.counter += 1
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
                                        devices_data_in_event_pack[current_event["deviceName"]]["telemetry"].append(
                                            item)
                                else:
                                    self.check_size(devices_data_in_event_pack)
                                    devices_data_in_event_pack[current_event["deviceName"]]["telemetry"].append(
                                        current_event["telemetry"])
                            if current_event.get("attributes"):
                                if isinstance(current_event["attributes"], list):
                                    for item in current_event["attributes"]:
                                        self.check_size(devices_data_in_event_pack)
                                        devices_data_in_event_pack[current_event["deviceName"]]["attributes"].update(
                                            item.items())
                                else:
                                    self.check_size(devices_data_in_event_pack)
                                    devices_data_in_event_pack[current_event["deviceName"]]["attributes"].update(
                                        current_event["attributes"].items())
                        if devices_data_in_event_pack:
                            if not self.tb_client.is_connected():
                                continue
                            while self.__rpc_reply_sent:
                                sleep(.2)
                            self.__send_data(devices_data_in_event_pack)
                            sleep(self.__min_pack_send_delay_ms)

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
                                event = self._published_events.get(False, 10)
                                try:
                                    if self.tb_client.is_connected() and (
                                            self.__remote_configurator is None or not self.__remote_configurator.in_process):
                                        if self.tb_client.client.quality_of_service == 1:
                                            success = event.get() == event.TB_ERR_SUCCESS
                                        else:
                                            success = True
                                    else:
                                        break
                                except Exception as e:
                                    log.exception(e)
                                    success = False
                                sleep(0.2)
                            if success and self.tb_client.is_connected():
                                self._event_storage.event_pack_processing_done()
                                del devices_data_in_event_pack
                                devices_data_in_event_pack = {}
                        else:
                            continue
                    else:
                        sleep(0.2)
                else:
                    sleep(0.2)
            except Exception as e:
                log.exception(e)
                sleep(1)

    def __send_data(self, devices_data_in_event_pack):
        try:
            for device in devices_data_in_event_pack:
                final_device_name = device if self.__renamed_devices.get(device) is None else self.__renamed_devices[device]
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
                connector_name = self.get_devices()[device].get("connector")
                if connector_name is not None:
                    connector_name.server_side_rpc_handler(content)
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
                            for connector_name in self.available_connectors:
                                if self.available_connectors[connector_name]._connector_type == module:
                                    log.debug("Sending command RPC %s to connector %s", content["method"],
                                              connector_name)
                                    result = self.available_connectors[connector_name].server_side_rpc_handler(content)
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
            method_function = self.__remote_shell.shell_commands.get(method_to_call,
                                                                     self.__gateway_rpc_methods.get(method_to_call))
        else:
            log.info("Remote shell is disabled.")
            method_function = self.__gateway_rpc_methods.get(method_to_call)
        if method_function is None and method_to_call in self.__rpc_scheduled_methods_functions:
            seconds_to_restart = arguments * 1000 if arguments and arguments != '{}' else 0
            self.__scheduled_rpc_calls.append(
                [time() * 1000 + seconds_to_restart, self.__rpc_scheduled_methods_functions[method_to_call]])
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
            if self.__connected_devices[device]["connector"] is not None:
                data_to_send[device] = self.__connected_devices[device]["connector"].get_name()
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
            result = {"resp": self.__updater.get_version(),
                      "code": 200,
                      }
        except Exception as e:
            result = {"error": str(e),
                      "code": 500
                      }
        return result

    def is_rpc_in_progress(self, topic):
        return topic in self.__rpc_requests_in_progress

    def rpc_with_reply_processing(self, topic, content):
        req_id = self.__rpc_requests_in_progress[topic][0]["data"]["id"]
        device = self.__rpc_requests_in_progress[topic][0]["device"]
        log.info("Outgoing RPC. Device: %s, ID: %d", device, req_id)
        self.send_rpc_reply(device, req_id, content)

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

    def __send_rpc_reply(self, device=None, req_id=None, content=None, success_sent=None, wait_for_publish=None, quality_of_service=0):
        try:
            self.__rpc_reply_sent = True
            rpc_response = {"success": False}
            if success_sent is not None:
                if success_sent:
                    rpc_response["success"] = True
            if device is not None and success_sent is not None:
                self.tb_client.client.gw_send_rpc_reply(device, req_id, dumps(rpc_response),
                                                        quality_of_service=quality_of_service)
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
                self.__connected_devices[content["device"]]["connector"].on_attributes_update(content)
            except Exception as e:
                log.exception(e)
        else:
            self._attributes_parse(content)

    def __form_statistics(self):
        summary_messages = {"eventsProduced": 0, "eventsSent": 0}
        telemetry = {}
        for connector in self.available_connectors:
            connector_camel_case = connector.lower().replace(' ', '')
            telemetry[(connector_camel_case + ' EventsProduced').replace(' ', '')] = \
                self.available_connectors[connector].statistics['MessagesReceived']
            self.available_connectors[connector].statistics['MessagesReceived'] = 0
            telemetry[(connector_camel_case + ' EventsSent').replace(' ', '')] = \
                self.available_connectors[connector].statistics['MessagesSent']
            self.available_connectors[connector].statistics['MessagesSent'] = 0
            summary_messages['eventsProduced'] += telemetry[
                str(connector_camel_case + ' EventsProduced').replace(' ', '')]
            summary_messages['eventsSent'] += telemetry[
                str(connector_camel_case + ' EventsSent').replace(' ', '')]
            summary_messages.update(**telemetry)
        return summary_messages

    def add_device_async(self, data):
        if data['deviceName'] not in self.__saved_devices:
            self.__async_device_actions_queue.put((DeviceActions.CONNECT, data))
            return Status.SUCCESS
        else:
            return Status.FAILURE

    def add_device(self, device_name, content, device_type=None):
        if device_name not in self.__saved_devices:
            device_type = device_type if device_type is not None else 'default'
            self.__connected_devices[device_name] = {**content, "device_type": device_type}
            self.__saved_devices[device_name] = {**content, "device_type": device_type}
            self.__save_persistent_devices()
            self.tb_client.client.gw_connect_device(device_name, device_type)

    def update_device(self, device_name, event, content):
        if event == 'connector' and self.__connected_devices[device_name].get(event) != content:
            self.__save_persistent_devices()
        self.__connected_devices[device_name][event] = content

    def del_device_async(self, data):
        if data['deviceName'] in self.__saved_devices:
            self.__async_device_actions_queue.put((DeviceActions.DISCONNECT, data))
            return Status.SUCCESS
        else:
            return Status.FAILURE

    def del_device(self, device_name):
        del self.__connected_devices[device_name]
        del self.__saved_devices[device_name]
        self.tb_client.client.gw_disconnect_device(device_name)
        self.__save_persistent_devices()

    def get_devices(self):
        return self.__connected_devices

    def __process_async_device_actions(self):
        while not self.stopped:
            if not self.__async_device_actions_queue.empty():
                action, data = self.__async_device_actions_queue.get()
                if action == DeviceActions.CONNECT:
                    self.add_device(data['deviceName'], {CONNECTOR_PARAMETER: self.available_connectors[data['name']]}, data.get('deviceType'))
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
        devices = None
        if CONNECTED_DEVICES_FILENAME in listdir(self._config_dir) and \
                path.getsize(self._config_dir + CONNECTED_DEVICES_FILENAME) > 0:
            try:
                devices = load_file(self._config_dir + CONNECTED_DEVICES_FILENAME)
            except Exception as e:
                log.exception(e)
        else:
            open(self._config_dir + CONNECTED_DEVICES_FILENAME, 'w').close()

        if devices is not None:
            log.debug("Loaded devices:\n %s", devices)
            for device_name in devices:
                try:
                    if not isinstance(devices[device_name], list):
                        open(self._config_dir + CONNECTED_DEVICES_FILENAME, 'w').close()
                        log.debug("Old connected_devices file, new file will be created")
                        return
                    if self.available_connectors.get(devices[device_name][0]):
                        device_data_to_save = {
                            "connector": self.available_connectors[devices[device_name][0]],
                            "device_type": devices[device_name][1]}
                        if len(devices[device_name]) > 2 and device_name not in self.__renamed_devices:
                            new_device_name = devices[device_name][2]
                            self.__renamed_devices[device_name] = new_device_name
                        self.__connected_devices[device_name] = device_data_to_save
                        self.__saved_devices[device_name] = device_data_to_save
                except Exception as e:
                    log.exception(e)
                    continue
        else:
            log.debug("No device found in connected device file.")
            self.__connected_devices = {} if self.__connected_devices is None else self.__connected_devices

    def __save_persistent_devices(self):
        data_to_save = {}
        for device in self.__connected_devices:
            if self.__connected_devices[device]["connector"] is not None:
                data_to_save[device] = [self.__connected_devices[device]["connector"].get_name(), self.__connected_devices[device]["device_type"]]
                if device in self.__renamed_devices:
                    data_to_save[device].append(self.__renamed_devices.get(device))
        with open(self._config_dir + CONNECTED_DEVICES_FILENAME, 'w') as config_file:
            try:
                config_file.write(dumps(data_to_save, indent=2, sort_keys=True))
            except Exception as e:
                log.exception(e)
        log.debug("Saved connected devices.")


if __name__ == '__main__':
    TBGatewayService(
        path.dirname(path.dirname(path.abspath(__file__))) + '/config/tb_gateway.yaml'.replace('/', path.sep))
