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

from base64 import b64encode, b64decode
from simplejson import dumps, loads, dump
from yaml import safe_dump
from time import time, sleep
from logging import getLogger
from os import remove
from thingsboard_gateway.gateway.tb_client import TBClient
from thingsboard_gateway.gateway.tb_logger import TBLoggerHandler

log = getLogger("service")


class RemoteConfigurator:
    def __init__(self, gateway, config):
        self.__gateway = gateway
        self.__new_configuration = None
        self.__old_configuration = None
        self.__apply_timeout = 10
        self.__old_tb_client = None
        self.__old_connectors_configs = {}
        self.__new_connectors_configs = {}
        self.__old_general_configuration_file = config
        self.__new_general_configuration_file = {}
        self.__old_event_storage = None
        self.__new_event_storage = None
        self.in_process = False

    def process_configuration(self, configuration):
        try:
            if not self.in_process:
                self.in_process = True
                decoded_configuration = b64decode(configuration)
                self.__new_configuration = loads(decoded_configuration)
                self.__old_connectors_configs = self.__gateway._connectors_configs
                self.__new_general_configuration_file = self.__new_configuration.get("thingsboard")
                if self.__old_configuration != self.__new_configuration:
                    log.info("Remote configuration received: \n %s", decoded_configuration)
                    self.__process_connectors_configuration()
                    self.__old_configuration = self.__new_configuration
                    # self.send_current_configuration()
                self.in_process = False
            else:
                log.error("Remote configuration is already in processing")
        except Exception as e:
            self.in_process = False
            log.exception(e)

    def send_current_configuration(self):
        current_configuration = {}
        for connector in self.__gateway._connectors_configs:
            log.debug(connector)
            if current_configuration.get(connector) is None:
                current_configuration[connector] = []
            for config_file in self.__gateway._connectors_configs[connector]:
                current_configuration[connector].append(config_file)
        current_configuration["thingsboard"] = self.__old_general_configuration_file
        encoded_current_configuration = b64encode(dumps(current_configuration).encode())
        self.__old_configuration = encoded_current_configuration
        self.__gateway.tb_client.client.send_attributes(
            {"current_configuration": encoded_current_configuration.decode("UTF-8")}).get()

    def __process_connectors_configuration(self):
        log.debug("Processing remote connectors configuration...")
        self.__apply_storage_configuration()
        if self.__apply_new_connectors_configuration():
            self.__write_new_configuration_files()
        if self.__safe_apply_connection_configuration():
            log.info("Remote configuration has been applied.")
            with open(self.__gateway._config_dir + "tb_gateway.yaml", "w") as general_configuration_file:
                safe_dump(self.__new_general_configuration_file, general_configuration_file)
            self.__old_connectors_configs = {}
            self.__new_connectors_configs = {}
            self.__old_general_configuration_file = self.__new_general_configuration_file
            self.__new_general_configuration_file = {}
        else:
            log.error("A remote general configuration applying has been failed.")
            self.__old_connectors_configs = {}
            self.__new_connectors_configs = {}
            self.__new_general_configuration_file = {}
            return

    def __apply_new_connectors_configuration(self):
        try:
            self.__gateway._load_connectors(self.__new_configuration["thingsboard"])
            for connector_name in self.__gateway.available_connectors:
                try:
                    self.__gateway.available_connectors[connector_name].close()
                except Exception as e:
                    log.exception(e)
            self.__gateway._connect_with_connectors()
            log.debug("New connectors configuration has been applied")
            self.__old_connectors_configs = {}
            return True
        except Exception as e:
            self.__gateway._connectors_configs = self.__old_connectors_configs
            for connector_name in self.__gateway.available_connectors:
                self.__gateway.available_connectors[connector_name].close()
            self.__gateway._connect_with_connectors()
            log.exception(e)
            return False

    def __write_new_configuration_files(self):
        try:
            self.__new_connectors_configs = self.__new_connectors_configs if self.__new_connectors_configs else self.__gateway._connectors_configs
            new_connectors_files = []
            for connector_type in self.__new_connectors_configs:
                for connector_config_section in self.__new_connectors_configs[connector_type]:
                    for connector_file in connector_config_section["config"]:
                        connector_config = connector_config_section["config"][connector_file]
                        for conn in self.__new_general_configuration_file["connectors"]:
                            with open(self.__gateway._config_dir + conn.get("configuration"), "w") as config_file:
                                dump(connector_config, config_file, sort_keys=True, indent=2)
                            new_connectors_files.append(connector_file)
                            log.debug("Saving new configuration for \"%s\" connector to file \"%s\"", connector_type,
                                      connector_file)
            for old_connector_type in self.__old_connectors_configs:
                for old_connector_config_section in self.__old_connectors_configs[old_connector_type]:
                    for old_connector_file in old_connector_config_section["config"]:
                        if old_connector_file not in new_connectors_files:
                            remove(self.__gateway._config_dir + old_connector_file)
                        log.debug("Remove old configuration file \"%s\" for \"%s\" connector ", old_connector_file,
                                  old_connector_type)
        except Exception as e:
            log.exception(e)

    def __safe_apply_connection_configuration(self):
        apply_start = time() * 1000
        self.__old_tb_client = self.__gateway.tb_client
        try:
            self.__old_tb_client.pause()
        except Exception as e:
            log.exception(e)
            self.__revert_configuration()
            return False
        try:
            tb_client = TBClient(self.__new_general_configuration_file["thingsboard"])
            tb_client.connect()
        except Exception as e:
            log.exception(e)
            self.__revert_configuration()
            return False
        self.__gateway.tb_client = tb_client
        try:
            connection_state = False
            while time() * 1000 - apply_start < self.__apply_timeout * 1000 and not connection_state:
                connection_state = self.__gateway.tb_client.is_connected()
                sleep(.1)
            if not connection_state:
                self.__revert_configuration()
                log.info("The gateway cannot connect to the ThingsBoard server with a new configuration.")
                return False
            else:
                self.__old_tb_client.unsubscribe("*")
                self.__old_tb_client.stop()
                self.__gateway.tb_client.client.gw_set_server_side_rpc_request_handler(
                    self.__gateway._rpc_request_handler)
                self.__gateway.tb_client.client.set_server_side_rpc_request_handler(self.__gateway._rpc_request_handler)
                self.__gateway.tb_client.client.subscribe_to_all_attributes(self.__gateway._attribute_update_callback)
                self.__gateway.tb_client.client.gw_subscribe_to_all_attributes(
                    self.__gateway._attribute_update_callback)
                return True
        except Exception as e:
            log.exception(e)
            self.__revert_configuration()
            return False

    def __apply_storage_configuration(self):
        if self.__old_general_configuration_file["storage"] != self.__new_general_configuration_file["storage"]:
            self.__old_event_storage = self.__gateway._event_storage
            try:
                self.__gateway._event_storage = self.__gateway._event_storage_types[
                    self.__new_general_configuration_file["storage"]["type"]](
                    self.__new_general_configuration_file["storage"])
                self.__old_event_storage = None
            except Exception as e:
                log.exception(e)
                self.__gateway._event_storage = self.__old_event_storage

    def __revert_configuration(self):
        log.info("Remote general configuration will be restored.")
        self.__new_general_configuration_file = self.__old_general_configuration_file
        self.__gateway.tb_client.disconnect()
        self.__gateway.tb_client.stop()
        self.__gateway.tb_client = self.__old_tb_client
        self.__gateway.tb_client.connect()
        self.__gateway.tb_client.unpause()
