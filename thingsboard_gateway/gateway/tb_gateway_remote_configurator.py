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
from logging.config import fileConfig
from logging.handlers import MemoryHandler
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
        self.__old_logs_configuration = self.__get_current_logs_configuration()
        self.__new_logs_configuration = None
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
                self.__old_connectors_configs = self.__gateway.connectors_configs
                self.__new_general_configuration_file = self.__new_configuration.get("thingsboard")
                self.__new_logs_configuration = b64decode(self.__new_general_configuration_file.pop("logs")).decode('UTF-8').replace('}}', '\n')
                if self.__old_configuration != decoded_configuration:
                    log.info("Remote configuration received: \n %s", decoded_configuration)
                    result = self.__process_connectors_configuration()
                    self.in_process = False
                    if result:
                        self.__old_configuration = self.__new_configuration
                        return True
                    else:
                        return False
                else:
                    log.info("Remote configuration is the same.")
            else:
                log.error("Remote configuration is already in processing")
                return False
        except Exception as e:
            self.in_process = False
            log.exception(e)

    def send_current_configuration(self):
        try:
            current_configuration = {}
            for connector in self.__gateway.connectors_configs:
                if current_configuration.get(connector) is None:
                    current_configuration[connector] = []
                for config in self.__gateway.connectors_configs[connector]:
                    for config_file in config['config']:
                        current_configuration[connector].append({'name': config['name'], 'config': config['config'][config_file]})
            current_configuration["thingsboard"] = self.__old_general_configuration_file
            current_configuration["thingsboard"]["logs"] = b64encode(self.__old_logs_configuration.replace('\n', '}}').encode("UTF-8"))
            json_current_configuration = dumps(current_configuration)
            encoded_current_configuration = b64encode(json_current_configuration.encode())
            self.__old_configuration = encoded_current_configuration
            self.__gateway.tb_client.client.send_attributes(
                {"current_configuration": encoded_current_configuration.decode("UTF-8")})
            log.debug('Current configuration has been sent to ThingsBoard: %s', json_current_configuration)
        except Exception as e:
            log.exception(e)

    def __process_connectors_configuration(self):
        log.info("Processing remote connectors configuration...")
        if self.__apply_new_connectors_configuration():
            self.__write_new_configuration_files()
        self.__apply_storage_configuration()
        if self.__safe_apply_connection_configuration():
            self.__update_logs_configuration()
            log.info("Remote configuration has been applied.")
            with open(self.__gateway._config_dir + "tb_gateway.yaml", "w") as general_configuration_file:
                safe_dump(self.__new_general_configuration_file, general_configuration_file)
            self.__old_connectors_configs = {}
            self.__new_connectors_configs = {}
            self.__old_general_configuration_file = self.__new_general_configuration_file
            self.__old_logs_configuration = self.__new_logs_configuration
            self.__new_logs_configuration = None
            self.__new_general_configuration_file = {}
            return True
        else:
            self.__update_logs_configuration()
            self.__old_general_configuration_file.pop("logs")
            with open(self.__gateway._config_dir + "tb_gateway.yaml", "w") as general_configuration_file:
                safe_dump(self.__old_general_configuration_file, general_configuration_file)
            log.error("A remote general configuration applying has been failed.")
            self.__old_connectors_configs = {}
            self.__new_connectors_configs = {}
            self.__new_logs_configuration = None
            self.__new_general_configuration_file = {}
            return False

    def __prepare_connectors_configuration(self, input_connector_config):
        try:
            self.__gateway.connectors_configs = {}
            for connector in input_connector_config['thingsboard']['connectors']:
                for input_connector in input_connector_config[connector['type']]:
                    if input_connector['name'] == connector['name']:
                        if not self.__gateway.connectors_configs.get(connector['type']):
                            self.__gateway.connectors_configs[connector['type']] = []
                        self.__gateway.connectors_configs[connector['type']].append(
                            {"name": connector["name"], "config": {connector['configuration']: input_connector["config"]}})
        except Exception as e:
            log.exception(e)

    def __apply_new_connectors_configuration(self):
        try:
            self.__prepare_connectors_configuration(self.__new_configuration)
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
            self.__gateway.connectors_configs = self.__old_connectors_configs
            for connector_name in self.__gateway.available_connectors:
                self.__gateway.available_connectors[connector_name].close()
            self.__gateway._connect_with_connectors()
            log.exception(e)
            return False

    def __write_new_configuration_files(self):
        try:
            self.__new_connectors_configs = self.__new_connectors_configs if self.__new_connectors_configs else self.__gateway.connectors_configs
            new_connectors_files = []
            for connector_type in self.__new_connectors_configs:
                for connector_config_section in self.__new_connectors_configs[connector_type]:
                    for connector_file in connector_config_section["config"]:
                        connector_config = connector_config_section["config"][connector_file]
                        with open(self.__gateway._config_dir + connector_file, "w") as config_file:
                            dump(connector_config, config_file, sort_keys=True, indent=2)
                        new_connectors_files.append(connector_file)
                        log.debug("Saving new configuration for \"%s\" connector to file \"%s\"", connector_type,
                                  connector_file)
                        break
            self.__old_general_configuration_file["connectors"] = self.__new_general_configuration_file["connectors"]
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
            self.__old_tb_client.unsubscribe('*')
            self.__old_tb_client.stop()
            self.__old_tb_client.disconnect()
            self.__gateway.tb_client = TBClient(self.__new_general_configuration_file["thingsboard"])
            self.__gateway.tb_client.connect()
            connection_state = False
            while time() * 1000 - apply_start < self.__apply_timeout * 1000 and not connection_state:
                connection_state = self.__gateway.tb_client.is_connected()
                sleep(.1)
            if not connection_state:
                self.__revert_configuration()
                log.info("The gateway cannot connect to the ThingsBoard server with a new configuration.")
                return False
            else:
                self.__old_tb_client.stop()
                self.__gateway.subscribe_to_required_topics()
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
        try:
            log.info("Remote general configuration will be restored.")
            self.__new_general_configuration_file = self.__old_general_configuration_file
            self.__gateway.tb_client.disconnect()
            self.__gateway.tb_client.stop()
            self.__gateway.tb_client = TBClient(self.__old_general_configuration_file["thingsboard"])
            self.__gateway.tb_client.connect()
            self.__gateway.subscribe_to_required_topics()
            log.debug("%s connection has been restored", str(self.__gateway.tb_client.client._client))
        except Exception as e:
            log.exception("Exception on reverting configuration occurred:")
            log.exception(e)

    def __get_current_logs_configuration(self):
        try:
            with open(self.__gateway._config_dir + 'logs.conf', 'r') as logs:
                current_logs_configuration = logs.read()
            return current_logs_configuration
        except Exception as e:
            log.exception(e)

    def __update_logs_configuration(self):
        try:
            if self.__old_logs_configuration == self.__new_logs_configuration:
                remote_handler_current_state = self.__gateway.remote_handler.activated
                remote_handler_current_level = self.__gateway.remote_handler.current_log_level
                logs_conf_file_path = self.__gateway._config_dir + 'logs.conf'
                with open(logs_conf_file_path, 'w') as logs:
                    logs.write(self.__new_logs_configuration+"\r\n")
                fileConfig(logs_conf_file_path)
                self.__gateway.main_handler = MemoryHandler(-1)
                self.__gateway.remote_handler = TBLoggerHandler(self.__gateway)
                self.__gateway.main_handler.setTarget(self.__gateway.remote_handler)
                if remote_handler_current_level != 'NOTSET':
                    self.__gateway.remote_handler.activate(remote_handler_current_level)
                if not remote_handler_current_state:
                    self.__gateway.remote_handler.deactivate()
                global log
                log = getLogger('service')
                log.debug("Logs configuration has been updated.")
        except Exception as e:
            log.exception(e)

