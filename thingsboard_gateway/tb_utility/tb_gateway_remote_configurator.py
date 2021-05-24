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

from os import remove, linesep
from os.path import exists, dirname
from re import findall
from time import time, sleep
from logging import getLogger
from logging.config import fileConfig
from base64 import b64encode, b64decode
from simplejson import dumps, loads, dump
from yaml import safe_dump
from configparser import ConfigParser

from thingsboard_gateway.gateway.tb_client import TBClient
from thingsboard_gateway.tb_utility.tb_loader import TBModuleLoader
from thingsboard_gateway.tb_utility.tb_logger import TBLoggerHandler

# pylint: disable=protected-access
LOG = getLogger("service")


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
                # while not self.__gateway._published_events.empty():
                #     LOG.debug("Waiting for end of the data processing...")
                #     sleep(1)
                decoded_configuration = b64decode(configuration)
                self.__new_configuration = loads(decoded_configuration)
                self.__old_connectors_configs = self.__gateway.connectors_configs
                self.__new_general_configuration_file = self.__new_configuration.get("thingsboard")
                self.__new_logs_configuration = b64decode(self.__new_general_configuration_file.pop("logs")).decode('UTF-8').replace('}}', '\n')
                if self.__old_configuration != decoded_configuration:
                    LOG.info("Remote configuration received: \n %s", decoded_configuration)
                    result = self.__process_connectors_configuration()
                    self.in_process = False
                    if result:
                        self.__old_configuration = self.__new_configuration
                        return True
                    else:
                        return False
                else:
                    LOG.info("Remote configuration is the same.")
            else:
                LOG.error("Remote configuration is already in processing")
                return False
        except Exception as e:
            self.in_process = False
            LOG.exception(e)

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
            LOG.debug('Current configuration has been sent to ThingsBoard: %s', json_current_configuration)
        except Exception as e:
            LOG.exception(e)

    def __process_connectors_configuration(self):
        LOG.info("Processing remote connectors configuration...")
        if self.__apply_new_connectors_configuration():
            self.__write_new_configuration_files()
        self.__apply_storage_configuration()
        if self.__safe_apply_connection_configuration():
            LOG.info("Remote configuration has been applied.")
            with open(self.__gateway.get_config_path() + "tb_gateway.yaml", "w", encoding="UTF-8") as general_configuration_file:
                safe_dump(self.__new_general_configuration_file, general_configuration_file)
            self.__old_connectors_configs = {}
            self.__new_connectors_configs = {}
            self.__old_general_configuration_file = self.__new_general_configuration_file
            self.__old_logs_configuration = self.__new_logs_configuration
            self.__update_logs_configuration()
            self.__new_logs_configuration = None
            self.__new_general_configuration_file = {}
            return True
        else:
            self.__update_logs_configuration()
            self.__old_general_configuration_file.pop("logs")
            with open(self.__gateway.get_config_path() + "tb_gateway.yaml", "w", encoding="UTF-8") as general_configuration_file:
                safe_dump(self.__old_general_configuration_file, general_configuration_file)
            LOG.error("A remote general configuration applying has been failed.")
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
                        connector_class = TBModuleLoader.import_module(connector["type"], self.__gateway._default_connectors.get(connector["type"], connector.get("class")))
                        self.__gateway._implemented_connectors[connector["type"]] = connector_class
        except Exception as e:
            LOG.exception(e)

    def __apply_new_connectors_configuration(self):
        try:
            self.__prepare_connectors_configuration(self.__new_configuration)
            for connector_name in self.__gateway.available_connectors:
                try:
                    self.__gateway.available_connectors[connector_name].close()
                except Exception as e:
                    LOG.exception(e)
            self.__gateway._connect_with_connectors()
            LOG.debug("New connectors configuration has been applied")
            self.__old_connectors_configs = {}
            return True
        except Exception as e:
            self.__gateway.connectors_configs = self.__old_connectors_configs
            for connector_name in self.__gateway.available_connectors:
                self.__gateway.available_connectors[connector_name].close()
            self.__gateway._load_connectors(self.__old_general_configuration_file)
            self.__gateway._connect_with_connectors()
            LOG.exception(e)
            return False

    def __write_new_configuration_files(self):
        try:
            self.__new_connectors_configs = self.__new_connectors_configs if self.__new_connectors_configs else self.__gateway.connectors_configs
            new_connectors_files = []
            for connector_type in self.__new_connectors_configs:
                for connector_config_section in self.__new_connectors_configs[connector_type]:
                    for connector_file in connector_config_section["config"]:
                        connector_config = connector_config_section["config"][connector_file]
                        with open(self.__gateway.get_config_path() + connector_file, "w", encoding="UTF-8") as config_file:
                            dump(connector_config, config_file, sort_keys=True, indent=2)
                        new_connectors_files.append(connector_file)
                        LOG.debug("Saving new configuration for \"%s\" connector to file \"%s\"", connector_type,
                                  connector_file)
                        break
            self.__old_general_configuration_file["connectors"] = self.__new_general_configuration_file["connectors"]
            for old_connector_type in self.__old_connectors_configs:
                for old_connector_config_section in self.__old_connectors_configs[old_connector_type]:
                    for old_connector_file in old_connector_config_section["config"]:
                        if old_connector_file not in new_connectors_files:
                            remove(self.__gateway.get_config_path() + old_connector_file)
                        LOG.debug("Remove old configuration file \"%s\" for \"%s\" connector ", old_connector_file,
                                  old_connector_type)
        except Exception as e:
            LOG.exception(e)

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
                LOG.info("The gateway cannot connect to the ThingsBoard server with a new configuration.")
                return False
            else:
                self.__old_tb_client.stop()
                self.__gateway.subscribe_to_required_topics()
                return True
        except Exception as e:
            LOG.exception(e)
            self.__revert_configuration()
            return False

    def __apply_storage_configuration(self):
        if self.__old_general_configuration_file["storage"] != self.__new_general_configuration_file["storage"]:
            self.__old_event_storage = self.__gateway._event_storage
            try:
                storage_class = self.__gateway._event_storage_types[self.__new_general_configuration_file["storage"]["type"]]
                self.__gateway._event_storage = storage_class(self.__new_general_configuration_file["storage"])
                self.__old_event_storage = None
            except Exception as e:
                LOG.exception(e)
                self.__gateway._event_storage = self.__old_event_storage

    def __revert_configuration(self):
        try:
            LOG.info("Remote general configuration will be restored.")
            self.__new_general_configuration_file = self.__old_general_configuration_file
            self.__gateway.tb_client.disconnect()
            self.__gateway.tb_client.stop()
            self.__gateway.tb_client = TBClient(self.__old_general_configuration_file["thingsboard"])
            self.__gateway.tb_client.connect()
            self.__gateway.subscribe_to_required_topics()
            LOG.debug("%s connection has been restored", str(self.__gateway.tb_client.client._client))
        except Exception as e:
            LOG.exception("Exception on reverting configuration occurred:")
            LOG.exception(e)

    def __get_current_logs_configuration(self):
        try:
            with open(self.__gateway.get_config_path() + 'logs.conf', 'r', encoding="UTF-8") as logs:
                current_logs_configuration = logs.read()
            return current_logs_configuration
        except Exception as e:
            LOG.exception(e)

    def __update_logs_configuration(self):
        global LOG
        try:
            LOG = getLogger('service')
            logs_conf_file_path = self.__gateway.get_config_path() + 'logs.conf'
            new_logging_level = findall(r'level=(.*)', self.__new_logs_configuration.replace("NONE", "NOTSET"))[-1]
            new_logging_config = self.__new_logs_configuration.replace("NONE", "NOTSET").replace("\r\n", linesep)
            logs_config = ConfigParser(allow_no_value=True)
            logs_config.read_string(new_logging_config)
            for section in logs_config:
                if "handler_" in section and section != "handler_consoleHandler":
                    args = tuple(logs_config[section]["args"]
                                 .replace('(', '')
                                 .replace(')', '')
                                 .split(', '))
                    path = args[0][1:-1]
                    LOG.debug("Checking %s...", path)
                    if not exists(dirname(path)):
                        raise FileNotFoundError
            with open(logs_conf_file_path, 'w', encoding="UTF-8") as logs:
                logs.write(self.__new_logs_configuration.replace("NONE", "NOTSET")+"\r\n")
            fileConfig(logs_config)
            LOG = getLogger('service')
            # self.__gateway.remote_handler.deactivate()
            self.__gateway.remote_handler = TBLoggerHandler(self.__gateway)
            self.__gateway.main_handler.setLevel(new_logging_level)
            self.__gateway.main_handler.setTarget(self.__gateway.remote_handler)
            if new_logging_level == "NOTSET":
                self.__gateway.remote_handler.deactivate()
            else:
                self.__gateway.remote_handler.activate(new_logging_level)
            LOG.debug("Logs configuration has been updated.")
        except Exception as e:
            LOG.error("Remote logging configuration is wrong!")
            LOG.exception(e)

