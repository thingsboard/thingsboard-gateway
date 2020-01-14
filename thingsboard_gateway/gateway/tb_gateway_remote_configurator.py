#     Copyright 2019. ThingsBoard
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

# from threading import Thread
from simplejson import dumps, loads, dump
from yaml import safe_dump
from time import time, sleep
from logging import getLogger
from os import remove
from thingsboard_gateway.gateway.tb_client import TBClient

log = getLogger("service")


class RemoteConfigurator:
    def __init__(self, gateway, config):
        self.__gateway = gateway
        self.__new_configuration = None
        self.__apply_timeout = 10
        self.__old_tb_client = None
        self.__old_connectors_configs = {}
        self.__new_connectors_configs = {}
        self.__old_general_configuration_file = config
        self.__new_general_configuration_file = {}

    def process_configuration(self, configuration):
        log.info("Remote configuration received: \n %s", dumps(configuration))
        self.__new_configuration = loads(configuration)
        self.__old_connectors_configs = self.__gateway._connectors_configs
        self.__new_general_configuration_file = self.__new_configuration.get("thingsboard")
        self.__process_general_configuration()
        self.__process_connectors_configuration()

    def send_current_configuration(self):
        current_configuration = {}
        for connector in self.__gateway._connectors_configs:
            log.debug(connector)
            if current_configuration.get(connector) is None:
                current_configuration[connector] = []
            for config_file in self.__gateway._connectors_configs[connector]:
                for config in config_file:
                    current_configuration[connector].append(config_file[config])
        current_configuration["thingsboard"] = self.__old_general_configuration_file
        self.__gateway.tb_client.client.send_attributes(dumps({"configuration": dumps(current_configuration)}))
        log.debug(current_configuration)


    def __process_general_configuration(self):
        # TODO Add remote configuration for the general configuration file
        if self.__new_general_configuration_file is not None and self.__old_general_configuration_file != self.__new_general_configuration_file:
            log.debug("New general configuration found.")
        else:
            log.debug("General configuration from server is the same like current gateway general configuration.")

    def __process_connectors_configuration(self):
        log.debug("Processing remote connectors configuration...")
        self.__prepare_connectors_configuration()
        if self.__apply_new_configuration():
            self.__write_new_configuration_files()

    def __prepare_connectors_configuration(self):
        self.__new_connectors_configs = {}
        try:
            for connector_type in {connector_type for connector_type in self.__new_configuration if "thingsboard" not in connector_type}:
                connector_number = 0
                for connector in self.__new_configuration[connector_type]:
                    log.debug(connector)
                    log.debug("Processing remote configuration for connector with type \"%s\" and name \"%s\".", connector_type, connector)
                    if not self.__new_connectors_configs.get(connector_type):
                        self.__new_connectors_configs[connector_type] = []
                    self.__new_connectors_configs[connector_type].append({connector_type.lower().replace(" ", "")+str(connector_number)+".json": connector})
                    connector_number += 1
            log.debug("Saved configuration for connectors: %s", ', '.join(con for con in self.__new_connectors_configs))

        except Exception as e:
            log.exception(e)

    def __apply_new_configuration(self):
        try:
            self.__gateway._connectors_configs = self.__new_connectors_configs
            for connector_name in self.__gateway.available_connectors:
                self.__gateway.available_connectors[connector_name].close()
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
            general_edited = False
            if self.__new_general_configuration_file and self.__new_general_configuration_file != self.__old_general_configuration_file:
                general_edited = False
                if self.__new_general_configuration_file["thingsboard"] != self.__old_general_configuration_file["thingsboard"]:
                    general_edited = True
                if self.__new_general_configuration_file["storage"] != self.__old_general_configuration_file["storage"]:
                    general_edited = True
            self.__new_general_configuration_file = self.__new_general_configuration_file if general_edited else self.__old_general_configuration_file
            self.__new_connectors_configs = self.__new_connectors_configs if self.__new_connectors_configs else self.__new_connectors_configs
            self.__new_general_configuration_file["connectors"] = []
            new_connectors_files = []
            for connector_type in self.__new_connectors_configs:
                for connector_config_section in self.__new_connectors_configs[connector_type]:
                    for connector_file in connector_config_section:
                        connector_config = connector_config_section[connector_file]
                        connector_name = connector_config["broker"]["name"] if connector_config.get("broker") else \
                                         connector_config["server"]["name"] if connector_config.get("server") else \
                                         connector_config["name"] if connector_config.get("name") else None
                        self.__new_general_configuration_file["connectors"].append(
                            {
                                "name": connector_name,
                                "type": connector_type,
                                "configuration": connector_file
                            }
                        )
                        with open(self.__gateway._config_dir + connector_file, "w") as config_file:
                            dump(connector_config, config_file, sort_keys=True)
                        new_connectors_files.append(connector_file)
                        log.debug("Saving new configuration for \"%s\" connector to file \"%s\"", connector_type, connector_file)
            for old_connector_type in self.__old_connectors_configs:
                for old_connector_config_section in self.__old_connectors_configs[old_connector_type]:
                    for old_connector_file in old_connector_config_section:
                        if old_connector_file not in new_connectors_files:
                            remove(self.__gateway._config_dir + old_connector_file)
                        log.debug("Remove old configuration file \"%s\" for \"%s\" connector ", old_connector_file, old_connector_type)
            if not general_edited:
                with open(self.__gateway._config_dir+"tb_gateway.yaml", "w") as general_configuration_file:
                    safe_dump(self.__new_general_configuration_file, general_configuration_file)
                self.__old_connectors_configs = {}
                self.__new_connectors_configs = {}
            else:
                if self.safe_apply():
                    log.info("A new configuration has been applied.")
                    with open(self.__gateway._config_dir+"tb_gateway.yaml", "w") as general_configuration_file:
                        safe_dump(self.__new_general_configuration_file, general_configuration_file)
                    self.__old_connectors_configs = {}
                    self.__new_connectors_configs = {}
                    self.__old_general_configuration_file = self.__new_general_configuration_file
                    self.__new_general_configuration_file = {}
                else:
                    log.error("A new configuration applying has been failed.")
                    self.__old_connectors_configs = {}
                    self.__new_connectors_configs = {}
                    self.__new_general_configuration_file = {}
        except Exception as e:
            log.exception(e)

    def safe_apply(self):
        # TODO Add check for connection to the ThingsBoard and revert configuration on fail in timeout
        apply_start = time()*1000
        self.__old_tb_client = self.__gateway.tb_client
        try:
            self.__old_tb_client.pause()
        except Exception as e:
            log.exception(e)
            self.__revert_configuration()
            return False
        connection_state = False
        try:
            tb_client = TBClient(self.__new_general_configuration_file["thingsboard"])
            tb_client.connect()
        except Exception as e:
            log.exception(e)
            self.__revert_configuration()
            return False
        self.__gateway.tb_client = tb_client
        try:

            while time()*1000-apply_start < self.__apply_timeout*1000:
                connection_state = self.__gateway.tb_client.is_connected()
                sleep(.1)
            if not connection_state:
                self.__revert_configuration()
                log.info("The gateway cannot connect to the ThingsBoard server with a new configuration.")
                return False
            else:
                self.__old_tb_client.stop()
                return True
        except Exception as e:
            log.exception(e)
            self.__revert_configuration()
            return False

    def __revert_configuration(self):
        log.info("Configuration will be restored.")
        self.__new_general_configuration_file = self.__old_general_configuration_file
        self.__gateway.tb_client.disconnect()
        self.__gateway.tb_client.stop()
        self.__gateway.tb_client = self.__old_tb_client
        self.__gateway.tb_client.connect()
        self.__gateway.tb_client.unpause()


