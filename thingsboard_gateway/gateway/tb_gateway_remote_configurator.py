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
from yaml import safe_load, safe_dump
from copy import deepcopy
from logging import getLogger
from os import remove

log = getLogger("service")

class RemoteConfigurator():
    def __init__(self, gateway, config):
        self.__gateway = gateway
        self.__new_configuration = None
        self.__old_connectors_configs = {}
        self.__new_connectors_configs = {}
        self.__old_general_configuration_file = config
        self.__new_general_configuration_file = {}

    def process_configuration(self, configuration):
        log.info("Remote configuration received: \n %s", dumps(configuration))
        self.__new_configuration = loads(configuration)
        self.__old_connectors_configs = self.__gateway._connectors_configs
        self.__process_general_configuration()
        self.__process_connectors_configuration()

    def __process_general_configuration(self):
        # TODO Add remote configuration for the general configuration file
        pass

    def __process_connectors_configuration(self):
        log.debug("Processing remote connectors configuration...")
        self.__prepare_connectors_configuration()
        if self.__apply_new_connectors_configuration():
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

    def __apply_new_connectors_configuration(self):
        try:
            self.__gateway._connectors_configs = self.__new_connectors_configs
            for connector_name in self.__gateway.available_connectors:
                self.__gateway.available_connectors[connector_name].close()
            self.__gateway._connect_with_connectors()
            log.debug("New connectors configuration has been applied")
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
                            dump(connector_config, config_file)
                        new_connectors_files.append(connector_file)
                        log.debug("Saving new configuration for \"%s\" connector to file \"%s\"", connector_type, connector_file)
            for old_connector_type in self.__old_connectors_configs:
                for old_connector_config_section in self.__old_connectors_configs[old_connector_type]:
                    for old_connector_file in old_connector_config_section:
                        if old_connector_file not in new_connectors_files:
                            remove(self.__gateway._config_dir+old_connector_file)
                        log.debug("Remove old configuration file \"%s\" for \"%s\" connector ", old_connector_file, old_connector_type)
            if not general_edited:
                with open(self.__gateway._config_dir+"tb_gateway.yaml", "w") as general_configuration_file:
                    safe_dump(self.__new_general_configuration_file, general_configuration_file)
            else:
                self.safe_apply()
        except Exception as e:
            log.exception(e)

    def safe_apply(self):
        # TODO Add check for connection to the ThingsBoard and revert configuration on fail in timeout
        pass
