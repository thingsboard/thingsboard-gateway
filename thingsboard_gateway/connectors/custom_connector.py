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

import logging
from random import choice
from string import ascii_lowercase
from threading import Thread
from thingsboard_gateway.connectors.connector import Connector
from thingsboard_gateway.tb_utility.tb_utility import TBUtility
from abc import abstractmethod

log = logging.getLogger('extension')


class CustomConnector(Connector, Thread):
    def __init__(self, gateway, config, connector_type):
        super().__init__()
        self.statistics = {'MessagesReceived': 0,
                           'MessagesSent': 0}
        self.__connector_type = connector_type
        self.__config = config
        self.__gateway = gateway
        self.setName(self.__config.get("name",
                                       "Custom %s connector " % self.get_name() + ''.join(choice(ascii_lowercase) for _ in range(5))))
        log.info("Starting Custom %s connector", self.get_name())
        self.daemon = True
        self.stopped = True
        self.connected = False
        self.devices = {}
        self.load_converters()
        log.info('Custom connector %s initialization success.', self.get_name())
        log.info("Devices in configuration file found: %s ", '\n'.join(device for device in self.devices))

    def open(self):
        self.stopped = False
        self.start()

    def close(self):
        self.stopped = True

    def get_name(self):
        return self.name

    def is_connected(self):
        return self.connected

    def load_converters(self):
        devices_config = self.__config.get('devices')
        try:
            if devices_config is not None:
                for device_config in devices_config:
                    if device_config.get('converter') is not None:
                        converter = TBUtility.check_and_import(self.__connector_type, device_config['converter'])
                        self.devices[device_config['name']] = {'converter': converter(device_config),
                                                               'device_config': device_config}
                    else:
                        log.error('Converter configuration for the custom connector %s -- not found, please check your configuration file.', self.get_name())
            else:
                log.error('Section "devices" in the configuration not found. A custom connector %s has being stopped.', self.get_name())
                self.close()
        except Exception as e:
            log.exception(e)

    @abstractmethod
    def on_attributes_update(self, content):
        pass

    @abstractmethod
    def server_side_rpc_handler(self, content):
        pass
