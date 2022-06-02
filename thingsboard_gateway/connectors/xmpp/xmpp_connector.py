#     Copyright 2022. ThingsBoard
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

from random import choice
from string import ascii_lowercase
from threading import Thread
from time import sleep

from thingsboard_gateway.connectors.connector import Connector, log


class XMPPConnector(Connector, Thread):
    def __init__(self, gateway, config, connector_type):
        self.statistics = {'MessagesReceived': 0,
                           'MessagesSent': 0}
        self.__log = log

        super().__init__()

        self._connector_type = connector_type
        self.__gateway = gateway
        self.__config = config

        self.setName(config.get("name", 'XMPP Connector ' + ''.join(choice(ascii_lowercase) for _ in range(5))))

        self.__stopped = False
        self._connected = False
        self.daemon = True

    def open(self):
        self.__stopped = False
        self.start()
        self.__log.info('Starting XMPP Connector')

    def run(self):
        while not self.__stopped:
            sleep(.2)

    def close(self):
        pass

    def get_name(self):
        return self.name

    def is_connected(self):
        pass

    def on_attributes_update(self, content):
        pass

    def server_side_rpc_handler(self, content):
        pass
