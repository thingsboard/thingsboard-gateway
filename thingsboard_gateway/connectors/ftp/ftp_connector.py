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

import time
from queue import Queue
from random import choice
from string import ascii_lowercase
from threading import Thread
from ftplib import FTP, FTP_TLS

from thingsboard_gateway.tb_utility.tb_utility import TBUtility
from thingsboard_gateway.tb_utility.tb_loader import TBModuleLoader

try:
    from requests import Timeout, request
except ImportError:
    print("Requests library not found - installing...")
    TBUtility.install_package("requests")
    from requests import Timeout, request

from thingsboard_gateway.connectors.connector import Connector, log


class FTPConnector(Connector, Thread):
    def __init__(self, gateway, config, connector_type):
        super().__init__()
        self.statistics = {'MessagesReceived': 0,
                           'MessagesSent': 0}
        self.__log = log
        self.__rpc_requests = []
        self.__config = config
        self.__connector_type = connector_type
        self.__gateway = gateway
        self.security = (self.__config["security"]["username"], self.__config["security"]["password"]) if \
            self.__config["security"]["type"] == 'basic' else ('anonymous', 'anonymous@')
        self.__tls_support = self.__config.get("TLSSupport", False)
        self.setName(self.__config.get("name", "".join(choice(ascii_lowercase) for _ in range(5))))
        self.daemon = True
        self.__stopped = False
        self.__requests_in_progress = []
        self.__convert_queue = Queue(1000000)
        self.__attribute_updates = []
        self._connected = False
        self.host = self.__config['host']
        self.port = self.__config.get('port', 21)
        self.__ftp = FTP_TLS if self.__tls_support else FTP

    def open(self):
        self.__stopped = False
        self.start()

    def run(self):
        try:
            with self.__ftp() as ftp:
                self.__connect(ftp)

        except Exception as e:
            self.__log.exception(e)
            try:
                self.close()
            except Exception as e:
                self.__log.exception(e)
        while True:
            if self.__stopped:
                break

    def __connect(self, ftp):
        try:
            ftp.connect(self.host, self.port)

            if isinstance(ftp, FTP_TLS):
                ftp.sendcmd('USER ' + self.security[0])
                ftp.sendcmd('PASS ' + self.security[1])
                ftp.prot_p()
                self.__log.info('Data protection level set to "private"')
            else:
                ftp.login(self.security[0], self.security[1])

        except Exception as e:
            self.__log.error(e)
            time.sleep(10)
        else:
            self._connected = True
            self.__log.info('FTP connected')

    def __process_paths(self):
        pass

    def close(self):
        self.__stopped = True

    def get_name(self):
        return self.name

    def is_connected(self):
        return self._connected

    def on_attributes_update(self, content):
        pass

    def server_side_rpc_handler(self, content):
        pass
