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
import time
from thingsboard_gateway.tb_client.tb_gateway_mqtt import TBGatewayMqttClient
import threading

log = logging.getLogger("tb_connection")


class TBClient(threading.Thread):
    def __init__(self, config):
        super().__init__()
        self.setName('Connection thread.')
        self.daemon = True
        self.__config = config
        self.__host = config["host"]
        self.__port = config.get("port", 1883)
        credentials = config["security"]
        self.__min_reconnect_delay = 10
        self.__tls = True if credentials.get('caCert', False) else False
        self.__ca_cert = None
        self.__private_key = None
        self.__cert = None
        self.__token = None
        self.__is_connected = False
        self.__stopped = False
        if credentials.get("accessToken") is not None:
            self.__token = str(credentials["accessToken"])
        if self.__tls:
            self.__ca_cert = credentials.get("caCert")
            self.__private_key = credentials.get("privateKey")
            self.__cert = credentials.get("cert")
        self.client = TBGatewayMqttClient(self.__host, self.__port, self.__token, self)
        # Adding callbacks
        self.client._client._on_connect = self._on_connect
        self.client._client._on_disconnect = self._on_disconnect
        self.client._client._on_log = self._on_log
        self.start()

    def _on_log(self, *args):
        log.debug(args)

    def is_connected(self):
        return self.client.is_connected()

    def _on_connect(self, client, userdata, flags, rc, *extra_params):
        log.debug('Gateway connected to ThingsBoard')
        self.client._on_connect(client, userdata, flags, rc, *extra_params)

    def _on_disconnect(self, client, userdata, rc):
        log.info('Gateway disconnected.')
        self.client._on_disconnect(client, userdata, rc)

    def disconnect(self):
        self.client.disconnect()

    def connect(self, min_reconnect_delay=10):
        self.__min_reconnect_delay = min_reconnect_delay

    def run(self):
        keep_alive = self.__config.get("keep_alive", 60)
        try:
            while not self.client.is_connected() and not self.__stopped:
                log.debug("connecting to ThingsBoard")
                try:
                    self.client.connect(tls=self.__tls,
                                        ca_certs=self.__ca_cert,
                                        cert_file=self.__cert,
                                        key_file=self.__private_key,
                                        keepalive=keep_alive,
                                        min_reconnect_delay=self.__min_reconnect_delay)
                except ConnectionRefusedError:
                    pass
                time.sleep(1)
        except Exception as e:
            log.exception(e)
            time.sleep(10)

        while True:
            try:
                if not self.__stopped:
                    time.sleep(1)
            except KeyboardInterrupt:
                self.__stopped = True
            except Exception as e:
                log.exception(e)

