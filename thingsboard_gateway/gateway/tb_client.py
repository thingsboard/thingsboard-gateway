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
import threading
import logging
from ssl import SSLContext, PROTOCOL_TLSv1_2, CERT_REQUIRED
from os import path

from thingsboard_gateway.tb_client.tb_gateway_mqtt import TBGatewayMqttClient

log = logging.getLogger("tb_connection")


class TBClient(threading.Thread):
    def __init__(self, config, config_folder_path):
        super().__init__()
        self.setName('Connection thread.')
        self.daemon = True
        self.__config = config
        self.__host = config["host"]
        self.__port = config.get("port", 1883)
        self.__default_quality_of_service = config.get("qos", 1)
        credentials = config["security"]
        self.__min_reconnect_delay = 1
        self.__tls = bool(credentials.get('tls', False) or credentials.get('caCert', False))
        self.__ca_cert = None
        self.__private_key = None
        self.__cert = None
        self.__token = None
        self.__is_connected = False
        self.__stopped = False
        self.__paused = False
        if credentials.get("accessToken") is not None:
            self.__token = str(credentials["accessToken"])
        self.client = TBGatewayMqttClient(self.__host, self.__port, self.__token, self, quality_of_service=self.__default_quality_of_service)
        if self.__tls:
            self.__ca_cert = config_folder_path + credentials.get("caCert") if credentials.get("caCert") is not None else None
            self.__private_key = config_folder_path + credentials.get("privateKey") if credentials.get("privateKey") is not None else None
            self.__cert = config_folder_path + credentials.get("cert") if credentials.get("cert") is not None else None
            self.client._client.tls_set(ca_certs=self.__ca_cert,
                                        certfile=self.__cert,
                                        keyfile=self.__private_key,
                                        tls_version=PROTOCOL_TLSv1_2,
                                        cert_reqs=CERT_REQUIRED,
                                        ciphers=None)
            if (self.__ca_cert is not None and (self.__private_key is not None or self.__cert is not None)) or credentials.get("insecure", False):
                self.client._client.tls_insecure_set(False)
            else:
                self.client._client.tls_insecure_set(True)
        # if self.__tls and self.__ca_cert is None and self.__private_key is None and self.__cert is None:
            # pylint: disable=protected-access
        # Adding callbacks
        self.client._client._on_connect = self._on_connect
        self.client._client._on_disconnect = self._on_disconnect
        # self.client._client._on_log = self._on_log
        self.start()

    # def _on_log(self, *args):
    #     if "exception" in args[-1]:
    #         log.exception(args)
    #     else:
    #         log.debug(args)

    def pause(self):
        self.__paused = True

    def unpause(self):
        self.__paused = False

    def is_connected(self):
        return self.__is_connected

    def _on_connect(self, client, userdata, flags, result_code, *extra_params):
        log.debug('TB client %s connected to ThingsBoard', str(client))
        if result_code == 0:
            self.__is_connected = True
        # pylint: disable=protected-access
        self.client._on_connect(client, userdata, flags, result_code, *extra_params)

    def _on_disconnect(self, client, userdata, result_code):
        # pylint: disable=protected-access
        if self.client._client != client:
            log.info("TB client %s has been disconnected. Current client for connection is: %s", str(client), str(self.client._client))
            client.disconnect()
            client.loop_stop()
        else:
            self.__is_connected = False
            self.client._on_disconnect(client, userdata, result_code)

    def stop(self):
        # self.disconnect()
        self.client.stop()
        self.__stopped = True

    def disconnect(self):
        self.__paused = True
        self.unsubscribe('*')
        self.client.disconnect()

    def unsubscribe(self, subsription_id):
        self.client.gw_unsubscribe(subsription_id)
        self.client.unsubscribe_from_attribute(subsription_id)

    def connect(self, min_reconnect_delay=10):
        self.__paused = False
        self.__stopped = False
        self.__min_reconnect_delay = min_reconnect_delay

    def run(self):
        keep_alive = self.__config.get("keep_alive", 120)
        try:
            while not self.client.is_connected() and not self.__stopped:
                if not self.__paused:
                    if self.__stopped:
                        break
                    log.debug("connecting to ThingsBoard")
                    try:
                        self.client.connect(keepalive=keep_alive,
                                            min_reconnect_delay=self.__min_reconnect_delay)
                    except ConnectionRefusedError:
                        pass
                    except Exception as e:
                        log.exception(e)
                time.sleep(1)
        except Exception as e:
            log.exception(e)
            time.sleep(10)

        while not self.__stopped:
            try:
                if not self.__stopped:
                    time.sleep(.1)
                else:
                    break
            except KeyboardInterrupt:
                self.__stopped = True
            except Exception as e:
                log.exception(e)

