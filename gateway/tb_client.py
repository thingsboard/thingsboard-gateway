import logging
import time

from tb_client.tb_gateway_mqtt import TBGatewayMqttClient
from tb_utility.tb_utility import TBUtility

log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)


class TBClient:
    def __init__(self, config):
        self.__config = config
        self.__host = config["host"]
        self.__port = TBUtility.get_parameter(config, "port", 1883)
        credentials = config["security"]
        token = str(credentials["accessToken"])
        self._client = TBGatewayMqttClient(self.__host, self.__port, token, self)
        # Adding callbacks
        self._client._client._on_connect = self._on_connect
        # self._client._client._on_message = self._on_message
        self._client._client._on_disconnect = self._on_disconnect

    def is_connected(self):
        return self._client.is_connected

    def _on_connect(self, client, userdata, flags, rc, *extra_params):
        log.debug('Gateway connected to ThingsBoard')
        self._client._on_connect(client, userdata, flags, rc, *extra_params)

    def _on_disconnect(self, client, userdata, rc):
        log.info('Gateway was disconnected trying to reconnect')

    def disconnect(self):
        self._client.disconnect()

    def connect(self):
        keep_alive = TBUtility.get_parameter(self.__config, "keep_alive", 60)

        while not self._client.is_connected():
            try:
                self._client.connect(keepalive=keep_alive)
            except Exception as e:
                log.error(e)
            log.debug("connecting to ThingsBoard...")
            time.sleep(1)

