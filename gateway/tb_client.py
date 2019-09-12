import logging
import json
import time

from tb_client.tb_gateway_mqtt import TBGatewayMqttClient
from tb_utility.tb_utility import TBUtility

log = logging.getLogger(__name__)


class TBClient:
    def __init__(self, config):
        self.__config = config
        self.__host = config["host"]
        self.__port = TBUtility.get_parameter(config, "port", 1883)
        credentials = config["security"]
        token = credentials["token"]
        self.client = TBGatewayMqttClient(self.__host, self.__port, token, self)

    def is_connected(self):
        return self.client.is_connected

    def connect(self):
        keep_alive = TBUtility.get_parameter(self.__config, "keep_alive", 60)

        while not self.client.is_connected():
            try:
                self.client.connect(keep_alive)
            except Exception as e:
                log.error(e)
            log.debug("connecting to ThingsBoard...")
            time.sleep(1)

