from paho.mqtt.client import Client
from threading import Thread
from tb_utility.tb_utility import TBUtility
import logging

log = logging.getLogger(__name__)


class MqttConnector(Thread):
    def __init__(self, gateway, config):
        self.__config = config
        self.__port = TBUtility.get_parameter(self.__config, 'port', 1883)
        self.__host = TBUtility.get_parameter(self.__config, 'host', 'localhost')
        log.debug(self.__config)


    def open(self):
        pass

    def close(self):
        pass
