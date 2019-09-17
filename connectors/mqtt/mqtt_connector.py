import time
import logging
import string
import random
from importlib import import_module
from paho.mqtt.client import Client
from connectors.connector import Connector
from connectors.mqtt.json_mqtt_uplink_converter import JsonMqttUplinkConverter
from threading import Thread
from tb_utility.tb_utility import TBUtility
from json import loads, dumps

log = logging.getLogger(__name__)


class MqttConnector(Connector,Thread):
    def __init__(self, gateway, config):
        super().__init__()
        self.__gateway = gateway
        self.__broker = config.get('broker')
        self.__mapping = config.get('mapping')
        self.__sub_topics = {}
        client_id = ''.join(random.choice(string.ascii_lowercase) for _ in range(23))
        self._client = Client(client_id)
        self.__name = TBUtility.get_parameter(self.__broker,"name", str(self.__broker["host"] + ":" + str(self.__broker.get("port"))))
        if self.__broker["credentials"]["type"] == "basic":
            self._client.username_pw_set(self.__broker["credentials"]["username"], self.__broker["credentials"]["password"])

        self._client.on_connect = self._on_connect
        self._client.on_message = self._on_message
        self._client.on_subscribe = self._on_subscribe
        self._client.on_disconnect = self._on_disconnect
        self._connected = False
        self.__stopped = False
        self.daemon = True

    def open(self):
        try:
            while not self._connected:
                try:
                    self._client.connect(self.__broker['host'],TBUtility.get_parameter(self.__broker,'port',1883))
                    self._client.loop_start()
                except Exception as e:
                    log.error(e)
                time.sleep(1)

        except Exception as e:
            log.error(e)
        self.__stopped = False
        self.start()

    def run(self):
        while True:
            time.sleep(.1)
            if self.__stopped:
                break

    def close(self):
        self._client.loop_stop()
        self._client.disconnect()
        self.__stopped = True
        log.debug(f'{self.getName()} has been stopped.')


    def getName(self):
        return self.__name

    def _on_connect(self, client, userdata, flags, rc, *extra_params):
        result_codes = {
            1: "incorrect protocol version",
            2: "invalid client identifier",
            3: "server unavailable",
            4: "bad username or password",
            5: "not authorised",
        }
        if rc == 0:
            self._connected = True
            log.debug(f'Connected to {self.__broker["host"]}:{TBUtility.get_parameter(self.__broker, "port", "1883")} - successfully')
            for mapping in self.__mapping:
                try:
                    log.debug(mapping)
                    if mapping["converter"]["type"] == "custom":
                        converter = 1
                        # extension_name = 'connectors.mqtt.' + mapping["converter"]["extension"] # TODO load custom extension
                        # converter = import_module(extension_name)
                    else:
                        converter = JsonMqttUplinkConverter(mapping)
                    self.__sub_topics[mapping["topicFilter"]] = {converter: None}
                    self._client.subscribe(mapping["topicFilter"])
                    log.info(f'Subscribe to {mapping["topicFilter"]}')
                except Exception as e:
                    log.error(e)

        else:
            if rc in result_codes:
                log.error("{connector} connection FAIL with error {rc} {explanation}".format(connector=self.getName(),
                                                                                    rc=rc,
                                                                                    explanation=result_codes[rc]))
            else:
                log.error(f"{self.getName()} connection FAIL with unknown error")

    def _on_disconnect(self):
        log.debug(self.getName() + ' was disconnected.')

    def _on_subscribe(self, client, userdata, mid, granted_qos):
        if granted_qos[0] == 128:
            log.debug("Subscribtion failed check your configs")


    def _on_message(self, client, userdata, message):
        log.debug("Received message")
        content = self._decode(message)
        log.debug(content)
        log.debug(message.topic)
        converter = self.__sub_topics.get(message.topic)
        if converter:
            self.__sub_topics.get(message.topic)[converter]= converter(content)
        else:
            log.error(f'Cannot find converter for topic:"{message.topic}"')

    @staticmethod
    def _decode(message):
        content = loads(message.payload.decode("utf-8"))
        return content
