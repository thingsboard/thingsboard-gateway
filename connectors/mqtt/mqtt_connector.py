import time
from paho.mqtt.client import Client
from connectors.connector import Connector
from threading import Thread
from tb_utility.tb_utility import TBUtility
from uuid import uuid1
import logging

log = logging.getLogger(__name__)


class MqttConnector(Connector,Thread):
    def __init__(self, gateway, config):
        super().__init__()
        self.__broker = config.get('broker')
        self.__mapping = config.get('mapping')
        self._client = Client(client_id=str(uuid1()))
        if self.__broker["credentials"]["type"] == "basic":
            self._client.username_pw_set(self.__broker["credentials"]["username"], self.__broker["credentials"]["password"])

        self._client.on_connect = self._on_connect
        self._client.on_message = self._on_message
        self._client.on_subscribe = self._on_subscribe
        self._client.on_disconnect = self._on_disconnect
        self._connected = False
        self.name = TBUtility.get_parameter(self.__broker,"name",self.__broker['host'])
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
        self.start()

    def run(self):
        while True:
            time.sleep(.1)

    def close(self):
        self._client.loop_stop()
        self._client.disconnect()

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
            # self._client.subscribe(ATTRIBUTES_TOPIC, qos=1)
            # self._client.subscribe(ATTRIBUTES_TOPIC + "/response/+", 1)
            # self._client.subscribe(RPC_REQUEST_TOPIC + '+')
            # self._client.subscribe(RPC_RESPONSE_TOPIC + '+', qos=1)
            self.__sub_topics = {}
            for mapping in self.__mapping:
                self.__sub_topics[mapping["topicFilter"]] = mapping["converter"]
                self._client.subscribe(mapping["topicFilter"])
                log.info(f'Subscribe to {mapping["topicFilter"]}')
            # self._client.publish("v1/devices/me/attributes/request/3",'{"sharedKeys":"asc"}')
        else:
            if rc in result_codes:
                log.error("connection FAIL with error {rc} {explanation}".format(rc=rc,
                                                                                 explanation=result_codes[rc]))
            else:
                log.error("connection FAIL with unknown error")

    def _on_disconnect(self):
        log.debug(self.name + 'was disconnected, trying to reconnect.')

    def _on_subscribe(self, client, userdata, mid, granted_qos):
        log.debug("Subscribtion QOS = " + str(granted_qos[0]))

    def _on_message(self, client, userdata, message):
        log.debug("Received message")
        content = message.decode("UTF-8")
        log.debug(content)

    @staticmethod
    def _decode(message):
        content = loads(message.payload.decode("utf-8"))
        log.debug(content)
        log.debug(message.topic)
        return content
