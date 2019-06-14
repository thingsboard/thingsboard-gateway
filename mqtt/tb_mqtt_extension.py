from paho.mqtt.client import Client
from threading import Thread
from json import loads
from tb_device_mqtt import TBDeviceMqttClient
from tb_utility import TBUtility
from uuid import uuid1
import logging
log = logging.getLogger(__name__)


class TBMQTT(Thread):
    def __init__(self, conf, gateway, ext_id):
        super(TBMQTT, self).__init__()
        self.daemon = True
        # todo implement ext_id in logging
        self.ext_id = ext_id
        self.gateway = gateway
        self.start()
        host = TBUtility.get_parameter(conf, "host", "localhost")
        port = TBUtility.get_parameter(conf, "port", 1883)
        ssl = TBUtility.get_parameter(conf, "ssl", False)
        keepalive = TBUtility.get_parameter(conf, "keepAlive", 60)
        # todo check if its implemented in client, mb yes
        # todo make  time in seconds??
        retryInterval = TBUtility.get_parameter(conf, "retryInterval", 3000) / 100
        credentials = conf["credentials"]
        # todo add other types
        if credentials["type"] == "anonymous":
            pass
        elif credentials["type"] == "basic":
            log.warning("basic cred type is not implemented yet")
            return
        if credentials["type"] == "cert.PEM":
            log.warning("cert pem type is not implemented yet")
            return
        client_id = TBUtility.get_parameter(conf, "clientId", uuid1())

        for mapping in conf["mappings"]:
            topicFiler = mapping["topicFilter"]

        self.client = Client()
        self.client._on_log = self._on_log
        self.client._on_connect = self._on_connect
        self.client._on_message = self._on_message

        # todo tls goes here
        self.client.connect(host, port, keepalive)
        self.client.loop_start()
        # todo check
        self.client.reconnect_delay_set(retryInterval, retryInterval)
        # todo add callbacks like on_connect
        # create and connect client
        # subscribe
        # todo implement wildcard subscription

    def _on_log(self, client, userdata, level, buf):
        log.debug(buf)

    def _on_connect(self, client, userdata, flags, rc, *extra_params):
        result_codes = {
            1: "incorrect protocol version",
            2: "invalid client identifier",
            3: "server unavailable",
            4: "bad username or password",
            5: "not authorised",
        }

        if rc == 0:
            self.__is_connected = True
            log.info("connection SUCCESS")

            # todo subscribe to everything from json
            self.client.subscribe("sensors/+/", qos=1)
        else:
            if rc in result_codes:
                # todo log with ext_id?
                log.error("connection FAIL with error {} {}, extension {}".format(rc,
                                                                                  result_codes[rc],
                                                                                  self.ext_id))
            else:
                log.error("connection FAIL with unknown error, extension {}".format(self.ext_id))

    def _on_message(self, client, userdata, message):
        message.payload = loads(message.payload.decode("utf-8"))
        # log.debug(content)
        # log.debug(message.topic)
        # todo put in queue and work with in another Thread
        self._on_decoded_message(message)


    def _on_decoded_message(self, message):
        for item in message.topic.split():

            pass
        pass
