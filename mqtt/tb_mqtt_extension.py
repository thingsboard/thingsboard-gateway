from paho.mqtt.client import Client
from threading import Thread
from json import loads
from utility.tb_utility import TBUtility
from utility.tb_extension_base import TBExtension
from uuid import uuid1
import logging
from time import sleep
from queue import Queue
from importlib import import_module
from copy import deepcopy

log = logging.getLogger(__name__)
QUEUE_SLEEP_INTERVAL = 0.5


class TBMQTT(TBExtension):
    def __init__(self, conf, gateway, ext_id):
        super(TBMQTT, self).__init__(ext_id, gateway)
        host = TBUtility.get_parameter(conf, "host", "localhost")
        #todo check
        self.__is_connected = False
        port = TBUtility.get_parameter(conf, "port", 1883)
        ssl = TBUtility.get_parameter(conf, "ssl", False)
        keepalive = TBUtility.get_parameter(conf, "keep_alive", 60)
        # todo check if its implemented in client, mb yes
        # todo make  time in seconds??
        retry_interval = TBUtility.get_parameter(conf, "retry_interval", 3000) / 1000

        # todo fix, error is here! uuid has len()
        # client_id = TBUtility.get_parameter(conf, "client_id", uuid1())
        self.dict_handlers = {}
        for mapping in conf["mappings"]:
            topic_filter = mapping["topic_filter"]
            extension_module = import_module("extensions.mqtt." + mapping["handler"])
            extension_class = extension_module.Extension
            self.dict_handlers.update({topic_filter: [topic_filter.split(), extension_class]})
        # self.client = Client(client_id=client_id)
        self.client = Client()
        try:
            credentials = conf["credentials"]
            if credentials["type"] == "anonymous":
                pass

            elif credentials["type"] == "basic":
                username = credentials["username"]
                password = credentials["password"]
                self.client.username_pw_set(username, password)
                log.warning("basic cred type is not implemented yet")
                return

            if credentials["type"] == "cert.PEM":
                ca_cert = credentials["caCert"]
                private_key = credentials["privateKey"]
                cert = credentials["cert"]

                self.client.tls_set(ca_certs=ca_cert,
                                    certfile=cert,
                                    keyfile=private_key,
                                    cert_reqs=ssl.CERT_REQUIRED,
                                    tls_version=ssl.PROTOCOL_TLSv1_2,
                                    ciphers=None)
                self.client.tls_insecure_set(False)
        except KeyError as e:
            log.error("got error while parsing config file: {}. extension {}".format(e, ext_id))

        self.client._on_log = self._on_log
        self.client._on_connect = self._on_connect
        self.client._on_message = self._on_message

        self.decode_message_queue = Queue()
        self.decode_message_thread = Thread(target=self._on_decoded_message)
        self.decode_message_thread.daemon = True
        self.decode_message_thread.start()

        while not self.__is_connected:
            try:
                self.client.connect(host, port, keepalive)
            except Exception as e:
                log.error(e)
            log.debug("connecting to ThingsBoard... ext id {}".format(ext_id))
            sleep(1)
        self.client.loop_start()
        self.client.reconnect_delay_set(retry_interval, retry_interval)

    def _on_log(self, client, userdata, level, buf):
        log.debug("{}, extension {}".format(buf, self.ext_id))

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
            log.info("connection SUCCESS for extension {}".format(self.ext_id))

            self.client.subscribe(list((topic, 1) for topic in self.dict_handlers))
        else:
            if rc in result_codes:
                log.error("connection FAIL with error {} {}, extension {}".format(rc,
                                                                                  result_codes[rc],
                                                                                  self.ext_id))
            else:
                log.error("connection FAIL with unknown error, extension {}".format(self.ext_id))

    def _on_message(self, client, userdata, message):
        message.payload = loads(message.payload.decode("utf-8"))
        # log.debug(content)
        # log.debug(message.topic)
        self.decode_message_queue.put(message)

    def _on_decoded_message(self):
        def is_equivalent(mask, topic):
            len_mask = len(mask)
            if len(topic) >= len_mask:
                for n in range(len_mask):
                    if mask[n] == "+":
                        topic[n] = "+"
                    elif mask[n] == "#":
                        mask = mask[:n]
                        topic = topic[:n]
                        break
                if topic == mask:
                    return True
        while True:
            try:
                if self.decode_message_queue.qsize() > 0:
                    message = self.decode_message_queue.get(timeout=1)
                    topic_s = message.topic.split()
                    for topic_filter in self.dict_handlers:
                        topic_filter_as_list = self.dict_handlers[topic_filter][0]
                        tmp_topic = deepcopy(topic_s)
                        if is_equivalent(topic_filter_as_list, tmp_topic):
                            extension_instance = self.dict_handlers[topic_filter][1]()
                            result = extension_instance.convert_message_to_json_for_storage(message.topic,
                                                                                            message.payload)
                            if not result:
                                continue
                            for device_data in result:
                                telemetry = None
                                device_name = device_data["device_name"]
                                if device_data.get("telemetry"):
                                    telemetry = device_data.pop("telemetry")
                                if device_data.get("attributes"):
                                    atributes = device_data.pop("attributes")
                                    self.gateway.send_attributes_to_storage(atributes,
                                                                            device_name)
                                if telemetry:
                                    self.gateway.send_telemetry_to_storage(telemetry, device_name)

                else:
                    sleep(QUEUE_SLEEP_INTERVAL)
                    continue
            except Exception as e:
                sleep(QUEUE_SLEEP_INTERVAL)

