from paho.mqtt.client import Client
from threading import Thread
from json import loads
from tb_device_mqtt import TBDeviceMqttClient
from tb_utility import TBUtility
from uuid import uuid1
import logging
from time import sleep
from queue import Queue, Empty
from importlib import import_module
from copy import deepcopy

log = logging.getLogger(__name__)
QUEUE_SLEEP_INTERVAL = 0.5


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
        self.dict_handlers = {}
        for mapping in conf["mappings"]:
            topic_filter = mapping["topicFilter"]
            extension_module = import_module("extensions.mqtt." + mapping["handler"])
            # todo remove
            extension_class = extension_module.Extension
            self.dict_handlers.update({topic_filter: [topic_filter.split(), extension_class]})
            log.critical("++++++++++++++++++++++")
        self.client = Client()
        self.client._on_log = self._on_log
        self.client._on_connect = self._on_connect
        self.client._on_message = self._on_message

        # todo implement max_size?
        self.decode_message_queue = Queue()
        self.decode_message_thread = Thread(target=self._on_decoded_message)
        self.decode_message_thread.daemon = True
        self.decode_message_thread.start()

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
            self.client.subscribe(list((topic, 1) for topic in self.dict_handlers))
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
                message = self.decode_message_queue.get(timeout=1)
                log.critical(1234)
            except Exception as e:
                sleep(QUEUE_SLEEP_INTERVAL)
                continue
            topic_s = message.topic.split()
            for topic_filter in self.dict_handlers:
                topic_filter_as_list = self.dict_handlers[topic_filter][0]
                tmp_topic = deepcopy(topic_s)
                if is_equivalent(topic_filter_as_list, tmp_topic):
                    log.critical("++++++++++++++++++++++++++++++++++++++++++++++++++++")
                    exctension_instance = self.dict_handlers[topic_filter][1]()
                    result = exctension_instance.convert_message_to_json_for_storage(message.topic,
                                                                                     message.payload)
                    # todo this is test version
                    if result[0] == "to_storage":
                        self.gateway.send_data_to_storage(data=result[1][0],
                                                          type_of_data=result[1][1],
                                                          device=result[1][2])
                    else:
                        log.warning("'to storage' is only available now")
                    # todo for logging, remove
            #todo make proper sleep with dynamic interval
            sleep(QUEUE_SLEEP_INTERVAL)

            #todo find a nd add try except where needed!!!
