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
from threading import RLock

log = logging.getLogger(__name__)
QUEUE_SLEEP_INTERVAL = 0.5


class TB_MQTT_Extension(TBExtension):
    class Callback:
        def __init__(self, ext, id, device_name, atr_name, mqtt_ext_id):
            self.mqtt_ext = ext
            self.request_id = id
            self.mqtt_ext_id = mqtt_ext_id
            self.device = device_name
            self.atr = atr_name

        def __call__(self, request_result, exception):
            if not exception:
                self.mqtt_ext.gw_send_rpc_reply(self.device,
                                                self.request_id,
                                                {self.atr: request_result["value"]})
            else:
                log.error("{} {}".format(exception, self.mqtt_ext_id))

    def __init__(self, conf, gateway, ext_id):
        super(TB_MQTT_Extension, self).__init__(ext_id, gateway)
        self.__device_client_rpc_number = 0
        self.__device_client_rpc_dict = {}
        host = TBUtility.get_parameter(conf, "host", "localhost")
        self.__is_connected = False
        self.lock = RLock()
        self._connected_devices = set()
        port = TBUtility.get_parameter(conf, "port", 1883)
        ssl = TBUtility.get_parameter(conf, "ssl", False)
        keep_alive = TBUtility.get_parameter(conf, "keep_alive", 60)
        # todo make  time in seconds??
        retry_interval = TBUtility.get_parameter(conf, "retry_interval", 3000) / 1000

        # todo fix, error is here! uuid has len()
        # client_id = TBUtility.get_parameter(conf, "client_id", uuid1())
        self.telemetry_atrs_dict_handlers = {}
        for mapping in conf["mappings"]:
            topic_filter = mapping["topic_filter"]
            extension_module = import_module("extensions.mqtt." + mapping["handler"])
            extension_class = extension_module.Extension
            self.telemetry_atrs_dict_handlers.update({topic_filter: [topic_filter.split(), extension_class]})
        # todo add client id
        # self.client = Client(client_id=client_id)
        # todo
        
        self.attribute_requests_handler_dict = {}
        if conf.get("attribute_requests"):
            for attribute_request in conf["attribute_requests"]:
                topic_filter = attribute_request["topic_filter"]
                extension_module = import_module("extensions.mqtt." + attribute_request["handler"])
                extension_class = extension_module.Extension
                self.attribute_requests_handler_dict.update({topic_filter: [topic_filter.split(), extension_class]})
        self._rpc_handler_by_method_name_dict = {}
        if conf.get("server_side_rpc"):
            for rpc_handler in conf["server_side_rpc"]:
                pass
        self.client = Client()
        try:
            credentials = conf["credentials"]
            credentials_type = credentials["type"]
            if ssl:
                ca_cert = credentials["caCert"]
                self.client.tls_set(ca_cert)
                self.client.tls_insecure_set(False)

            if credentials_type == "anonymous":
                pass

            elif credentials_type == "basic":
                username = credentials["username"]
                password = credentials["password"]
                self.client.username_pw_set(username, password)

            elif credentials_type == "cert.PEM":
                ca_cert = credentials["caCert"]
                private_key = credentials["privateKey"]
                cert = credentials["cert"]

                # todo add protocol version support
                self.client.tls_set(ca_certs=ca_cert,
                                    certfile=cert,
                                    keyfile=private_key,
                                    cert_reqs=ssl.CERT_REQUIRED,
                                    ciphers=None)
                self.client.tls_insecure_set(False)
            else:
                log.error("unknown credentials type: {}".format(credentials_type))
                return
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
                self.client.connect(host, port, keep_alive)
                self.client.loop_start()
                self.client.reconnect_delay_set(retry_interval)
            except Exception as e:
                log.error(e)
            log.debug("connecting to ThingsBoard... ext id {}".format(ext_id))
            sleep(1)

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
            self.client.subscribe("#")
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


    class RPC_Callback:
        def __init__(self):
            pass

    def __handler(self, request_body):

        device = request_body["device"]
        req_id = request_body["data"]["id"]
        method = request_body["data"]["method"]

        # self._rpc_handler_by_method_name_dict[method]

        # import some
        # execute
        # get topic + payload from it
        # publish
        # if we dont expect answer fuch this shit im out!

        # else(
        # add handler for
        # hanlder should



        #todo add
        rpc_request_topic, payload = None, None
        # self.validate(RPC_VALIDATOR, params)
        # todo add
        # with self.lock:
        #     self.__device_client_rpc_number += 1
        #     self.__device_client_rpc_dict.update({self.__device_client_rpc_number: callback})
        #     rpc_request_id = self.__device_client_rpc_number
        # payload = {"method": method, "params": params}
        # self.client.publish(rpc_request_topic + str(rpc_request_id),
        #                      dumps(payload),
        #                      qos=1)

        pass

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
                    topic_s = message.topic.split("/")
                    # server-side rpc case
                    # todo add
                    last_word = topic_s[-1]
                    # attribute request case
                    is_atr_request = False
                    for topic_filter in self.attribute_requests_handler_dict:
                        topic_filter_as_list = self.attribute_requests_handler_dict[topic_filter][0]
                        for topic in topic_filter_as_list:
                            tmp_filter = topic.split("/")
                            tmp_topic = deepcopy(topic_s)
                            if is_equivalent(tmp_filter, tmp_topic):
                                extension_instance = self.attribute_requests_handler_dict[topic_filter][1]()
                                result, id = extension_instance.convert_message_to_atr_request(message.topic,
                                                                                               message.payload)
                                if not result:
                                    continue
                                for item in result:  # result = {}
                                    device_name = item["device_name"]

                                    ext = self
                                    if item.get("client_keys"):
                                        # is_atr_request = True
                                        for key in item["client_keys"]:
                                            callback = TB_MQTT_Extension.Callback(ext, id, device_name, key, self.ext_id)
                                            self.gateway.mqtt_gateway.gw_request_client_attributes(device_name,
                                                                                                   key,
                                                                                                   callback)

                                    if item.get("shared_keys"):
                                        for key in item["shared_keys"]:
                                            # is_atr_request = True
                                            callback = TB_MQTT_Extension.Callback(ext, id, device_name, key, self.ext_id)
                                            self.gateway.mqtt_gateway.gw_request_shared_attributes(device_name,
                                                                                                   item["shared_keys"],
                                                                                                   callback)
                                    #todo add else log.warning?
                                # todo add gateway method?
                    # if it is attribute request we should skip other
                    if is_atr_request:
                        raise Exception("is atr request")
                    # connect case
                    if last_word == "connect":
                        topic_s = topic_s[:-1]
                        # todo add rpc handler update here
                        # todo create set_devices??? and use here
                        self.gateway.on_device_connected(topic_s[-1], self, self.__handler)
                    # disconnect case
                    elif last_word == "disconnect":
                        self.gateway.on_device_disconnected(topic_s[-2])
                        raise Exception("disconnect")
                    # connect case with parameters or plain telemetry #todo if atrs go here
                    for topic_filter in self.telemetry_atrs_dict_handlers:

                        topic_filter_as_list = self.telemetry_atrs_dict_handlers[topic_filter][0]
                        for filter in topic_filter_as_list:
                            filter = filter.split(sep="/")
                            tmp_topic = deepcopy(topic_s)
                            if is_equivalent(filter, tmp_topic):
                                extension_instance = self.telemetry_atrs_dict_handlers[topic_filter][1]()
                                result = extension_instance.convert_message_to_json_for_storage(message.topic,
                                                                                                message.payload)
                                if not result:
                                    # todo log error of extension?
                                    continue
                                for device_data in result:
                                    telemetry = None
                                    # todo add device_type here
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

