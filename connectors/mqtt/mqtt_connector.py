import time
import logging
import string
import random
import re
import ssl
from paho.mqtt.client import Client
from connectors.connector import Connector
from connectors.mqtt.json_mqtt_uplink_converter import JsonMqttUplinkConverter
from threading import Thread
from tb_utility.tb_utility import TBUtility
from json import loads

log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)


class MqttConnector(Connector, Thread):
    def __init__(self, gateway, config):
        super().__init__()
        self.__gateway = gateway
        self.__broker = config.get('broker')
        self.__mapping = config.get('mapping')
        self.__server_side_rpc = config.get('serverSideRpc')
        self.__service_config = {"connectRequests": None, "disconnectRequests": None}
        self.__attribute_updates = []
        self.__get_service_config(config)
        self.__sub_topics = {}
        client_id = ''.join(random.choice(string.ascii_lowercase) for _ in range(23))
        self._client = Client(client_id)
        self.setName(TBUtility.get_parameter(self.__broker,
                                             "name",
                                             'Mqtt Broker ' + ''.join(random.choice(string.ascii_lowercase) for _ in range(5))))
        if self.__broker["credentials"]["type"] == "basic":
            self._client.username_pw_set(self.__broker["credentials"]["username"],
                                         self.__broker["credentials"]["password"])
        elif self.__broker["credentials"]["type"] == "cert.PEM":
            ca_cert = self.__broker["credentials"].get("caCert")
            private_key = self.__broker["credentials"].get("privateKey")
            cert = self.__broker["credentials"].get("cert")
            if ca_cert is None or cert is None:
                log.error("caCert and cert parameters must be in config if you need to use the SSL. Please add it and try again.")
            else:
                try:
                    self._client.tls_set(ca_certs=ca_cert,
                                         certfile=cert,
                                         keyfile=private_key,
                                         cert_reqs=ssl.CERT_REQUIRED,
                                         tls_version=ssl.PROTOCOL_TLSv1,
                                         ciphers=None)
                except Exception as e:
                    log.error("Cannot setup connection to broker %s using SSL. Please check your configuration.\n\
                              Error cathed: %s",
                              self.get_name(),
                              e)
                self._client.tls_insecure_set(False)
        self._client.on_connect = self._on_connect
        self._client.on_message = self._on_message
        self._client.on_subscribe = self._on_subscribe
        self.__sended_subscribes = {}  # For logging the subscriptions
        self._client.on_disconnect = self._on_disconnect
        self._client.on_log = self._on_log
        self._connected = False
        self.__stopped = False
        self.daemon = True

    def open(self):
        self.__stopped = False
        self.start()
        try:
            while not self._connected:
                try:
                    self._client.connect(self.__broker['host'],
                                         TBUtility.get_parameter(self.__broker, 'port', 1883))
                    self._client.loop_start()
                except Exception as e:
                    log.error(e)
                time.sleep(1)

        except Exception as e:
            log.error(e)
            try:
                self.close()
            except Exception as e:
                log.debug(e)

    def run(self):
        while True:
            time.sleep(.1)
            if self.__stopped:
                break

    def close(self):
        self._client.loop_stop()
        self._client.disconnect()
        self.__stopped = True
        log.info('%s has been stopped.', self.get_name())

    def get_name(self):
        return self.name

    def __subscribe(self, topic):
        message = self._client.subscribe(topic)
        self.__sended_subscribes[message[1]] = topic

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
            log.info('%s connected to %s:%s - successfully.',
                     self.get_name(),
                     self.__broker["host"],
                    TBUtility.get_parameter(self.__broker, "port", "1883"))
            for mapping in self.__mapping:
                try:
                    converter = None
                    if mapping["converter"]["type"] == "custom":
                        try:
                            module = TBUtility.check_and_import('mqtt', mapping["converter"]["extension"])
                            if module is not None:
                                log.debug('Custom converter for topic %s - found!', mapping["topicFilter"])
                                converter = module(mapping)
                            else:
                                log.error("\n\nCannot find extension module for %s topic.\n\
                                           Please check your configuration.\n", mapping["topicFilter"])
                        except Exception as e:
                            log.exception(e)
                    else:
                        converter = JsonMqttUplinkConverter(mapping)
                    if converter is not None:
                        regex_topic = TBUtility.topic_to_regex(mapping.get("topicFilter"))
                        if not self.__sub_topics.get(regex_topic):
                            self.__sub_topics[regex_topic] = []

                        self.__sub_topics[regex_topic].append({converter: None})
                        # self._client.subscribe(TBUtility.regex_to_topic(regex_topic))
                        self.__subscribe(TBUtility.regex_to_topic(regex_topic))
                        log.info('Connector "%s" subscribe to %s',
                                 self.get_name(),
                                 TBUtility.regex_to_topic(regex_topic))
                    else:
                        log.error("Cannot find converter for %s topic", mapping["topicFilter"])
                except Exception as e:
                    log.exception(e)
            try:
                for request in self.__service_config:
                    if self.__service_config.get(request) is not None:
                        for request_config in self.__service_config.get(request):
                            self._client.subscribe(request_config["topicFilter"])
            except Exception as e:
                log.error(e)

        else:
            if rc in result_codes:
                log.error("%s connection FAIL with error %s %s!", self.get_name(), rc, result_codes[rc])
            else:
                log.error("%s connection FAIL with unknown error!", self.get_name())

    def _on_disconnect(self, *args):
        log.debug('"%s" was disconnected.', self.get_name())

    def _on_log(self,*args):
        log.debug(args)

    def _on_subscribe(self, client, userdata, mid, granted_qos):
        if granted_qos[0] == 128:
            log.error('"%s" subscription failed to topic %s', self.get_name(), self.__sended_subscribes[mid])
        else:
            log.error('"%s" subscription success to topic %s', self.get_name(), self.__sended_subscribes[mid])
        del self.__sended_subscribes[mid]

    def __get_service_config(self, config):
        for service_config in self.__service_config:
            if service_config != "attributeUpdates" and config.get(service_config):
                self.__service_config[service_config] = config[service_config]
            else:
                self.__attribute_updates = config[service_config]

    def _on_message(self, client, userdata, message):
        content = self._decode(message)
        regex_topic = [regex for regex in self.__sub_topics if re.fullmatch(regex, message.topic)]
        if regex_topic:
            for regex in regex_topic:
                if self.__sub_topics.get(regex):
                    for converter_value in range(len(self.__sub_topics.get(regex))):
                        if self.__sub_topics[regex][converter_value]:
                            for converter in self.__sub_topics.get(regex)[converter_value]:
                                converted_content = converter.convert(message.topic, content)
                                if converted_content and TBUtility.validate_converted_data(converted_content):
                                    self.__sub_topics[regex][converter_value][converter] = converted_content
                                    if not self.__gateway._TBGatewayService__connected_devices.get(converted_content["deviceName"]):
                                        self.__gateway._TBGatewayService__connected_devices[converted_content["deviceName"]] = {"connector": None}
                                    self.__gateway._TBGatewayService__connected_devices[converted_content["deviceName"]]["connector"] = self
                                    self.__gateway._send_to_storage(self.get_name(), converted_content)
                                else:
                                    continue
                        else:
                            log.error('Cannot find converter for topic:"%s"!', message.topic)
        elif self.__service_config.get("connectRequests"):
            connect_requests = [connect_request for connect_request in self.__service_config.get("connectRequests")]
            if connect_requests:
                for request in connect_requests:
                    if request.get("topicFilter"):
                        if message.topic in request.get("topicFilter") or\
                                (request.get("deviceNameTopicExpression") is not None and re.search(request.get("deviceNameTopicExpression"), message.topic)):
                            founded_device_name = None
                            if request.get("deviceNameJsonExpression"):
                                founded_device_name = TBUtility.get_value(request["deviceNameJsonExpression"], content)
                            if request.get("deviceNameTopicExpression"):
                                device_name_expression = request["deviceNameTopicExpression"]
                                founded_device_name = re.search(device_name_expression, message.topic)
                            if founded_device_name is not None and founded_device_name not in self.__gateway._TBGatewayService__connected_devices:
                                self.__gateway._TBGatewayService__connected_devices[founded_device_name] = {"connector": self}
                                self.__gateway.tb_client._client.gw_connect_device(founded_device_name)
                        else:
                            log.error("Cannot find connect request for device from message from topic: %s and with data: %s",
                                      message.topic,
                                      content)
                    else:
                        log.error("\"topicFilter\" in connect requests config not found.")
            else:
                log.error("Connection requests in config not found.")

        elif self.__service_config.get("disconnectRequests") is not None:
            disconnect_requests = [disconnect_request for disconnect_request in  self.__service_config.get("disconnectRequests")]
            if disconnect_requests:
                for request in disconnect_requests:
                    if request.get("topicFilter") is not None:
                        if message.topic in request.get("topicFilter") or\
                                (request.get("deviceNameTopicExpression") is not None and re.search(request.get("deviceNameTopicExpression"), message.topic)):
                            founded_device_name = None
                            if request.get("deviceNameJsonExpression"):
                                founded_device_name = TBUtility.get_value(request["deviceNameJsonExpression"], content)
                            if request.get("deviceNameTopicExpression"):
                                device_name_expression = request["deviceNameTopicExpression"]
                                founded_device_name = re.search(device_name_expression, message.topic)
                            if founded_device_name is not None and founded_device_name in self.__gateway._TBGatewayService__connected_devices:
                                del self.__gateway._TBGatewayService__connected_devices[founded_device_name]
                                self.__gateway.tb_client._client.gw_disconnect_device(founded_device_name)
                        else:
                            log.error("Cannot find connect request for device from message from topic: %s and with data: %s",
                                      message.topic,
                                      content)
                    else:
                        log.error("\"topicFilter\" in connect requests config not found.")
            else:
                log.error("Disconnection requests in config not found.")
        elif message.topic in self.__gateway.rpc_requests_in_progress:
            self.__gateway.rpc_with_reply_processing(message.topic, content)
        else:
            log.debug("Received message to topic \"%s\" with unknown interpreter data: \n\n\"%s\"",
                      message.topic,
                      content)

    def on_attributes_update(self, content):
        attribute_updates_config = [update for update in self.__attribute_updates]
        if attribute_updates_config:
            for attribute_update in attribute_updates_config:
                if re.match(attribute_update["deviceNameFilter"], content["device"]) and \
                        content["data"].get(attribute_update["attributeFilter"]):
                    topic = attribute_update["topicExpression"]\
                            .replace("${deviceName}", content["device"])\
                            .replace("${attributeKey}", attribute_update["attributeFilter"])\
                            .replace("${attributeValue}", content["data"][attribute_update["attributeFilter"]])
                    try:
                        data = attribute_update["valueExpression"]\
                                .replace("${attributeKey}", attribute_update["attributeFilter"])\
                                .replace("${attributeValue}", content["data"][attribute_update["attributeFilter"]])
                    except Exception as e:
                        log.error(e)
                    self._client.publish(topic, data).wait_for_publish()
                    log.debug("Attribute Update data: %s for device %s to topic: %s",
                              data,
                              content["device"],
                              topic)
                else:
                    log.error("Not found deviceName by filter in message or attributeFilter in message with data: %s",
                              content)
        else:
            log.error("Attribute updates config not found.")

    def server_side_rpc_handler(self, content):
        for rpc_config in self.__server_side_rpc:
            if re.search(rpc_config["deviceNameFilter"], content["device"]) \
                    and re.search(rpc_config["methodFilter"], content["data"]["method"]) is not None:
                # Subscribe to RPC response topic
                if rpc_config.get("responseTopicExpression"):
                    topic_for_subscribe = rpc_config["responseTopicExpression"] \
                        .replace("${deviceName}", content["device"]) \
                        .replace("${methodName}", content["data"]["method"]) \
                        .replace("${requestId}", content["data"]["id"]) \
                        .replace("${params}", content["data"]["params"])
                    if rpc_config.get("responseTimeout"):
                        timeout = time.time()+rpc_config.get("responseTimeout")
                        self.__gateway.register_rpc_request_timeout(content,
                                                                    timeout,
                                                                    topic_for_subscribe,
                                                                    self._client.rpc_cancel_processing)
                        # Maybe we need to wait for the command to execute successfully before publishing the request.
                        self._client.subscribe(topic_for_subscribe)
                    else:
                        log.error("Not found RPC response timeout in config, sending without waiting for response")
                # Publish RPC request
                if rpc_config.get("requestTopicExpression") is not None\
                        and rpc_config.get("valueExpression"):
                    topic = rpc_config.get("requestTopicExpression")\
                        .replace("${deviceName}", content["device"])\
                        .replace("${methodName}", content["data"]["method"])\
                        .replace("${requestId}", content["data"]["id"])\
                        .replace("${params}", content["data"]["params"])
                    data_to_send = rpc_config.get("valueExpression")\
                        .replace("${deviceName}", content["device"])\
                        .replace("${methodName}", content["data"]["method"])\
                        .replace("${requestId}", content["data"]["id"])\
                        .replace("${params}", content["data"]["params"])
                    try:
                        self._client.publish(topic, data_to_send)
                        log.debug("Send RPC with no response request to topic: %s with data %s",
                                  topic,
                                  data_to_send)
                    except Exception as e:
                        log.error(e)

    def rpc_cancel_processing(self, topic):
        self._client.unsubscribe(topic)


    @staticmethod
    def _decode(message):
        content = loads(message.payload.decode("utf-8"))
        return content
