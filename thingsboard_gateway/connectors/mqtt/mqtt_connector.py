#     Copyright 2020. ThingsBoard
#
#     Licensed under the Apache License, Version 2.0 (the "License");
#     you may not use this file except in compliance with the License.
#     You may obtain a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#     Unless required by applicable law or agreed to in writing, software
#     distributed under the License is distributed on an "AS IS" BASIS,
#     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#     See the License for the specific language governing permissions and
#     limitations under the License.

import time
import string
import random
from re import match, fullmatch, search
import ssl
from paho.mqtt.client import Client
from thingsboard_gateway.connectors.connector import Connector, log
from thingsboard_gateway.connectors.mqtt.json_mqtt_uplink_converter import JsonMqttUplinkConverter
from threading import Thread
from thingsboard_gateway.tb_utility.tb_utility import TBUtility
from simplejson import loads


class MqttConnector(Connector, Thread):
    def __init__(self, gateway, config, connector_type):
        super().__init__()
        self.__log = log
        self.config = config
        self.__connector_type = connector_type
        self.statistics = {'MessagesReceived': 0,
                           'MessagesSent': 0}
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
        self.setName(config.get("name", self.__broker.get("name",
                                       'Mqtt Broker ' + ''.join(random.choice(string.ascii_lowercase) for _ in range(5)))))
        if "username" in self.__broker["security"]:
            self._client.username_pw_set(self.__broker["security"]["username"],
                                         self.__broker["security"]["password"])
        if "caCert" in self.__broker["security"] or self.__broker["security"].get("type", "none").lower() == "tls":
            ca_cert = self.__broker["security"].get("caCert")
            private_key = self.__broker["security"].get("privateKey")
            cert = self.__broker["security"].get("cert")
            if ca_cert is None:
                self._client.tls_set_context(ssl.SSLContext(ssl.PROTOCOL_TLSv1_2))
            else:
                try:
                    self._client.tls_set(ca_certs=ca_cert,
                                         certfile=cert,
                                         keyfile=private_key,
                                         cert_reqs=ssl.CERT_REQUIRED,
                                         tls_version=ssl.PROTOCOL_TLSv1_2,
                                         ciphers=None)
                except Exception as e:
                    self.__log.error("Cannot setup connection to broker %s using SSL. Please check your configuration.\nError: %s",
                              self.get_name(),
                              e)
                self._client.tls_insecure_set(False)
        self._client.on_connect = self._on_connect
        self._client.on_message = self._on_message
        self._client.on_subscribe = self._on_subscribe
        self.__subscribes_sent = {}  # For logging the subscriptions
        self._client.on_disconnect = self._on_disconnect
        self._client.on_log = self._on_log
        self._connected = False
        self.__stopped = False
        self.daemon = True

    def is_connected(self):
        return self._connected

    def open(self):
        self.__stopped = False
        self.start()

    def run(self):
        try:
            while not self._connected and not self.__stopped:
                try:
                    self._client.connect(self.__broker['host'],
                                         self.__broker.get('port', 1883))
                    self._client.loop_start()
                    if not self._connected:
                        time.sleep(1)
                except Exception as e:
                    self.__log.exception(e)
                    time.sleep(10)

        except Exception as e:
            self.__log.exception(e)
            try:
                self.close()
            except Exception as e:
                self.__log.exception(e)
        while True:
            if self.__stopped:
                break
            else:
                time.sleep(1)

    def close(self):
        self.__stopped = True
        try:
            self._client.disconnect()
        except Exception as e:
            log.exception(e)
        self._client.loop_stop()
        self.__log.info('%s has been stopped.', self.get_name())

    def get_name(self):
        return self.name

    def __subscribe(self, topic):
        message = self._client.subscribe(topic)
        try:
            self.__subscribes_sent[message[1]] = topic
        except Exception as e:
            self.__log.exception(e)

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
            self.__log.info('%s connected to %s:%s - successfully.',
                     self.get_name(),
                     self.__broker["host"],
                     self.__broker.get("port", "1883"))
            for mapping in self.__mapping:
                try:
                    converter = None
                    if mapping["converter"]["type"] == "custom":
                        try:
                            module = TBUtility.check_and_import(self.__connector_type, mapping["converter"]["extension"])
                            if module is not None:
                                self.__log.debug('Custom converter for topic %s - found!', mapping["topicFilter"])
                                converter = module(mapping)
                            else:
                                self.__log.error("\n\nCannot find extension module for %s topic.\n\Please check your configuration.\n", mapping["topicFilter"])
                        except Exception as e:
                            self.__log.exception(e)
                    else:
                        converter = JsonMqttUplinkConverter(mapping)
                    if converter is not None:
                        regex_topic = TBUtility.topic_to_regex(mapping.get("topicFilter"))
                        if not self.__sub_topics.get(regex_topic):
                            self.__sub_topics[regex_topic] = []

                        self.__sub_topics[regex_topic].append({converter: None})
                        # self._client.subscribe(TBUtility.regex_to_topic(regex_topic))
                        self.__subscribe(mapping["topicFilter"])
                        self.__log.info('Connector "%s" subscribe to %s',
                                 self.get_name(),
                                 TBUtility.regex_to_topic(regex_topic))
                    else:
                        self.__log.error("Cannot find converter for %s topic", mapping["topicFilter"])
                except Exception as e:
                    self.__log.exception(e)
            try:
                for request in self.__service_config:
                    if self.__service_config.get(request) is not None:
                        for request_config in self.__service_config.get(request):
                            self.__subscribe(request_config["topicFilter"])
            except Exception as e:
                self.__log.error(e)

        else:
            if rc in result_codes:
                self.__log.error("%s connection FAIL with error %s %s!", self.get_name(), rc, result_codes[rc])
            else:
                self.__log.error("%s connection FAIL with unknown error!", self.get_name())

    def _on_disconnect(self, *args):
        self.__log.debug('"%s" was disconnected.', self.get_name())

    def _on_log(self, *args):
        self.__log.debug(args)
        # pass

    def _on_subscribe(self, client, userdata, mid, granted_qos):
        try:
            if granted_qos[0] == 128:
                self.__log.error('"%s" subscription failed to topic %s subscription message id = %i', self.get_name(), self.__subscribes_sent.get(mid), mid)
            else:
                self.__log.info('"%s" subscription success to topic %s, subscription message id = %i', self.get_name(), self.__subscribes_sent.get(mid), mid)
                if self.__subscribes_sent.get(mid) is not None:
                    del self.__subscribes_sent[mid]
        except Exception as e:
            self.__log.exception(e)

    def __get_service_config(self, config):
        for service_config in self.__service_config:
            if service_config != "attributeUpdates" and config.get(service_config):
                self.__service_config[service_config] = config[service_config]
            else:
                self.__attribute_updates = config[service_config]

    def _on_message(self, client, userdata, message):
        self.statistics['MessagesReceived'] += 1
        content = TBUtility.decode(message)
        regex_topic = [regex for regex in self.__sub_topics if fullmatch(regex, message.topic)]
        if regex_topic:
            try:
                for regex in regex_topic:
                    if self.__sub_topics.get(regex):
                        for converter_value in range(len(self.__sub_topics.get(regex))):
                            if self.__sub_topics[regex][converter_value]:
                                for converter in self.__sub_topics.get(regex)[converter_value]:
                                    converted_content = converter.convert(message.topic, content)
                                    if converted_content:
                                        try:
                                            self.__sub_topics[regex][converter_value][converter] = converted_content
                                        except Exception as e:
                                            self.__log.exception(e)
                                        self.__gateway.send_to_storage(self.name, converted_content)
                                        self.statistics['MessagesSent'] += 1
                                    else:
                                        continue
                            else:
                                self.__log.error('Cannot find converter for topic:"%s"!', message.topic)
                                return
            except Exception as e:
                log.exception(e)
                return
        elif self.__service_config.get("connectRequests"):
            connect_requests = [connect_request for connect_request in self.__service_config.get("connectRequests")]
            if connect_requests:
                for request in connect_requests:
                    if request.get("topicFilter"):
                        if message.topic in request.get("topicFilter") or\
                                (request.get("deviceNameTopicExpression") is not None and search(request.get("deviceNameTopicExpression"), message.topic)):
                            founded_device_name = None
                            founded_device_type
                            if request.get("deviceNameJsonExpression"):
                                founded_device_name = TBUtility.get_value(request["deviceNameJsonExpression"], content)
                            if request.get("deviceNameTopicExpression"):
                                device_name_expression = request["deviceNameTopicExpression"]
                                founded_device_name = search(device_name_expression, message.topic)
                            if request.get("deviceTypeJsonExpression"):
                                founded_device_type = TBUtility.get_value(request["deviceTypeJsonExpression"], content)
                            if request.get("deviceTypeTopicExpression"):
                                device_type_expression = request["deviceTypeTopicExpression"]
                                founded_device_type = search(device_type_expression, message.topic)
                            if founded_device_name is not None and founded_device_name not in self.__gateway.get_devices():
                                self.__gateway.add_device(founded_device_name, {"connector": self}, device_type=founded_device_type)
                        else:
                            self.__log.error("Cannot find connect request for device from message from topic: %s and with data: %s",
                                             message.topic,
                                             content)
                    else:
                        self.__log.error("\"topicFilter\" in connect requests config not found.")
            else:
                self.__log.error("Connection requests in config not found.")

        elif self.__service_config.get("disconnectRequests") is not None:
            disconnect_requests = [disconnect_request for disconnect_request in self.__service_config.get("disconnectRequests")]
            if disconnect_requests:
                for request in disconnect_requests:
                    if request.get("topicFilter") is not None:
                        if message.topic in request.get("topicFilter") or\
                                (request.get("deviceNameTopicExpression") is not None and search(request.get("deviceNameTopicExpression"), message.topic)):
                            founded_device_name = None
                            if request.get("deviceNameJsonExpression"):
                                founded_device_name = TBUtility.get_value(request["deviceNameJsonExpression"], content)
                            if request.get("deviceNameTopicExpression"):
                                device_name_expression = request["deviceNameTopicExpression"]
                                founded_device_name = search(device_name_expression, message.topic)
                            if founded_device_name is not None and founded_device_name in self.__gateway.get_devices():
                                self.__gateway.del_device(founded_device_name)
                        else:
                            self.__log.error("Cannot find connect request for device from message from topic: %s and with data: %s",
                                      message.topic,
                                      content)
                    else:
                        self.__log.error("\"topicFilter\" in connect requests config not found.")
            else:
                self.__log.error("Disconnection requests in config not found.")
        elif message.topic in self.__gateway.rpc_requests_in_progress:
            self.__gateway.rpc_with_reply_processing(message.topic, content)
        else:
            self.__log.debug("Received message to topic \"%s\" with unknown interpreter data: \n\n\"%s\"",
                      message.topic,
                      content)

    def on_attributes_update(self, content):
        attribute_updates_config = [update for update in self.__attribute_updates]
        if attribute_updates_config:
            for attribute_update in attribute_updates_config:
                if match(attribute_update["deviceNameFilter"], content["device"]) and \
                        content["data"].get(attribute_update["attributeFilter"]):
                    topic = attribute_update["topicExpression"]\
                            .replace("${deviceName}", content["device"])\
                            .replace("${attributeKey}", attribute_update["attributeFilter"])\
                            .replace("${attributeValue}", content["data"][attribute_update["attributeFilter"]])
                    data = ''
                    try:
                        data = attribute_update["valueExpression"]\
                                .replace("${attributeKey}", attribute_update["attributeFilter"])\
                                .replace("${attributeValue}", content["data"][attribute_update["attributeFilter"]])
                    except Exception as e:
                        self.__log.error(e)
                    self._client.publish(topic, data).wait_for_publish()
                    self.__log.debug("Attribute Update data: %s for device %s to topic: %s",
                              data,
                              content["device"],
                              topic)
                else:
                    self.__log.error("Not found deviceName by filter in message or attributeFilter in message with data: %s",
                              content)
        else:
            self.__log.error("Attribute updates config not found.")

    def server_side_rpc_handler(self, content):
        for rpc_config in self.__server_side_rpc:
            if search(rpc_config["deviceNameFilter"], content["device"]) \
                    and search(rpc_config["methodFilter"], content["data"]["method"]) is not None:
                # Subscribe to RPC response topic
                if rpc_config.get("responseTopicExpression"):
                    topic_for_subscribe = rpc_config["responseTopicExpression"] \
                        .replace("${deviceName}", content["device"]) \
                        .replace("${methodName}", content["data"]["method"]) \
                        .replace("${requestId}", str(content["data"]["id"])) \
                        .replace("${params}", content["data"]["params"])
                    if rpc_config.get("responseTimeout"):
                        timeout = time.time()*1000+rpc_config.get("responseTimeout")
                        self.__gateway.register_rpc_request_timeout(content,
                                                                    timeout,
                                                                    topic_for_subscribe,
                                                                    self.rpc_cancel_processing)
                        # Maybe we need to wait for the command to execute successfully before publishing the request.
                        self._client.subscribe(topic_for_subscribe)
                    else:
                        self.__log.error("Not found RPC response timeout in config, sending without waiting for response")
                # Publish RPC request
                if rpc_config.get("requestTopicExpression") is not None\
                        and rpc_config.get("valueExpression"):
                    topic = rpc_config.get("requestTopicExpression")\
                        .replace("${deviceName}", content["device"])\
                        .replace("${methodName}", content["data"]["method"])\
                        .replace("${requestId}", str(content["data"]["id"]))\
                        .replace("${params}", content["data"]["params"])
                    data_to_send = rpc_config.get("valueExpression")\
                        .replace("${deviceName}", content["device"])\
                        .replace("${methodName}", content["data"]["method"])\
                        .replace("${requestId}", str(content["data"]["id"]))\
                        .replace("${params}", content["data"]["params"])
                    try:
                        self._client.publish(topic, data_to_send)
                        self.__log.debug("Send RPC with no response request to topic: %s with data %s",
                                  topic,
                                  data_to_send)
                        if rpc_config.get("responseTopicExpression") is None:
                            self.__gateway.send_rpc_reply(device=content["device"], req_id=content["data"]["id"], success_sent=True)
                    except Exception as e:
                        self.__log.exception(e)

    def rpc_cancel_processing(self, topic):
        self._client.unsubscribe(topic)

