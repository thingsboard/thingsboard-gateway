import time
import logging
import string
import random
import re
from importlib import import_module
from paho.mqtt.client import Client
from connectors.connector import Connector
from connectors.mqtt.json_mqtt_uplink_converter import JsonMqttUplinkConverter
from connectors.mqtt.custom_mqtt_uplink_converter import CustomMqttUplinkConverter
from threading import Thread
from tb_utility.tb_utility import TBUtility
from json import loads, dumps

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
            self._client.username_pw_set(self.__broker["credentials"]["username"], self.__broker["credentials"]["password"])

        self._client.on_connect = self._on_connect
        self._client.on_message = self._on_message
        self._client.on_subscribe = self._on_subscribe
        self._client.on_disconnect = self._on_disconnect
        self._client.on_log = self._on_log
        self.__rpc_requests = {}
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
            log.info('%s connected to %s:%s - successfully.', self.get_name(), self.__broker["host"], TBUtility.get_parameter(self.__broker, "port", "1883"))
            for mapping in self.__mapping:
                try:
                    regex_topic = TBUtility.topic_to_regex(mapping.get("topicFilter"))
                    if not self.__sub_topics.get(regex_topic):
                        self.__sub_topics[regex_topic] = []
                    if mapping["converter"]["type"] == "custom":
                        converter = CustomMqttUplinkConverter(mapping)
                    else:
                        converter = JsonMqttUplinkConverter(mapping)
                    self.__sub_topics[regex_topic].append({converter: None})
                    self._client.subscribe(TBUtility.regex_to_topic(regex_topic))
                    log.info('Connector "%s" subscribe to %s', self.get_name(), TBUtility.regex_to_topic(regex_topic))
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
            log.error('"%s" subscription failed.', self.get_name())
        else:
            log.error('"%s" subscription success.', self.get_name())

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
                                converted_content = converter.convert(content)
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
        self.__rpc_requests[content["data"]["id"]] = content
        for rpc_config in self.__server_side_rpc:
            if re.search(rpc_config["deviceNameFilter"], content["device"]):
                if re.search(rpc_config["methodFilter"], content["data"]["method"]) is not None:
                    if rpc_config.get("responseTopicExpression"):
                        pass #TODO Add rpc handler with waiting for response
                    else:
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

    @staticmethod
    def _decode(message):
        content = loads(message.payload.decode("utf-8"))
        return content
