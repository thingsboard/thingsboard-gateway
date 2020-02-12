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

import logging
import queue
import ssl
import time
from simplejson import loads, dumps
from threading import RLock
from threading import Thread
from thingsboard_gateway.tb_utility.tb_utility import TBUtility

import paho.mqtt.client as paho
from jsonschema import Draft7Validator
from jsonschema import ValidationError

KV_SCHEMA = {
    "type": "object",
    "patternProperties":
        {
            ".": {"type": ["integer",
                           "string",
                           "boolean",
                           "number"]}
        },
    "minProperties": 1,
}
SCHEMA_FOR_CLIENT_RPC = {
    "type": "object",
    "patternProperties":
        {
            ".": {"type": ["integer",
                           "string",
                           "boolean",
                           "number"]}
        },
    "minProperties": 0,
}
TS_KV_SCHEMA = {
    "type": "object",
    "properties": {
        "ts": {
            "type": ["integer"]
        },
        "values": KV_SCHEMA
    },
    "additionalProperties": False
}
DEVICE_TS_KV_SCHEMA = {
    "type": "array",
    "items": TS_KV_SCHEMA
}
DEVICE_TS_OR_KV_SCHEMA = {
    "type": "array",
    "items":    {
        "anyOf":
            [
                TS_KV_SCHEMA,
                KV_SCHEMA
            ]
    }
}
RPC_VALIDATOR = Draft7Validator(SCHEMA_FOR_CLIENT_RPC)
KV_VALIDATOR = Draft7Validator(KV_SCHEMA)
TS_KV_VALIDATOR = Draft7Validator(TS_KV_SCHEMA)
DEVICE_TS_KV_VALIDATOR = Draft7Validator(DEVICE_TS_KV_SCHEMA)
DEVICE_TS_OR_KV_VALIDATOR = Draft7Validator(DEVICE_TS_OR_KV_SCHEMA)

RPC_RESPONSE_TOPIC = 'v1/devices/me/rpc/response/'
RPC_REQUEST_TOPIC = 'v1/devices/me/rpc/request/'
ATTRIBUTES_TOPIC = 'v1/devices/me/attributes'
ATTRIBUTES_TOPIC_REQUEST = 'v1/devices/me/attributes/request/'
ATTRIBUTES_TOPIC_RESPONSE = 'v1/devices/me/attributes/response/'
TELEMETRY_TOPIC = 'v1/devices/me/telemetry'
log = logging.getLogger("tb_connection")


class TBTimeoutException(Exception):
    pass


class TBQoSException(Exception):
    pass


class TBPublishInfo:

    TB_ERR_AGAIN = -1
    TB_ERR_SUCCESS = 0
    TB_ERR_NOMEM = 1
    TB_ERR_PROTOCOL = 2
    TB_ERR_INVAL = 3
    TB_ERR_NO_CONN = 4
    TB_ERR_CONN_REFUSED = 5
    TB_ERR_NOT_FOUND = 6
    TB_ERR_CONN_LOST = 7
    TB_ERR_TLS = 8
    TB_ERR_PAYLOAD_SIZE = 9
    TB_ERR_NOT_SUPPORTED = 10
    TB_ERR_AUTH = 11
    TB_ERR_ACL_DENIED = 12
    TB_ERR_UNKNOWN = 13
    TB_ERR_ERRNO = 14
    TB_ERR_QUEUE_SIZE = 15

    def __init__(self, message_info):
        self.message_info = message_info

    def rc(self):
        return self.message_info.rc

    def mid(self):
        return self.message_info.mid

    def get(self):
        self.message_info.wait_for_publish()
        return self.message_info.rc


class TBDeviceMqttClient:
    def __init__(self, host, port=1883, token=None):
        self._client = paho.Client()
        self.__host = host
        self.__port = port
        if token == "":
            log.warning("token is not set, connection without tls wont be established")
        else:
            self._client.username_pw_set(token)
        self._lock = RLock()

        self._attr_request_dict = {}
        self.stopped = False
        self.__timeout_queue = queue.Queue()
        self.__timeout_thread = Thread(target=self.__timeout_check)
        self.__timeout_thread.daemon = True
        self.__timeout_thread.start()
        self.__is_connected = False
        self.__device_on_server_side_rpc_response = None
        self.__connect_callback = None
        self.__device_max_sub_id = 0
        self.__device_client_rpc_number = 0
        self.__device_sub_dict = {}
        self.__device_client_rpc_dict = {}
        self.__attr_request_number = 0
        self._client.on_connect = self._on_connect
        self._client.on_log = self._on_log
        self._client.on_publish = self._on_publish
        self._client.on_message = self._on_message
        self._client.on_disconnect = self._on_disconnect

    def _on_log(self, client, userdata, level, buf):
        log.exception(buf)

    def _on_publish(self, client, userdata, result):
        # log.debug("Data published to ThingsBoard!")
        pass

    def _on_disconnect(self, client, userdata, rc):
        log.debug(client)
        log.debug("Disconnected")

    def _on_connect(self, client, userdata, flags, rc, *extra_params):
        result_codes = {
            1: "incorrect protocol version",
            2: "invalid client identifier",
            3: "server unavailable",
            4: "bad username or password",
            5: "not authorised",
        }
        if self.__connect_callback:
            self.__connect_callback(client, userdata, flags, rc, *extra_params)
        if rc == 0:
            self.__is_connected = True
            log.info("connection SUCCESS")
            log.debug(client)
            self._client.subscribe(ATTRIBUTES_TOPIC, qos=1)
            self._client.subscribe(ATTRIBUTES_TOPIC + "/response/+", 1)
            self._client.subscribe(RPC_REQUEST_TOPIC + '+')
            self._client.subscribe(RPC_RESPONSE_TOPIC + '+', qos=1)
        else:
            if rc in result_codes:
                log.error("connection FAIL with error {rc} {explanation}".format(rc=rc,
                                                                                 explanation=result_codes[rc]))
            else:
                log.error("connection FAIL with unknown error")

    def is_connected(self):
        return self.__is_connected

    def connect(self, callback=None, min_reconnect_delay=1, timeout=120, tls=False, ca_certs=None, cert_file=None, key_file=None, keepalive=60):
        if tls:
            self._client.tls_set(ca_certs=ca_certs,
                                 certfile=cert_file,
                                 keyfile=key_file,
                                 cert_reqs=ssl.CERT_REQUIRED,
                                 tls_version=ssl.PROTOCOL_TLSv1_2,
                                 ciphers=None)
            self._client.tls_insecure_set(False)
        self._client.connect(self.__host, self.__port, keepalive=keepalive)
        self.reconnect_delay_set(min_reconnect_delay, timeout)
        self._client.loop_start()
        self.__connect_callback = callback

    def disconnect(self):
        self._client.disconnect()
        log.debug(self._client)
        log.debug("Disconnecting from ThingsBoard")
        self.__is_connected = False
        self._client.loop_stop()

    def stop(self):
        self.stopped = True

    def _on_message(self, client, userdata, message):
        content = TBUtility.decode(message)
        self._on_decoded_message(content, message)

    @staticmethod
    def validate(validator, data):
        try:
            validator.validate(data)
        except ValidationError as e:
            log.error(e)
            raise e

    def _on_decoded_message(self, content, message):
        if message.topic.startswith(RPC_REQUEST_TOPIC):
            request_id = message.topic[len(RPC_REQUEST_TOPIC):len(message.topic)]
            if self.__device_on_server_side_rpc_response:
                self.__device_on_server_side_rpc_response(request_id, content)
        elif message.topic.startswith(RPC_RESPONSE_TOPIC):
            with self._lock:
                request_id = int(message.topic[len(RPC_RESPONSE_TOPIC):len(message.topic)])
                callback = self.__device_client_rpc_dict.pop(request_id)
            callback(request_id, content, None)
        elif message.topic == ATTRIBUTES_TOPIC:
            dict_results = []

            with self._lock:
                # callbacks for everything
                if self.__device_sub_dict.get("*"):
                    for x in self.__device_sub_dict["*"]:
                        dict_results.append(self.__device_sub_dict["*"][x])
                # specific callback
                keys = content.keys()
                keys_list = []
                for key in keys:
                    keys_list.append(key)
                # iterate through message
                for key in keys_list:
                    # find key in our dict
                    if self.__device_sub_dict.get(key):
                        for x in self.__device_sub_dict[key]:
                            dict_results.append(self.__device_sub_dict[key][x])
            for res in dict_results:
                res(content, None)
        elif message.topic.startswith(ATTRIBUTES_TOPIC_RESPONSE):
            with self._lock:
                req_id = int(message.topic[len(ATTRIBUTES_TOPIC+"/response/"):])
                # pop callback and use it
                callback = self._attr_request_dict.pop(req_id)
            callback(content, None)

    def max_inflight_messages_set(self, inflight):
        """Set the maximum number of messages with QoS>0 that can be part way through their network flow at once.
        Defaults to 20. Increasing this value will consume more memory but can increase throughput."""
        self._client.max_inflight_messages_set(inflight)

    def max_queued_messages_set(self, queue_size):
        """Set the maximum number of outgoing messages with QoS>0 that can be pending in the outgoing message queue.
        Defaults to 0. 0 means unlimited. When the queue is full, any further outgoing messages would be dropped."""
        self._client.max_queued_messages_set(queue_size)

    def reconnect_delay_set(self, min_delay=1, max_delay=120):
        """The client will automatically retry connection. Between each attempt it will wait a number of seconds
         between min_delay and max_delay. When the connection is lost, initially the reconnection attempt is delayed
         of min_delay seconds. It’s doubled between subsequent attempt up to max_delay. The delay is reset to min_delay
          when the connection complete (e.g. the CONNACK is received, not just the TCP connection is established)."""
        self._client.reconnect_delay_set(min_delay, max_delay)

    def send_rpc_reply(self, req_id, resp, quality_of_service=1, wait_for_publish=False):
        if quality_of_service != 0 and quality_of_service != 1:
            log.error("Quality of service (qos) value must be 0 or 1")
            return
        info = self._client.publish(RPC_RESPONSE_TOPIC + req_id, resp, qos=quality_of_service)
        if wait_for_publish:
            info.wait_for_publish()

    def send_rpc_call(self, method, params, callback):
        self.validate(RPC_VALIDATOR, params)
        with self._lock:
            self.__device_client_rpc_number += 1
            self.__device_client_rpc_dict.update({self.__device_client_rpc_number: callback})
            rpc_request_id = self.__device_client_rpc_number
        payload = {"method": method, "params": params}
        self._client.publish(RPC_REQUEST_TOPIC + str(rpc_request_id),
                             dumps(payload),
                             qos=1)

    def set_server_side_rpc_request_handler(self, handler):
        self.__device_on_server_side_rpc_response = handler

    def publish_data(self, data, topic, qos):
        data = dumps(data)
        if qos != 0 and qos != 1:
            log.exception("Quality of service (qos) value must be 0 or 1")
            raise TBQoSException("Quality of service (qos) value must be 0 or 1")
        else:
            return TBPublishInfo(self._client.publish(topic, data, qos))

    def send_telemetry(self, telemetry, quality_of_service=1):
        if type(telemetry) is not list:
            telemetry = [telemetry]
        self.validate(DEVICE_TS_OR_KV_VALIDATOR, telemetry)
        return self.publish_data(telemetry, TELEMETRY_TOPIC, quality_of_service)

    def send_attributes(self, attributes, quality_of_service=1):
        self.validate(KV_VALIDATOR, attributes)
        return self.publish_data(attributes, ATTRIBUTES_TOPIC, quality_of_service)

    def unsubscribe_from_attribute(self, subscription_id):
        with self._lock:
            for x in self.__device_sub_dict:
                if self.__device_sub_dict[x].get(subscription_id):
                    del self.__device_sub_dict[x][subscription_id]
                    log.debug("Unsubscribed from {attribute}, subscription id {sub_id}".format(attribute=x,
                                                                                               sub_id=subscription_id))
            if subscription_id == '*':
                self.__device_sub_dict = {}
            self.__device_sub_dict = dict((k, v) for k, v in self.__device_sub_dict.items() if v is not {})

    def subscribe_to_all_attributes(self, callback):
        return self.subscribe_to_attribute("*", callback)

    def subscribe_to_attribute(self, key, callback):
        with self._lock:
            self.__device_max_sub_id += 1
            if key not in self.__device_sub_dict:
                self.__device_sub_dict.update({key: {self.__device_max_sub_id: callback}})
            else:
                self.__device_sub_dict[key].update({self.__device_max_sub_id: callback})
            log.debug("Subscribed to {key} with id {id}".format(key=key, id=self.__device_max_sub_id))
            return self.__device_max_sub_id

    def request_attributes(self, client_keys=None, shared_keys=None, callback=None):
        msg = {}
        if client_keys:
            tmp = ""
            for key in client_keys:
                tmp += key + ","
            tmp = tmp[:len(tmp) - 1]
            msg.update({"clientKeys": tmp})
        if shared_keys:
            tmp = ""
            for key in shared_keys:
                tmp += key + ","
            tmp = tmp[:len(tmp) - 1]
            msg.update({"sharedKeys": tmp})

        ts_in_millis = int(round(time.time() * 1000))

        attr_request_number = self._add_attr_request_callback(callback)

        info = self._client.publish(topic=ATTRIBUTES_TOPIC_REQUEST + str(self.__attr_request_number),
                                    payload=dumps(msg),
                                    qos=1)
        self._add_timeout(attr_request_number, ts_in_millis + 30000)
        return info

    def _add_timeout(self, attr_request_number, ts):
        self.__timeout_queue.put({"ts": ts, "attribute_request_id": attr_request_number})

    def _add_attr_request_callback(self, callback):
        with self._lock:
            self.__attr_request_number += 1
            self._attr_request_dict.update({self.__attr_request_number: callback})
            attr_request_number = self.__attr_request_number
        return attr_request_number

    def __timeout_check(self):
        while not self.stopped:
            try:
                if not self.__timeout_queue.empty():
                    item = self.__timeout_queue.get_nowait()
                    if item is not None:
                        while not self.stopped:
                            current_ts_in_millis = int(round(time.time() * 1000))
                            if current_ts_in_millis > item["ts"]:
                                break
                            else:
                                time.sleep(0.001)
                        with self._lock:
                            callback = None
                            if item.get("attribute_request_id"):
                                if self._attr_request_dict.get(item["attribute_request_id"]):
                                    callback = self._attr_request_dict.pop(item["attribute_request_id"])
                            elif item.get("rpc_request_id"):
                                if self.__device_client_rpc_dict.get(item["rpc_request_id"]):
                                    callback = self.__device_client_rpc_dict.pop(item["rpc_request_id"])
                        if callback is not None:
                            callback(None, TBTimeoutException("Timeout while waiting for reply from ThingsBoard!"))
                else:
                    time.sleep(0.01)
            except Exception as e:
                log.warning(e)
