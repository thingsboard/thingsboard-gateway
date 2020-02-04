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
import time
from simplejson import dumps

from thingsboard_gateway.tb_client.tb_device_mqtt import TBDeviceMqttClient, DEVICE_TS_KV_VALIDATOR, KV_VALIDATOR
from thingsboard_gateway.tb_utility.tb_utility import TBUtility

GATEWAY_ATTRIBUTES_TOPIC = "v1/gateway/attributes"
GATEWAY_ATTRIBUTES_REQUEST_TOPIC = "v1/gateway/attributes/request"
GATEWAY_ATTRIBUTES_RESPONSE_TOPIC = "v1/gateway/attributes/response"
GATEWAY_MAIN_TOPIC = "v1/gateway/"
GATEWAY_RPC_TOPIC = "v1/gateway/rpc"

log = logging.getLogger("tb_connection")
log.setLevel(logging.DEBUG)


class TBGatewayAPI:
    pass


class TBGatewayMqttClient(TBDeviceMqttClient):
    def __init__(self, host, port, token=None, gateway=None):
        super().__init__(host, port, token)
        self.__max_sub_id = 0
        self.__sub_dict = {}
        self.__connected_devices = set("*")
        self.devices_server_side_rpc_request_handler = None
        self._client.on_connect = self._on_connect
        self._client.on_message = self._on_message
        self._client.on_subscribe = self._on_subscribe
        self._gw_subscriptions = {}
        self.gateway = gateway

    def _on_connect(self, client, userdata, flags, rc, *extra_params):
        super()._on_connect(client, userdata, flags, rc, *extra_params)
        if rc == 0:
            self._gw_subscriptions[int(self._client.subscribe(GATEWAY_ATTRIBUTES_TOPIC, qos=1)[1])] = GATEWAY_ATTRIBUTES_TOPIC
            self._gw_subscriptions[int(self._client.subscribe(GATEWAY_ATTRIBUTES_RESPONSE_TOPIC + "/+")[1])] = GATEWAY_ATTRIBUTES_RESPONSE_TOPIC
            self._gw_subscriptions[int(self._client.subscribe(GATEWAY_RPC_TOPIC + "/+")[1])] = GATEWAY_RPC_TOPIC

    def _on_subscribe(self, client, userdata, mid, granted_qos):
        subscription = self._gw_subscriptions.get(mid)
        if subscription is not None:
            if mid == 128:
                log.error("Service subscription to topic %s - failed.", subscription)
                del(self._gw_subscriptions[mid])
            else:
                log.debug("Service subscription to topic %s - successfully completed.", subscription)
                del(self._gw_subscriptions[mid])

    def get_subscriptions_in_progress(self):
        return True if self._gw_subscriptions else False

    def _on_message(self, client, userdata, message):
        content = TBUtility.decode(message)
        super()._on_decoded_message(content, message)
        self._on_decoded_message(content, message)

    def _on_decoded_message(self, content, message):
        if message.topic.startswith(GATEWAY_ATTRIBUTES_RESPONSE_TOPIC):
            with self._lock:
                req_id = content["id"]
                # pop callback and use it
                if self._attr_request_dict[req_id]:
                    self._attr_request_dict.pop(req_id)(content, None)
                else:
                    log.error("Unable to find callback to process attributes response from TB")
        elif message.topic == GATEWAY_ATTRIBUTES_TOPIC:
            with self._lock:
                # callbacks for everything
                if self.__sub_dict.get("*|*"):
                    for x in self.__sub_dict["*|*"]:
                        self.__sub_dict["*|*"][x](content)
                # callbacks for device. in this case callback executes for all attributes in message
                target = content["device"] + "|*"
                if self.__sub_dict.get(target):
                    for x in self.__sub_dict[target]:
                        self.__sub_dict[target][x](content)
                # callback for atr. in this case callback executes for all attributes in message
                targets = [content["device"] + "|" + x for x in content["data"]]
                for target in targets:
                    if self.__sub_dict.get(target):
                        for sub_id in self.__sub_dict[target]:
                            self.__sub_dict[target][sub_id](content)
        elif message.topic == GATEWAY_RPC_TOPIC:
            if self.devices_server_side_rpc_request_handler:
                self.devices_server_side_rpc_request_handler(self, content)

    def __request_attributes(self, device, keys, callback, type_is_client=False):
        if not keys:
            log.error("There are no keys to request")
            return False
        keys_str = ""
        for key in keys:
            keys_str += key + ","
        keys_str = keys_str[:len(keys_str) - 1]
        ts_in_millis = int(round(time.time() * 1000))
        attr_request_number = self._add_attr_request_callback(callback)
        msg = {"key": keys_str,
               "device": device,
               "client": type_is_client,
               "id": attr_request_number}
        info = self._client.publish(GATEWAY_ATTRIBUTES_REQUEST_TOPIC, dumps(msg), 1)
        self._add_timeout(attr_request_number, ts_in_millis + 30000)
        return info

    def gw_request_shared_attributes(self, device_name, keys, callback):
        return self.__request_attributes(device_name, keys, callback, False)

    def gw_request_client_attributes(self, device_name, keys, callback):
        return self.__request_attributes(device_name, keys, callback, True)

    def gw_send_attributes(self, device, attributes, quality_of_service=1):
        # self.validate(KV_VALIDATOR, attributes)
        return self.publish_data({device: attributes}, GATEWAY_MAIN_TOPIC + "attributes", quality_of_service)

    def gw_send_telemetry(self, device, telemetry, quality_of_service=1):
        if type(telemetry) is not list:
            telemetry = [telemetry]
        # self.validate(DEVICE_TS_KV_VALIDATOR, telemetry)
        return self.publish_data({device: telemetry}, GATEWAY_MAIN_TOPIC + "telemetry", quality_of_service, )

    #TODO ADD "type" to connection request

    def gw_connect_device(self, device_name, device_type):
        info = self._client.publish(topic=GATEWAY_MAIN_TOPIC + "connect", payload=dumps({"device": device_name, "type": device_type}), qos=1)
        self.__connected_devices.add(device_name)
        # if self.gateway:
        #     self.gateway.on_device_connected(device_name, self.__devices_server_side_rpc_request_handler)
        log.debug("Connected device {name}".format(name=device_name))
        return info

    def gw_disconnect_device(self, device_name):
        info = self._client.publish(topic=GATEWAY_MAIN_TOPIC + "disconnect", payload=dumps({"device": device_name}),
                                    qos=1)
        self.__connected_devices.remove(device_name)
        if self.gateway:
            self.gateway.on_device_disconnected(self, device_name)
        log.debug("Disconnected device {name}".format(name=device_name))
        return info

    def gw_subscribe_to_all_attributes(self, callback):
        return self.gw_subscribe_to_attribute("*", "*", callback)

    def gw_subscribe_to_all_device_attributes(self, device, callback):
        return self.gw_subscribe_to_attribute(device, "*", callback)

    def gw_subscribe_to_attribute(self, device, attribute, callback):
        if device not in self.__connected_devices:
            log.error("Device {name} not connected".format(name=device))
            return False
        with self._lock:
            self.__max_sub_id += 1
            key = device + "|" + attribute
            if key not in self.__sub_dict:
                self.__sub_dict.update({key: {self.__max_sub_id: callback}})
            else:
                self.__sub_dict[key].update({self.__max_sub_id: callback})
            log.debug("Subscribed to {key} with id {id}".format(key=key, id=self.__max_sub_id))
            return self.__max_sub_id

    def gw_unsubscribe(self, subscription_id):
        with self._lock:
            for x in self.__sub_dict:
                if self.__sub_dict[x].get(subscription_id):
                    del self.__sub_dict[x][subscription_id]
                    log.debug("Unsubscribed from {attribute}, subscription id {sub_id}".format(attribute=x,
                                                                                               sub_id=subscription_id))
            if subscription_id == '*':
                self.__sub_dict = {}

    def gw_set_server_side_rpc_request_handler(self, handler):
        self.devices_server_side_rpc_request_handler = handler

    def gw_send_rpc_reply(self, device, req_id, resp, quality_of_service=1):
        if quality_of_service != 0 and quality_of_service != 1:
            log.error("Quality of service (qos) value must be 0 or 1")
            return
        info = self._client.publish(GATEWAY_RPC_TOPIC,
                                    dumps({"device": device, "id": req_id, "data": resp}),
                                    qos=quality_of_service)
        return info
