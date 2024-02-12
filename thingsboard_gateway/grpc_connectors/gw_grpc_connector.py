#     Copyright 2024. ThingsBoard
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

from logging import getLogger
from logging.config import fileConfig
from threading import Thread
from time import sleep, time
from typing import Union

import simplejson
from simplejson import dumps

from thingsboard_gateway.gateway.proto.messages_pb2 import *
from thingsboard_gateway.grpc_connectors.gw_grpc_client import GrpcClient
from thingsboard_gateway.grpc_connectors.gw_grpc_msg_creator import GrpcMsgCreator, Status

log = getLogger('connector')


class GwGrpcConnector(Thread):
    def __init__(self, connector_config: str, config_dir_path: str):
        super().__init__()
        fileConfig(config_dir_path + 'logs.json')
        global log
        log = getLogger('connector')
        self.stopped = False
        self.__started = False
        self.registered = False
        self.connection_config = simplejson.loads(connector_config)
        self.__connector_id = None
        self.__connector_name = None
        self.__received_configuration = None
        self.__registration_request_sent = False
        self.__connected_devices_requested = False
        self.__connected_devices_info = {}
        self.__attributes_request_id_counter = 0
        if self.connection_config is None:
            return
        self._grpc_client = GrpcClient(self.__on_connect,
                                       self._incoming_messages_callback,
                                       self.connection_config['gateway']['host'],
                                       self.connection_config['gateway']['port'])
        self.__connection_thread = Thread(target=self.connect,
                                          name="Registration thread",
                                          daemon=True)
        self.__connection_thread.start()
        self.__request_data_thread = Thread(target=self.__check_incoming_data,
                                            name="Request data thread",
                                            daemon=True)
        self.__request_data_thread.start()
        log.info("Connector initialized")

    def start(self) -> None:
        self.__started = True
        self._grpc_client.start()
        super().start()

    def connect(self) -> None:
        interval = self.connection_config['gateway'].get("reconnectDelay", 60)
        previous_check = 0
        while not self.stopped:
            if not self.registered and time() - previous_check > interval and (not self.__registration_request_sent or time() - previous_check > interval*2):
                log.debug("Reconnecting...")
                self.__connect_to_gateway()
                previous_check = time()
            sleep(.2)

    def __on_connect(self):
        self.__registration_request_sent = False
        self.registered = False

    def __check_incoming_data(self) -> None:
        interval = self.connection_config['gateway'].get("checkIncomingDataIntervalMs", 10000)
        previous_sent = time()*1000
        while not self.stopped:
            cur_time = time()*1000
            if (self.registered and cur_time - self._grpc_client.last_response_received_time >= interval) or (self.__registration_request_sent and cur_time-previous_sent >= 10):
                previous_sent = time()*1000
                self.__request_data()
            else:
                sleep(1)

    def run(self):
        while not self.stopped:
            if not self._grpc_client.connected and self.registered:
                self.registered = False
            sleep(.2)

    def stop(self):
        self.stopped = True
        self._grpc_client.stop()

    def _incoming_messages_callback(self, data):
        if data.HasField('response'):
            if data.response.HasField('connectorMessage'):
                if data.response.connectorMessage.HasField('registerConnectorMsg'):
                    log.debug("Received response for registration message.")
                    if data.response.status == ResponseStatus.SUCCESS:
                        pass
                    elif data.response.status == ResponseStatus.FAILURE and not self.registered:
                        self.__unregister_message_to_gateway()
                        self.__registration_request_sent = False
                if data.response.connectorMessage.HasField('unregisterConnectorMsg'):
                    log.debug("Received response for unregistration message.")
                if data.response.connectorMessage.HasField('connectorGetConnectedDevicesMsg'):
                    self.__connected_devices_requested = False
        if data.HasField("gatewayAttributeUpdateNotificationMsg"):
            log.debug(data.gatewayAttributeUpdateNotificationMsg)
            self.on_attributes_update(data.gatewayAttributeUpdateNotificationMsg)
        if data.HasField("gatewayAttributeResponseMsg"):
            log.debug(data.gatewayAttributeResponseMsg)
            self.on_attributes_update(data.gatewayAttributeResponseMsg)
        if data.HasField("gatewayDeviceRpcRequestMsg"):
            log.debug(data.gatewayDeviceRpcRequestMsg)
            self.server_side_rpc_handler(data.gatewayDeviceRpcRequestMsg)
        if data.HasField("unregisterConnectorMsg"):
            log.debug("Received unregistration message")
            self.registered = False
        if data.HasField("connectorConfigurationMsg"):
            self.registered = True
            self.__registration_request_sent = False
            self.__received_configuration = data.connectorConfigurationMsg.configuration
            self.__connector_id = data.connectorConfigurationMsg.connectorId
            self.__connector_name = data.connectorConfigurationMsg.connectorName
            log.info("[%r] Connector %s connected to ThingsBoard IoT gateway", self.__connector_id, self.__connector_name)
            log.debug("Configuration - received.")
            log.debug(self.__received_configuration)
            if data.HasField('connectorGetConnectedDevicesResponseMsg'):
                log.debug("Received response with connected device infos.")
                log.debug(data.connectorGetConnectedDevicesResponseMsg)
                self.__connected_devices_info = data.connectorGetConnectedDevicesResponseMsg

    def __request_data(self):
        self._grpc_client.send_get_data_message()

    def request_device_attributes(self, device, keys, client_scope=False):
        if self.registered:
            self.__attributes_request_id_counter = self.__attributes_request_id_counter + 1
            message_to_gateway = GrpcMsgCreator.create_attributes_request_connector_msg(device, keys, client_scope,
                                                                                        self.__attributes_request_id_counter)
            log.debug("Sending request attributes message for device %s with keys: %r, and %s scope", device, keys,
                      "CLIENT" if client_scope else "SHARED")
            self._grpc_client.send_service_message(message_to_gateway)

    def __send_response(self, status: Union[Status, None]):
        if status is None:
            status = Status.FAILURE
        message_to_gateway = GrpcMsgCreator.create_response_connector_msg(status)
        self._grpc_client.send_service_message(message_to_gateway)

    def __connect_to_gateway(self):
        if not self.__registration_request_sent:
            message_to_gateway = GrpcMsgCreator.create_register_connector_msg(
                self.connection_config['grpc_key'])
            self._grpc_client.send_service_message(message_to_gateway)
            log.debug("Sending registration message.")
            self.__registration_request_sent = True

    def get_connected_devices(self):
        if self.registered and not self.__connected_devices_requested:
            message_to_gateway = GrpcMsgCreator.create_get_connected_devices_msg(
                self.connection_config['gateway']['connectorKey'])
            self._grpc_client.send_service_message(message_to_gateway)
            log.debug("Sending get devices message.")
            self.__connected_devices_requested = True
        else:
            log.debug("Cannot request connected devices.")

    def __unregister_message_to_gateway(self):
        message_to_gateway = GrpcMsgCreator.create_unregister_connector_msg(
            self.connection_config['grpc_key'])
        self._grpc_client.send_service_message(message_to_gateway)
        log.debug("Sending unregistration message.")

    @staticmethod
    def __not_implemented_method(msg):
        log.error("Not implemented processing for %s type message", msg.DESCRIPTOR.name)

    def on_attributes_update(self, msg: GatewayAttributeUpdateNotificationMsg):
        self.__not_implemented_method(msg)

    def server_side_rpc_handler(self, msg: GatewayDeviceRpcRequestMsg):
        self.__not_implemented_method(msg)
        self.send_rpc_reply(msg.deviceName, msg.rpcRequestMsg.requestId, None, False)

    def send_rpc_reply(self, device=None, req_id=None, content=None, success=None):
        try:
            rpc_response = {}
            if success is not None and isinstance(success, bool):
                rpc_response = {"success": success}
            if content is not None:
                rpc_response.update(content)
            rpc_response = dumps(rpc_response)
            message_to_gateway = GrpcMsgCreator.create_rpc_response_connector_msg(device, req_id, rpc_response)
            self._grpc_client.send_service_message(message_to_gateway)
        except Exception as e:
            log.exception(e)
