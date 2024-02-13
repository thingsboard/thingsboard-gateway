#      Copyright 2024. ThingsBoard
#  #
#      Licensed under the Apache License, Version 2.0 (the "License");
#      you may not use this file except in compliance with the License.
#      You may obtain a copy of the License at
#  #
#          http://www.apache.org/licenses/LICENSE-2.0
#  #
#      Unless required by applicable law or agreed to in writing, software
#      distributed under the License is distributed on an "AS IS" BASIS,
#      WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#      See the License for the specific language governing permissions and
#      limitations under the License.

import asyncio
import logging
from threading import Thread
from time import sleep

import grpc
from simplejson import dumps

from thingsboard_gateway.gateway.constant_enums import DownlinkMessageType, Status
from thingsboard_gateway.gateway.grpc_service.grpc_downlink_converter import GrpcDownlinkConverter
from thingsboard_gateway.gateway.grpc_service.grpc_uplink_converter import GrpcUplinkConverter
from thingsboard_gateway.gateway.grpc_service.tb_grpc_server import TBGRPCServer
from thingsboard_gateway.gateway.proto.messages_pb2 import *
from thingsboard_gateway.gateway.proto.messages_pb2_grpc import add_TBGatewayProtoServiceServicer_to_server

log = logging.getLogger('grpc')

DEFAULT_STATISTICS_DICT = {"MessagesReceived": 0, "MessagesSent": 0}


class TBGRPCServerManager(Thread):
    def __init__(self, gateway, config):
        super().__init__()
        self.daemon = True
        self.__gateway = gateway
        self.name = "TB GRPC manager thread"
        self.__aio_server: grpc.aio.Server = None
        self.__register_connector = None
        self.__unregister_connector = None
        self.__send_data_to_storage = None
        self._stopped = False
        self.__config = config
        self.__grpc_port = config['serverPort']
        self.__connectors_sessions = {}
        self.__grpc_server = TBGRPCServer(self.incoming_messages_cb)
        self.__uplink_converter = GrpcUplinkConverter()
        self.__downlink_converter = GrpcDownlinkConverter()
        self.sessions = {}
        self.start()

    def run(self):
        log.info("GRPC server started.")
        asyncio.run(self.serve(self.__config), debug=True)
        while not self._stopped:
            sleep(.2)

    def incoming_messages_cb(self, session_id, msg: FromConnectorMessage):
        log.debug("Connected client with identifier: %s", session_id)
        # if session_id not in self.sessions:
        #     self.sessions[session_id] = {"context": context}
        # else:
        #     log.debug("Existing client context is: %s", self.sessions[session_id])
        #     self.sessions[session_id]["context"] = context
        log.debug("[GRPC] incoming message: %s", msg)
        try:
            outgoing_message = None
            downlink_converter_config = {"message_type": [DownlinkMessageType.Response], "additional_message": msg}
            if msg.HasField("registerConnectorMsg"):
                self.__register_connector(session_id, msg.registerConnectorMsg.connectorKey)
                outgoing_message = True
            elif msg.HasField("unregisterConnectorMsg"):
                self.__unregister_connector(session_id, msg.unregisterConnectorMsg.connectorKey)
                outgoing_message = True
            elif self.sessions.get(session_id) is not None and self.sessions[session_id].get('name') is not None:
                if msg.HasField("response"):
                    if msg.response.ByteSize() == 0:
                        outgoing_message = True
                if msg.HasField("connectorGetConnectedDevicesMsg"):
                    connector_id = list(self.__connectors_sessions.keys())[
                        list(self.__connectors_sessions.values()).index(session_id)]
                    connected_devices = self.__get_connector_devices(connector_id)
                    downlink_converter_config = {
                        "message_type": [DownlinkMessageType.ConnectorGetConnectedDevicesResponseMsg],
                        "additional_message": connected_devices}
                    outgoing_message = self.__downlink_converter.convert(downlink_converter_config, None)
                if msg.HasField("gatewayTelemetryMsg"):
                    data = self.__convert_with_uplink_converter(msg.gatewayTelemetryMsg)
                    result_status = self.__gateway.send_to_storage(self.sessions[session_id]['name'], self.sessions[session_id]['id'], data)
                    outgoing_message = True
                    self.__increase_incoming_statistic(session_id)
                if msg.HasField("gatewayAttributesMsg"):
                    data = self.__convert_with_uplink_converter(msg.gatewayAttributesMsg)
                    result_status = self.__gateway.send_to_storage(self.sessions[session_id]['name'], self.sessions[session_id]['id'], data)
                    outgoing_message = True
                    self.__increase_incoming_statistic(session_id)
                if msg.HasField("gatewayClaimMsg"):
                    data = self.__convert_with_uplink_converter(msg.gatewayClaimMsg)
                    result_status = self.__gateway.send_to_storage(self.sessions[session_id]['name'], self.sessions[session_id]['id'], data)
                    outgoing_message = self.__downlink_converter.convert(downlink_converter_config, result_status)
                    self.__increase_incoming_statistic(session_id)
                if msg.HasField("connectMsg"):
                    data = self.__convert_with_uplink_converter(msg.connectMsg)
                    data['name'] = self.sessions[session_id]['name']
                    result_status = self.__gateway.add_device_async(data)
                    outgoing_message = self.__downlink_converter.convert(downlink_converter_config, result_status)
                    self.__increase_incoming_statistic(session_id)
                if msg.HasField("disconnectMsg"):
                    data = self.__convert_with_uplink_converter(msg.disconnectMsg)
                    data['name'] = self.sessions[session_id]['name']
                    result_status = self.__gateway.del_device_async(data)
                    outgoing_message = self.__downlink_converter.convert(downlink_converter_config, result_status)
                    self.__increase_incoming_statistic(session_id)
                if msg.HasField("gatewayRpcResponseMsg"):
                    data = self.__convert_with_uplink_converter(msg.gatewayRpcResponseMsg)
                    result_status = self.__gateway.send_rpc_reply(device=data['deviceName'], req_id=data['id'],
                                                                  content=data['data'])
                    outgoing_message = True
                    self.__increase_incoming_statistic(session_id)
                if msg.HasField("gatewayAttributeRequestMsg"):
                    shared_keys = None
                    client_keys = None
                    device_name = msg.gatewayAttributeRequestMsg.deviceName
                    request_id = msg.gatewayAttributeRequestMsg.id
                    is_client = msg.gatewayAttributeRequestMsg.client
                    keys = list(msg.gatewayAttributeRequestMsg.keys)
                    if is_client:
                        client_keys = keys
                    else:
                        shared_keys = keys
                    callback_with_extra_params = (self.__process_requested_attributes,
                                                  {"request_id": request_id, "session_id": session_id,
                                                   "device_name": device_name, "client": is_client})
                    if len(keys) == 1:
                        callback_with_extra_params[1]["key"] = keys[0]
                    self.__gateway.request_device_attributes(device_name,
                                                             shared_keys,
                                                             client_keys,
                                                             callback_with_extra_params
                                                             )
                    outgoing_message = True
                    self.__increase_incoming_statistic(session_id)
            else:
                outgoing_message = self.__downlink_converter.convert(downlink_converter_config, Status.FAILURE)
            if outgoing_message is None:
                log.debug("Cannot convert outgoing message!")
            elif isinstance(outgoing_message, FromServiceMessage):
                self.__grpc_server.write(session_id, outgoing_message)
        except ValueError as e:
            log.error("Received unknown GRPC message!", e)

    def write(self, connector_name, connector_id, msg: FromServiceMessage, session_id=None):
        log.debug("[GRPC] outgoing message: %s", msg)
        if session_id is None:
            session_id = self.__connectors_sessions.get(connector_id)
        if session_id is not None:
            self.__grpc_server.write(session_id, msg)
            self.__increase_outgoing_statistic(session_id)
        else:
            log.warning("[%r] Cannot write to connector with name %s, session is not found. Client is not registered!",
                        connector_id, connector_name)

    def registration_finished(self, registration_result: Status, session_id, connector_configuration):
        additional_message = FromConnectorMessage()
        additional_message.registerConnectorMsg.MergeFrom(RegisterConnectorMsg())
        if registration_result == Status.SUCCESS:
            connector_name = connector_configuration['name']
            connector_id = connector_configuration['id']
            self.sessions[session_id] = {"config": connector_configuration, "name": connector_name,
                                         "id": connector_id, "statistics": DEFAULT_STATISTICS_DICT}
            self.__connectors_sessions[connector_id] = session_id
            msg = self.__grpc_server.get_response("SUCCESS", additional_message)
            configuration_msg = ConnectorConfigurationMsg()
            configuration_msg.connectorName = connector_name
            configuration_msg.connectorId = connector_id
            configuration_msg.configuration = dumps(connector_configuration['config'])
            msg.connectorConfigurationMsg.MergeFrom(configuration_msg)
            self.__grpc_server.write(session_id, msg)
            log.debug('Connector "%s" configuration sent!', connector_name)
        elif registration_result == Status.NOT_FOUND:
            msg = self.__grpc_server.get_response("NOT_FOUND", additional_message)
            self.__grpc_server.write(session_id, msg)
        elif registration_result == Status.FAILURE:
            msg = self.__grpc_server.get_response("FAILURE", additional_message)
            self.__grpc_server.write(session_id, msg)

    def unregister(self, unregistration_result: Status, session_id, connector):
        additional_message = FromConnectorMessage()
        additional_message.unregisterConnectorMsg.MergeFrom(UnregisterConnectorMsg())
        if unregistration_result == Status.SUCCESS:
            connector_name = connector.get_name()
            connector_id = connector.get_id()
            connector_session_id = self.__connectors_sessions.pop(connector_id)
            del self.sessions[connector_session_id]
            msg = self.__grpc_server.get_response("SUCCESS", additional_message)
            self.__grpc_server.write(session_id, msg)
        elif unregistration_result == Status.NOT_FOUND:
            msg = self.__grpc_server.get_response("NOT_FOUND", additional_message)
            self.__grpc_server.write(session_id, msg)
        elif unregistration_result == Status.FAILURE:
            msg = self.__grpc_server.get_response("FAILURE", additional_message)
            self.__grpc_server.write(session_id, msg)

    def get_connector_statistics(self, session_id):
        if session_id in self.sessions:
            return self.sessions[session_id].get('statistics', DEFAULT_STATISTICS_DICT)
        else:
            return DEFAULT_STATISTICS_DICT

    def __process_requested_attributes(self, content, error, extra_params):
        log.debug("Received requested attributes")
        if error:
            log.error(error)
        downlink_converter_config = {"message_type": [DownlinkMessageType.GatewayAttributeResponseMsg], "additional_message": {**extra_params, "error": str(error)}}
        outgoing_message = self.__downlink_converter.convert(downlink_converter_config, content)
        self.__grpc_server.write(extra_params['session_id'], outgoing_message)

    def __convert_with_uplink_converter(self, data):
        return self.__uplink_converter.convert(None, data)

    def __increase_incoming_statistic(self, session_id):
        if session_id in self.sessions:
            self.sessions[session_id]['statistics']["MessagesReceived"] += 1

    def __increase_outgoing_statistic(self, session_id):
        if session_id in self.sessions:
            self.sessions[session_id]['statistics']["MessagesSent"] += 1

    async def serve(self, config):
        self.__aio_server = grpc.aio.server(
            options=(
                ('grpc.keepalive_time_ms', config.get('keepaliveTimeMs', 10000)),
                ('grpc.keepalive_timeout_ms', config.get('keepaliveTimeoutMs', 5000)),
                ('grpc.keepalive_permit_without_calls', config.get('keepalivePermitWithoutCalls', True)),
                ('grpc.http2.max_pings_without_data', config.get('maxPingsWithoutData', 0)),
                ('grpc.http2.min_time_between_pings_ms', config.get('minTimeBetweenPingsMs', 10000)),
                ('grpc.http2.min_ping_interval_without_data_ms', config.get('minPingIntervalWithoutDataMs', 5000)),
            ))
        add_TBGatewayProtoServiceServicer_to_server(self.__grpc_server, self.__aio_server)
        self.__aio_server.add_insecure_port("[::]:%s" % (self.__grpc_port,))
        await self.__aio_server.start()
        await self.__aio_server.wait_for_termination()

    def stop(self):
        self._stopped = True
        if self.__aio_server is not None:
            loop = asyncio.get_event_loop()
            loop.create_task(self.__aio_server.stop(True))

    def __get_connector_devices(self, connector_id: str):
        return self.__gateway.get_devices(connector_id)

    def set_gateway_read_callbacks(self, registration_cb, unregistration_cb):
        self.__register_connector = registration_cb
        self.__unregister_connector = unregistration_cb
