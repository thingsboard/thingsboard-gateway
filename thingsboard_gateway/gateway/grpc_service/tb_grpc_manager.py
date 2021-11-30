#      Copyright 2021. ThingsBoard
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
from typing import Optional

import grpc
import logging
from threading import Thread
from time import sleep
from enum import Enum
from simplejson import dumps

from thingsboard_gateway.gateway.grpc_service.grpc_connector import GrpcConnector
from thingsboard_gateway.gateway.grpc_service.grpc_downlink_converter import GrpcDownlinkConverter
from thingsboard_gateway.gateway.grpc_service.grpc_uplink_converter import GrpcUplinkConverter
from thingsboard_gateway.gateway.constant_enums import DownlinkMessageType, UplinkMessageType

from thingsboard_gateway.gateway.proto.messages_pb2_grpc import add_TBGatewayProtoServiceServicer_to_server
from thingsboard_gateway.gateway.proto.messages_pb2 import *
from thingsboard_gateway.gateway.grpc_service.tb_grpc_server import TBGRPCServer

log = logging.getLogger('service')


class Status(Enum):
    FAILURE = 1,
    NOT_FOUND = 2,
    SUCCESS = 3


class TBGRPCServerManager(Thread):
    def __init__(self, gateway, config):
        super().__init__()
        self.daemon = True
        self.__gateway = gateway
        self.setName("TB GRPC manager thread")
        self.__aio_server: grpc.aio.Server = None
        self.__register_connector = None
        self.__unregister_connector = None
        self.__send_data_to_storage = None
        self._stopped = False
        self.__config = config
        self.__grpc_port = config['serverPort']
        self.__connectors_sessions = {}
        self.__grpc_server = TBGRPCServer(self.read_cb)
        self.__uplink_converter = GrpcUplinkConverter()
        self.__downlink_converter = GrpcDownlinkConverter()
        self.sessions = {}
        self.start()

    def run(self):
        log.info("GRPC server started.")
        asyncio.run(self.serve(), debug=True)
        while not self._stopped:
            sleep(.1)

    def read_cb(self, context, msg: FromConnectorMessage):
        log.debug("Connected client with peer address: %s", context.peer())
        if context.peer() not in self.sessions:
            self.sessions[context.peer()] = {"context": context}
        else:
            log.debug("Existing client context is: %s", self.sessions[context.peer()])
        log.debug("[GRPC] incoming message: %s", msg)
        try:
            if msg.HasField("response"):
                pass
            if msg.HasField("gatewayTelemetryMsg"):
                pass
            if msg.HasField("gatewayAttributesMsg"):
                pass
            if msg.HasField("gatewayClaimMsg"):
                pass
            if msg.HasField("registerConnectorMsg"):
                self.__register_connector(context, msg.registerConnectorMsg.connectorKey)
            if msg.HasField("unregisterConnectorMsg"):
                self.__unregister_connector(context, msg.unregisterConnectorMsg.connectorKey)
            if msg.HasField("connectMsg"):
                data = self.__uplink_converter.convert(UplinkMessageType.ConnectMsg, msg.connectMsg)
                data['name'] = self.sessions[context.peer()]['name']
                result_status = self.__gateway.add_device_async(data)
                outgoing_message = self.__downlink_converter.convert([DownlinkMessageType.Response], result_status)
                if not outgoing_message:
                    log.error("Cannot convert outgoing message!")
                self.__grpc_server.write(context, outgoing_message)
            if msg.HasField("disconnectMsg"):
                pass
            if msg.HasField("gatewayRpcResponseMsg"):
                pass
            if msg.HasField("gatewayAttributeRequestMsg"):
                pass
        except ValueError as e:
            log.error("Received unknown GRPC message!", e)
        # self.__send_data_to_storage()
        # self.write("", "")

    def write(self, connector_name, msg: FromServiceMessage):
        log.debug("[GRPC] outgoing message: %s", msg)
        grpc_client_peer = self.__connectors_sessions.get(connector_name)

        if grpc_client_peer is not None:
            grpc_session = self.sessions[grpc_client_peer]
            self.__grpc_server.write(grpc_session['context'], msg)
        else:
            log.warning("Cannot write to connector with name %s, session is not found. Client is not registered!", connector_name)

    def registration_finished(self, registration_result: Status, context, connector_configuration):
        if registration_result == Status.SUCCESS:
            connector_name = connector_configuration['name']
            self.sessions[context.peer()].update({"config": connector_configuration,
                                                  "name": connector_name})
            self.__connectors_sessions[connector_name] = context.peer()
            msg = self.__grpc_server.get_response("SUCCESS")
            configuration_msg = ConnectorConfigurationMsg()
            configuration_msg.connectorName = connector_name
            configuration_msg.configuration = dumps(connector_configuration['config'])
            msg.connectorConfigurationMsg.MergeFrom(configuration_msg)
            self.__grpc_server.write(context, msg)
        elif registration_result == Status.NOT_FOUND:
            msg = self.__grpc_server.get_response("NOT_FOUND")
            self.__grpc_server.write(context, msg)
        elif registration_result == Status.FAILURE:
            msg = self.__grpc_server.get_response("FAILURE")
            self.__grpc_server.write(context, msg)

    def unregister(self, unregistration_result: Status, context, connector: Optional[GrpcConnector]):
        if unregistration_result == Status.SUCCESS:
            connector_name = connector.get_name()
            grpc_client_peer = self.__connectors_sessions.pop(connector_name)
            connector_session = self.sessions.pop(grpc_client_peer)
            msg = self.__grpc_server.get_response("SUCCESS")
            self.__grpc_server.write(context, msg)
        elif unregistration_result == Status.NOT_FOUND:
            msg = self.__grpc_server.get_response("NOT_FOUND")
            self.__grpc_server.write(context, msg)
        elif unregistration_result == Status.FAILURE:
            msg = self.__grpc_server.get_response("FAILURE")
            self.__grpc_server.write(context, msg)

    async def serve(self):
        self.__aio_server = grpc.aio.server(
            options=(
                ('grpc.keepalive_time_ms', 10000),
                ('grpc.keepalive_timeout_ms', 5000),
                ('grpc.keepalive_permit_without_calls', True),
                ('grpc.http2.max_pings_without_data', 0),
                ('grpc.http2.min_time_between_pings_ms', 10000),
                ('grpc.http2.min_ping_interval_without_data_ms', 5000),
            ))
        add_TBGatewayProtoServiceServicer_to_server(self.__grpc_server, self.__aio_server)
        self.__aio_server.add_insecure_port("[::]:%s" % (self.__grpc_port,))
        await self.__aio_server.start()
        await self.__aio_server.wait_for_termination()

    def stop(self):
        self._stopped = True
        self.__grpc_server.stop()
        if self.__aio_server is not None:
            loop = asyncio.get_event_loop()
            loop.create_task(self.__aio_server.stop(True))

    def set_gateway_read_callbacks(self, registeration_cb, unregistration_cb, send_data_cb):
        self.__register_connector = registeration_cb
        self.__unregister_connector = unregistration_cb
        self.__send_data_to_storage = send_data_cb


if __name__ == '__main__':
    test_conf = {"serverPort": 9595}
    TBGRPCServerManager(test_conf)
