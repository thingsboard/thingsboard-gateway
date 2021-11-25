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
import grpc
import logging
from threading import Thread
from time import sleep
from enum import Enum
from simplejson import dumps

from thingsboard_gateway.gateway.proto.messages_pb2_grpc import add_TBGatewayProtoServiceServicer_to_server
from thingsboard_gateway.gateway.proto.messages_pb2 import *
from thingsboard_gateway.gateway.grpc_service.tb_grpc_server import TBGRPCServer


log = logging.getLogger('service')


class RegistrationStatus(Enum):
    FAILURE = 1,
    NOT_FOUND = 2,
    SUCCESS = 3


class TBGRPCServerManager(Thread):
    def __init__(self, config):
        super().__init__()
        self.daemon = True
        self.setName("TB GRPC manager thread")
        self.__aio_server: grpc.aio.Server = None
        self.__register_connector = None
        self.__send_data_to_storage = None
        self._stopped = False
        self.__config = config
        self.__grpc_port = config['serverPort']
        self.__connectors_sessions = {}
        self.__grpc_server = TBGRPCServer(self.read_cb)
        self.start()

    def run(self):
        log.info("GRPC server started.")
        asyncio.run(self.serve(), debug=True)
        while not self._stopped:
            sleep(.1)

    def read_cb(self, context, msg: FromConnectorMessage):
        log.debug("[GRPC] incoming message: %s", msg)
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
            pass
        if msg.HasField("connectMsg"):
            pass
        if msg.HasField("disconnectMsg"):
            pass
        if msg.HasField("gatewayRpcResponseMsg"):
            pass
        if msg.HasField("gatewayAttributeRequestMsg"):
            pass
        # self.__send_data_to_storage()
        # self.write("", "")

    def write(self, connector_name, data):
        log.debug("[GRPC] outgoing message: %s", data)
        if self.__connectors_sessions.get(connector_name) is not None:
            self.__grpc_server.write(self.__grpc_server.get_response('SUCCESS'))

    def registration_finished(self, registration_result: RegistrationStatus, context, connector_configuration):
        if registration_result == RegistrationStatus.SUCCESS:
            connector_name = connector_configuration['name']
            self.__connectors_sessions[connector_name] = {"context": context, "config": connector_configuration}
            msg = self.__grpc_server.get_response("SUCCESS")
            configuration_msg = ConnectorConfigurationMsg()
            configuration_msg.connectorName = connector_name
            configuration_msg.configuration = dumps(connector_configuration['config'])
            msg.connectorConfigurationMsg.MergeFrom(configuration_msg)
            self.__grpc_server.write(msg)
        elif registration_result == RegistrationStatus.NOT_FOUND:
            msg = self.__grpc_server.get_response("NOT_FOUND")
            self.__grpc_server.write(msg)
        elif registration_result == RegistrationStatus.FAILURE:
            msg = self.__grpc_server.get_response("FAILURE")
            self.__grpc_server.write(msg)

    async def serve(self):
        self.__aio_server = grpc.aio.server()
        add_TBGatewayProtoServiceServicer_to_server(self.__grpc_server, self.__aio_server)
        self.__aio_server.add_insecure_port("[::]:%s" % (self.__grpc_port,))
        await self.__aio_server.start()
        await self.__aio_server.wait_for_termination()

    def stop(self):
        self._stopped = True
        if self.__aio_server is not None:
            loop = asyncio.get_event_loop()
            loop.create_task(self.__aio_server.stop(True))

    def set_gateway_read_callbacks(self, register, send_data_to_storage):
        self.__register_connector = register
        self.__send_data_to_storage = send_data_to_storage


if __name__ == '__main__':
    test_conf = {"serverPort": 9595}
    TBGRPCServerManager(test_conf)
