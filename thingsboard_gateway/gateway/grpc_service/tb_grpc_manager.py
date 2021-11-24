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
from time import sleep

from thingsboard_gateway.gateway.proto.messages_pb2_grpc import add_TBGatewayProtoServiceServicer_to_server
from thingsboard_gateway.gateway.proto.messages_pb2 import FromConnectorMessage
from thingsboard_gateway.gateway.grpc_service.tb_grpc_server import TBGRPCServer


log = logging.getLogger('service')


class TBGRPCServerManager:
    def __init__(self, config):
        self.__aio_server = None
        self.__register_connector = None
        self.__send_data_to_storage = None
        self._stopped = False
        self.__config = config
        self.__grpc_port = config['serverPort']
        self.__connectors_sessions = {}
        self.__grpc_server = TBGRPCServer(self.read_cb, self.write_cb)
        asyncio.run(self.serve(), debug=True)
        while not self._stopped:
            sleep(.1)

    def write_cb(self):
        pass

    def read_cb(self, context, msg:FromConnectorMessage):
        #TODO parse incoming message
        self.__send_data_to_storage()
        self.write("", "")

    def write(self, connector_name, data):
        # if self.__connectors_sessions.get(connector_name) is not None:
            self.__grpc_server.write(self.__grpc_server.get_response('SUCCESS'))

    async def serve(self):
        self.__aio_server = grpc.aio.server()
        add_TBGatewayProtoServiceServicer_to_server(self.__grpc_server, self.__aio_server)
        self.__aio_server.add_insecure_port("[::]:%s" % (self.__grpc_port,))
        await self.__aio_server.start()
        await self.__aio_server.wait_for_termination()

    def stop(self):
        self._stopped = True
        if self.__aio_server is not None:
            self.__aio_server.stop()

    def set_gateway_read_callbacks(self, register, send_data_to_storage):
        self.__register_connector = register
        self.__send_data_to_storage = send_data_to_storage


if __name__ == '__main__':
    test_conf = {"serverPort": 9595}
    TBGRPCServerManager(test_conf)
