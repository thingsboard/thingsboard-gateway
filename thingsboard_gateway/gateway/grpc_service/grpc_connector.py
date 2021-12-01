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


from thingsboard_gateway.connectors.connector import Connector
from thingsboard_gateway.gateway.grpc_service.tb_grpc_manager import TBGRPCServerManager


class GrpcConnector(Connector):
    def __init__(self, gateway, config, tb_grpc_server_manager: TBGRPCServerManager):
        self.name = None
        self.__server_manager = tb_grpc_server_manager

    def setName(self, name):
        self.name = name

    def open(self):
        pass

    def close(self):
        # send unregister
        pass

    def get_name(self):
        return self.name

    def is_connected(self):
        pass

    def on_attributes_update(self, content):
        # send updated
        pass

    def server_side_rpc_handler(self, content):
        # send command
        pass
