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


from thingsboard_gateway.connectors.connector import Connector
from thingsboard_gateway.gateway.constant_enums import DownlinkMessageType
from thingsboard_gateway.gateway.grpc_service.tb_grpc_manager import TBGRPCServerManager
from thingsboard_gateway.gateway.grpc_service.grpc_downlink_converter import GrpcDownlinkConverter


class GrpcConnector(Connector):
    def __init__(self, gateway, config, tb_grpc_server_manager: TBGRPCServerManager, session_id):
        self.name = None
        self.__id = config.get("id")
        self.__server_manager = tb_grpc_server_manager
        self.__session_id = session_id
        self.__downlink_converter = GrpcDownlinkConverter()

    def setName(self, name):
        self.name = name

    def open(self):
        pass

    def close(self):
        converter_config = {"message_type": DownlinkMessageType.UnregisterConnectorMsg}
        message_to_connector = self.__downlink_converter.convert(converter_config, "")
        self.__server_manager.write(self.name, message_to_connector, self.__session_id)

    def get_id(self):
        return self.__id

    def get_name(self):
        return self.name

    def is_connected(self):
        pass

    def on_attributes_update(self, content):
        converter_config = {"message_type": DownlinkMessageType.GatewayAttributeUpdateNotificationMsg}
        message_to_connector = self.__downlink_converter.convert(converter_config, content)
        self.__server_manager.write(self.name, message_to_connector, self.__session_id)

    def server_side_rpc_handler(self, content):
        converter_config = {"message_type": DownlinkMessageType.GatewayDeviceRpcRequestMsg}
        message_to_connector = self.__downlink_converter.convert(converter_config, content)
        self.__server_manager.write(self.name, message_to_connector, self.__session_id)

    @property
    def statistics(self):
        return self.__server_manager.get_connector_statistics(self.__session_id)
