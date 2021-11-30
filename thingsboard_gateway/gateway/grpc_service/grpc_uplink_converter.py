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
from typing import Union

from thingsboard_gateway.connectors.converter import Converter, log
from thingsboard_gateway.gateway.constant_enums import UplinkMessageType
from thingsboard_gateway.gateway.proto.messages_pb2 import ConnectMsg, DisconnectMsg, GatewayAttributesMsg, GatewayAttributesRequestMsg, GatewayClaimMsg, \
    GatewayRpcResponseMsg, GatewayTelemetryMsg, RegisterConnectorMsg, Response, UnregisterConnectorMsg


class GrpcUplinkConverter(Converter):
    def __init__(self):
        self.__conversion_methods = {
            UplinkMessageType.Response: self.__convert_response_msg,
            UplinkMessageType.GatewayTelemetryMsg: self.__convert_gateway_telemetry_msg,
            UplinkMessageType.GatewayAttributesMsg: self.__convert_gateway_attributes_msg,
            UplinkMessageType.GatewayClaimMsg: self.__convert_gateway_claim_msg,
            UplinkMessageType.RegisterConnectorMsg: self.__convert_register_connector_msg,
            UplinkMessageType.UnregisterConnectorMsg: self.__convert_unregister_connector_msg,
            UplinkMessageType.ConnectMsg: self.__convert_connect_msg,
            UplinkMessageType.DisconnectMsg: self.__convert_disconnect_msg,
            UplinkMessageType.GatewayRpcResponseMsg: self.__convert_gateway_rpc_response_msg,
            UplinkMessageType.GatewayAttributesRequestMsg: self.__convert_gateway_attributes_request_msg
            }

    def convert(self, config, data):
        try:
            if self.__conversion_methods.get(config) is not None:
                return self.__conversion_methods[config](data)
            else:
                log.error("[GRPC] unknown uplink message type: %r", config)
                return {}
        except Exception as e:
            log.exception("[GRPC] ", e)
            return {}

    def __convert_response_msg(self, msg: Response):
        pass

    def __convert_gateway_telemetry_msg(self, msg: GatewayTelemetryMsg):
        pass

    def __convert_gateway_attributes_msg(self, msg: GatewayAttributesMsg):
        pass

    def __convert_gateway_claim_msg(self, msg: GatewayClaimMsg):
        pass

    def __convert_register_connector_msg(self, msg: RegisterConnectorMsg):
        pass

    def __convert_unregister_connector_msg(self, msg: UnregisterConnectorMsg):
        pass

    @staticmethod
    def __convert_connect_msg(msg: ConnectMsg) -> dict:
        result_dict = {'deviceName': msg.deviceName, 'deviceType': msg.deviceType}
        return result_dict

    def __convert_disconnect_msg(self, msg: DisconnectMsg):
        pass

    def __convert_gateway_rpc_response_msg(self, msg: GatewayRpcResponseMsg):
        pass

    def __convert_gateway_attributes_request_msg(self, msg: GatewayAttributesRequestMsg):
        pass
