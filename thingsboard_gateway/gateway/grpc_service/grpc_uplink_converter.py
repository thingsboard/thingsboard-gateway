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

from thingsboard_gateway.connectors.converter import Converter, log
from thingsboard_gateway.gateway.proto.messages_pb2 import ConnectMsg, DisconnectMsg, GatewayAttributesMsg, GatewayAttributesRequestMsg, GatewayClaimMsg, \
    GatewayRpcResponseMsg, GatewayTelemetryMsg, KeyValueProto, KeyValueType, Response


class GrpcUplinkConverter(Converter):
    def __init__(self):
        self.__conversion_methods = {
            Response.DESCRIPTOR: self.__convert_response_msg,
            GatewayTelemetryMsg.DESCRIPTOR: self.__convert_gateway_telemetry_msg,
            GatewayAttributesMsg.DESCRIPTOR: self.__convert_gateway_attributes_msg,
            GatewayClaimMsg.DESCRIPTOR: self.__convert_gateway_claim_msg,
            ConnectMsg.DESCRIPTOR: self.__convert_connect_msg,
            DisconnectMsg.DESCRIPTOR: self.__convert_disconnect_msg,
            GatewayRpcResponseMsg.DESCRIPTOR: self.__convert_gateway_rpc_response_msg,
            GatewayAttributesRequestMsg.DESCRIPTOR: self.__convert_gateway_attributes_request_msg
            }

    def convert(self, config, data):
        try:
            if self.__conversion_methods.get(data.DESCRIPTOR) is not None:
                return self.__conversion_methods[data.DESCRIPTOR](data)
            else:
                log.error("[GRPC] unknown uplink message descriptor: %r", data.DESCRIPTOR)
                return {}
        except Exception as e:
            log.exception("[GRPC] ", e)
            return {}

    @staticmethod
    def __convert_response_msg(self, msg: Response):
        log.debug("Converted response: %r", msg.response)

    @staticmethod
    def __convert_gateway_telemetry_msg(msg: GatewayTelemetryMsg):
        result = []
        for telemetry_msg in msg.msg:
            device_dict = {"deviceName": telemetry_msg.deviceName, "telemetry": []}
            for ts_kv_list in telemetry_msg.msg.tsKvList:
                ts_kv_list_dict = {"ts": ts_kv_list.ts, "values": {}}
                for kv in ts_kv_list.kv:
                    ts_kv_list_dict['values'][kv.key] = GrpcUplinkConverter.get_value(kv)
                device_dict['telemetry'].append(ts_kv_list_dict)
            result.append(device_dict)
        return result

    @staticmethod
    def __convert_gateway_attributes_msg(msg: GatewayAttributesMsg):
        result = []
        for attributes_msg in msg.msg:
            device_dict = {"deviceName": attributes_msg.deviceName, "attributes": {}}
            for kv in attributes_msg.msg.kv:
                device_dict['attributes'][kv.key] = GrpcUplinkConverter.get_value(kv)
            result.append(device_dict)
        return result

    @staticmethod
    def __convert_gateway_claim_msg(msg: GatewayClaimMsg):
        result_dict = {}
        for claim_device_msg in msg:
            result_dict.update({claim_device_msg.deviceName: {"secretKey": claim_device_msg.msg.secretKey, "durationMs": claim_device_msg.msg.durationMs}})
        return result_dict

    @staticmethod
    def __convert_connect_msg(msg: ConnectMsg) -> dict:
        result_dict = {'deviceName': msg.deviceName, 'deviceType': msg.deviceType}
        return result_dict

    @staticmethod
    def __convert_disconnect_msg(msg: DisconnectMsg):
        return {"deviceName": msg.deviceName}

    @staticmethod
    def __convert_gateway_rpc_response_msg(msg: GatewayRpcResponseMsg):
        return {"deviceName": msg.deviceName, "id": msg.id, "data": msg.data}

    @staticmethod
    def __convert_gateway_attributes_request_msg(msg: GatewayAttributesRequestMsg):
        return {"id": msg.id, "deviceName": msg.deviceName, "client": msg.client, "keys": msg.keys}

    @staticmethod
    def get_value(msg: KeyValueProto):
        if msg.type == KeyValueType.BOOLEAN_V:
            return msg.bool_v
        if msg.type == KeyValueType.LONG_V:
            return msg.long_v
        if msg.type == KeyValueType.DOUBLE_V:
            return msg.double_v
        if msg.type == KeyValueType.STRING_V:
            return msg.string_v
        if msg.type == KeyValueType.JSON_V:
            return msg.json_v
