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

from enum import Enum
from typing import Union
from simplejson import dumps
from time import time

from thingsboard_gateway.gateway.proto.messages_pb2 import *


class Status(Enum):
    FAILURE = 1,
    NOT_FOUND = 2,
    SUCCESS = 3


class KeyValueTypeEnum(Enum):
    BOOLEAN_V = 0,
    LONG_V = 1,
    DOUBLE_V = 2,
    STRING_V = 3,
    JSON_V = 4,


def is_not_none(param):
    if param is None:
        raise ValueError("Parameter is None!")


class GrpcMsgCreator:
    @staticmethod
    def create_response_connector_msg(status: Union[str, Status, None], basic_message=None):
        basic_message = GrpcMsgCreator.get_basic_message(basic_message)
        if status is not None:
            basic_message.response.status = ResponseStatus.Value(status.name if isinstance(status, Status) else status)
        else:
            basic_message.response.MergeFrom(Response())
        return basic_message

    @staticmethod
    def create_telemetry_connector_msg(telemetry: Union[list, dict] = None, device_name=None,
                                       basic_message=None) -> FromConnectorMessage:
        """
        Creates GatewayAttributesMsg.\n
        :param telemetry:
        :param device_name:
        :param basic_message:
        :return FromConnectorMessage:
        attributes parameter possible structures:\n
        1. You can provide device_name parameter and telemetry parameter as a dictionary \n
        (timestamp will be set to current timestamp):\n
        \n
        **{"KEY1": "VALUE1", "KEY2": "VALUE2"}**
        \n\n
        1. You can provide device_name parameter and telemetry parameter as a dictionary with "ts" (timestamp) field:\n
        \n
        **{"ts": 1638439434, "values": {"KEY1": "VALUE1", "KEY2": "VALUE2"}}**
        \n\n
        2. You can provide device_name parameter and telemetry parameter as a list with dictionaries with "ts" (timestamp) field:\n
        \n
        **[{"ts": 1638439434, "values": {"KEY1": "VALUE1"}}]**
        \n\n
        3. You can leave device_name parameter as None and provide a list with dictionaries with deviceName parameter and telemetry data for several devices \n
        (timestamp will be set to current timestamp)\n
        \n
        [{"deviceName": "DEVICE_NAME",
          "telemetry": {
            "KEY": "VALUE"
            }
        }]
        \n\n
        4. You can leave device_name parameter as None and provide a list with dictionaries with deviceName parameter and telemetry data dictionary with "ts" (timestamp) field:\n
        [{"deviceName": "DEVICE_NAME",
          "telemetry": [{"ts": 1638439434,
                         "values":{"KEY1": "VALUE1",
                                   "KEY2": "VALUE2"}
                        }]
        }]
        """
        is_not_none(telemetry)
        basic_message = GrpcMsgCreator.get_basic_message(basic_message)
        gateway_telemetry_msg = GatewayTelemetryMsg()
        if device_name is not None:
            telemetry_msg = GrpcMsgCreator.__get_telemetry_msg_for_device(device_name, telemetry)
            gateway_telemetry_msg.msg.extend([telemetry_msg])
        else:
            for device_entry in telemetry:
                telemetry_msg = GrpcMsgCreator.__get_telemetry_msg_for_device(device_entry['deviceName'],
                                                                              device_entry['telemetry'])
                gateway_telemetry_msg.msg.extend([telemetry_msg])
        basic_message.gatewayTelemetryMsg.MergeFrom(gateway_telemetry_msg)
        return basic_message

    @staticmethod
    def create_attributes_connector_msg(attributes: Union[list, dict] = None, device_name=None,
                                        basic_message=None) -> FromConnectorMessage:
        """
        Creates GatewayAttributesMsg.\n
        :param attributes:
        :param device_name:
        :param basic_message:
        :return FromConnectorMessage:
        attributes parameter possible structures:\n
        1. You can provide device_name parameter and attributes parameter as a dictionary:\n
        \n
        **{"KEY1": "VALUE1"}**
        \n\n
        2. You can leave device_name parameter as None and provide a dictionary with attributes data for several devices\n
        \n
        **[{"deviceName": "DEVICE_NAME", "attributes": {"KEY": "VALUE"}}]**
        \n
        """
        is_not_none(attributes)
        basic_message = GrpcMsgCreator.get_basic_message(basic_message)
        gateway_attributes_msg = GatewayAttributesMsg()
        if device_name is not None:
            attributes_msg = AttributesMsg()
            attributes_msg.deviceName = device_name
            post_attributes_msg = PostAttributeMsg()
            for entry_key in attributes:
                key_value_proto = GrpcMsgCreator.__get_key_value_proto_value(entry_key, attributes[entry_key])
                post_attributes_msg.kv.extend([key_value_proto])
            attributes_msg.msg.MergeFrom(post_attributes_msg)
            gateway_attributes_msg.msg.extend([attributes_msg])
        else:
            for device_entry in attributes:
                attributes_msg = AttributesMsg()
                attributes_msg.deviceName = device_entry['deviceName']
                post_attributes_msg = PostAttributeMsg()
                for entry_key in device_entry['attributes']:
                    key_value_proto = GrpcMsgCreator.__get_key_value_proto_value(entry_key,
                                                                                 device_entry['attributes'][entry_key])
                    post_attributes_msg.kv.extend([key_value_proto])
                attributes_msg.msg.MergeFrom(post_attributes_msg)
                gateway_attributes_msg.msg.extend([attributes_msg])
        basic_message.gatewayAttributesMsg.MergeFrom(gateway_attributes_msg)
        return basic_message

    @staticmethod
    def create_claim_connector_msg(claiming: Union[list, dict], device_name,
                                   basic_message=None) -> FromConnectorMessage:
        """
        Creates GatewayClaimMsg.
        :param claiming:
        :param device_name:
        :param basic_message:
        :return FromConnectorMessage:
        properties parameter possible structures:\n
        1. You can provide device_name parameter and properties parameter as a dictionary:\n
        \n
        **{"secretKey": "SECRET_KEY", "durationMs": 120000}**
        \n\n
        2. You can leave device_name parameter as None and provide a dictionary with claiming data for several devices\n
        \n
        **[{"deviceName": "DEVICE_NAME", "claiming": {"secretKey": "SECRET_KEY", "durationMs": 120000}}]**
        \n
        """
        is_not_none(claiming)
        basic_message = GrpcMsgCreator.get_basic_message(basic_message)
        gateway_claim_message = GatewayClaimMsg()
        if device_name is not None:
            claim_device_msg = ClaimDeviceMsg()
            claim_device_msg.deviceName = device_name
            claim_device = ClaimDevice()
            claim_device.secretKey = claiming['secretKey']
            claim_device.durationMs = claiming['durationMs']
            claim_device_msg.claimRequest.MergeFrom(claim_device)
            gateway_claim_message.msg.extend([claim_device_msg])
        else:
            for device_entry in claiming:
                claim_device_msg = ClaimDeviceMsg()
                claim_device_msg.deviceName = device_entry['deviceName']
                claim_device = ClaimDevice()
                claim_device.secretKey = device_entry['claiming']['secretKey']
                claim_device.durationMs = device_entry['claiming']['durationMs']
                claim_device_msg.claimRequest.MergeFrom(claim_device)
                gateway_claim_message.msg.extend([claim_device_msg])
        basic_message.gatewayClaimMsg.MergeFrom(gateway_claim_message)
        return basic_message

    @staticmethod
    def create_register_connector_msg(connector_key: str, basic_message=None):
        is_not_none(connector_key)
        basic_message = GrpcMsgCreator.get_basic_message(basic_message)
        reg_msg = RegisterConnectorMsg()
        reg_msg.connectorKey = connector_key
        basic_message.registerConnectorMsg.MergeFrom(reg_msg)
        return basic_message

    @staticmethod
    def create_unregister_connector_msg(connector_key, basic_message=None):
        is_not_none(connector_key)
        basic_message = GrpcMsgCreator.get_basic_message(basic_message)
        un_reg_msg = UnregisterConnectorMsg()
        un_reg_msg.connectorKey = connector_key
        basic_message.unregisterConnectorMsg.MergeFrom(un_reg_msg)
        return basic_message

    @staticmethod
    def create_get_connected_devices_msg(connector_key: str, basic_message=None):
        is_not_none(connector_key)
        basic_message = GrpcMsgCreator.get_basic_message(basic_message)
        get_connected_devices_msg = ConnectorGetConnectedDevicesMsg()
        get_connected_devices_msg.connectorKey = connector_key
        basic_message.connectorGetConnectedDevicesMsg.MergeFrom(get_connected_devices_msg)
        return basic_message

    @staticmethod
    def create_device_connected_msg(device_name, device_type="default", basic_message=None):
        is_not_none(device_name)
        basic_message = GrpcMsgCreator.get_basic_message(basic_message)
        connect_device_msg = ConnectMsg()
        connect_device_msg.deviceName = device_name
        connect_device_msg.deviceType = device_type
        basic_message.connectMsg.MergeFrom(connect_device_msg)
        return basic_message

    @staticmethod
    def create_device_disconnected_msg(device_name, basic_message=None):
        is_not_none(device_name)
        basic_message = GrpcMsgCreator.get_basic_message(basic_message)
        disconnect_device_msg = DisconnectMsg()
        disconnect_device_msg.deviceName = device_name
        basic_message.disconnectMessage.MergeFrom(disconnect_device_msg)
        return basic_message

    @staticmethod
    def create_rpc_response_connector_msg(device, id, data, basic_message=None):
        is_not_none(device)
        is_not_none(id)
        is_not_none(data)
        basic_message = GrpcMsgCreator.get_basic_message(basic_message)
        gw_rpc_response_msg = GatewayRpcResponseMsg()
        gw_rpc_response_msg.deviceName = device
        gw_rpc_response_msg.id = id
        gw_rpc_response_msg.data = data
        basic_message.gatewayRpcResponseMsg.MergeFrom(gw_rpc_response_msg)
        return basic_message

    @staticmethod
    def create_attributes_request_connector_msg(device, keys: list[str], client_scope=False, request_id=None,
                                                basic_message=None):
        is_not_none(device)
        is_not_none(keys)
        is_not_none(request_id)
        basic_message = GrpcMsgCreator.get_basic_message(basic_message)
        gw_attr_request_msg = GatewayAttributesRequestMsg()
        gw_attr_request_msg.deviceName = device
        gw_attr_request_msg.id = request_id
        gw_attr_request_msg.client = client_scope
        gw_attr_request_msg.keys.extend(keys)
        basic_message.gatewayAttributeRequestMsg.MergeFrom(gw_attr_request_msg)
        return basic_message

    @staticmethod
    def __get_key_value_proto_value(key: str, value: Union[str, bool, int, float, dict]) -> KeyValueProto:
        key_value_proto = KeyValueProto()
        key_value_proto.key = key
        if isinstance(value, bool):
            key_value_proto.type = KeyValueType.Value(KeyValueTypeEnum.BOOLEAN_V.name)
            key_value_proto.bool_v = value
        elif isinstance(value, int):
            key_value_proto.type = KeyValueType.Value(KeyValueTypeEnum.LONG_V.name)
            key_value_proto.long_v = value
        elif isinstance(value, float):
            key_value_proto.type = KeyValueType.Value(KeyValueTypeEnum.DOUBLE_V.name)
            key_value_proto.double_v = value
        elif isinstance(value, str):
            key_value_proto.type = KeyValueType.Value(KeyValueTypeEnum.STRING_V.name)
            key_value_proto.string_v = value
        elif isinstance(value, dict):
            key_value_proto.type = KeyValueType.Value(KeyValueTypeEnum.JSON_V.name)
            key_value_proto.json_v = dumps(value)
        return key_value_proto

    @staticmethod
    def __get_ts_kv_list_proto(telemetry_dict, ts: Union[int, None]) -> TsKvListProto:
        ts_kv_list_proto = TsKvListProto()
        if ts == 0:
            ts = round(time() * 1000)
        for telemetry_key in telemetry_dict:
            ts_kv_list_proto.ts = ts
            key_value_proto = GrpcMsgCreator.__get_key_value_proto_value(telemetry_key, telemetry_dict[telemetry_key])
            ts_kv_list_proto.kv.extend([key_value_proto])
        return ts_kv_list_proto

    @staticmethod
    def __get_telemetry_msg_for_device(device_name, telemetry):
        telemetry_msg = TelemetryMsg()
        telemetry_msg.deviceName = device_name
        post_telemetry_msg = PostTelemetryMsg()
        if isinstance(telemetry, list):
            # [{"ts": 1638439434, "values": {"KEY1": "VALUE1", "KEY2": "VALUE2"}}]
            for entry in telemetry:
                if entry.get("ts") is not None:
                    # {"ts": 1638439434, "values": {"KEY1": "VALUE1", "KEY2": "VALUE2"}}
                    ts_kv_list_proto = GrpcMsgCreator.__get_ts_kv_list_proto(entry['values'], entry['ts'])
                else:
                    # {"KEY1": "VALUE1", "KEY2": "VALUE2"}
                    ts_kv_list_proto = GrpcMsgCreator.__get_ts_kv_list_proto(entry, 0)
                post_telemetry_msg.tsKvList.extend([ts_kv_list_proto])
        elif isinstance(telemetry, dict):
            if telemetry.get("ts") is not None:
                # {"ts": 1638439434, "values": {"KEY1": "VALUE1", "KEY2": "VALUE2"}}
                ts_kv_list_proto = GrpcMsgCreator.__get_ts_kv_list_proto(telemetry['values'], telemetry['ts'])
            else:
                # {"KEY1": "VALUE1", "KEY2": "VALUE2"}
                ts_kv_list_proto = GrpcMsgCreator.__get_ts_kv_list_proto(telemetry, 0)
            post_telemetry_msg.tsKvList.extend([ts_kv_list_proto])
        else:
            raise ValueError("Unknown telemetry type!")
        telemetry_msg.msg.MergeFrom(post_telemetry_msg)
        return telemetry_msg

    @staticmethod
    def get_basic_message(basic_message):
        if basic_message is None:
            basic_message = FromConnectorMessage()
        return basic_message
