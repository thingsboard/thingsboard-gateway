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

from thingsboard_gateway.gateway.proto.messages_pb2 import *


class GwMsgCallbacks:
    __CALLBACKS = {
        Response: None,
        ConnectorConfigurationMsg: None,
        GatewayAttributeUpdateNotificationMsg: None,
        GatewayAttributeResponseMsg: None,
        GatewayDeviceRpcRequestMsg: None,
        UnregisterConnectorMsg: None,
    }

    def set_callback(self,
                     received_response_cb=None,
                     received_connector_configuration_cb=None,
                     received_gateway_attribute_update_notification_cb=None,
                     received_gateway_attribute_response_cb=None,
                     received_gateway_device_rpc_request_cb=None,
                     received_unregister_connector_cb=None
                     ):
        if received_response_cb is not None:
            self.__CALLBACKS[Response] = received_response_cb
        if received_connector_configuration_cb is not None:
            self.__CALLBACKS[ConnectorConfigurationMsg] = received_connector_configuration_cb
        if received_gateway_attribute_update_notification_cb is not None:
            self.__CALLBACKS[GatewayAttributeUpdateNotificationMsg] = received_gateway_attribute_update_notification_cb
        if received_gateway_attribute_response_cb is not None:
            self.__CALLBACKS[GatewayAttributeResponseMsg] = received_gateway_attribute_response_cb
        if received_gateway_device_rpc_request_cb is not None:
            self.__CALLBACKS[GatewayDeviceRpcRequestMsg] = received_gateway_device_rpc_request_cb
        if received_unregister_connector_cb is not None:
            self.__CALLBACKS[UnregisterConnectorMsg] = received_unregister_connector_cb
