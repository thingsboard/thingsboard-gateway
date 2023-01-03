#      Copyright 2022. ThingsBoard
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

from enum import Enum


class DeviceActions(Enum):
    UNKNOWN = 0,
    CONNECT = 1,
    DISCONNECT = 2


class DownlinkMessageType(Enum):
    Response = 0,
    ConnectorConfigurationMsg = 1,
    GatewayAttributeUpdateNotificationMsg = 2,
    GatewayAttributeResponseMsg = 3,
    GatewayDeviceRpcRequestMsg = 4,
    UnregisterConnectorMsg = 5,
    ConnectorGetConnectedDevicesResponseMsg = 6


class Status(Enum):
    FAILURE = 1,
    NOT_FOUND = 2,
    SUCCESS = 3,
    NO_NEW_DATA = 4
    FORBIDDEN_DEVICE = 5
