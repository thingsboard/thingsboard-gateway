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

VERSION = "3.2"

# Service constants

STATISTIC_MESSAGE_RECEIVED_PARAMETER = "MessagesReceived"
STATISTIC_MESSAGE_SENT_PARAMETER = "MessagesSent"

CONNECTOR_PARAMETER = "connector"
CONVERTER_PARAMETER = "converter"
UPLINK_PREFIX = "uplink_"
DOWNLINK_PREFIX = "downlink_"

CONFIG_SECTION_PARAMETER = "config"
CONFIG_SERVER_SECTION_PARAMETER = "server"
CONFIG_DEVICES_SECTION_PARAMETER = "devices"

CONNECTED_DEVICES_FILENAME = "connected_devices.json"
PERSISTENT_GRPC_CONNECTORS_KEY_FILENAME = "persistent_keys.json"

# Data parameter constants

DEVICE_SECTION_PARAMETER = "device"

DEVICE_NAME_PARAMETER = "deviceName"
DEVICE_TYPE_PARAMETER = "deviceType"

ATTRIBUTES_PARAMETER = "attributes"
TELEMETRY_PARAMETER = "telemetry"
TELEMETRY_TIMESTAMP_PARAMETER = "ts"
TELEMETRY_VALUES_PARAMETER = "values"

SEND_ON_CHANGE_PARAMETER = "sendDataOnlyOnChange"
# TTL value in milliseconds
SEND_ON_CHANGE_TTL_PARAMETER = "sendDataOnlyOnChangeTtl"

DEFAULT_SEND_ON_CHANGE_VALUE = False
# TTL value in milliseconds
DEFAULT_SEND_ON_CHANGE_INFINITE_TTL_VALUE = 0

# RPC parameter constants

RPC_ID_PARAMETER = "id"
RPC_METHOD_PARAMETER = "method"
RPC_PARAMS_PARAMETER = "params"
DATA_PARAMETER = "data"
