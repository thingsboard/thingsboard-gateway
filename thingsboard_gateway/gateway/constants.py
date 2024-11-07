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

from simplejson import dumps
from time import time


# Service constants

STATISTIC_MESSAGE_RECEIVED_PARAMETER = "MessagesReceived"
STATISTIC_MESSAGE_SENT_PARAMETER = "MessagesSent"

CONNECTOR_PARAMETER = "connector"
CONVERTER_PARAMETER = "converter"
UPLINK_PREFIX = "uplink_"
DOWNLINK_PREFIX = "downlink_"

CONFIG_SECTION_PARAMETER = "config"
CONFIG_VERSION_PARAMETER = "configVersion"
CONFIG_SERVER_SECTION_PARAMETER = "server"
CONFIG_DEVICES_SECTION_PARAMETER = "devices"

CONNECTED_DEVICES_FILENAME = "connected_devices.json"
PERSISTENT_GRPC_CONNECTORS_KEY_FILENAME = "persistent_keys.json"

RENAMING_PARAMETER = "renaming"
CONNECTOR_ID_PARAMETER = "connectorId"
CONNECTOR_NAME_PARAMETER = "connectorName"

SECURITY_VAR = ('type', 'accessToken', 'caCert', 'privateKey', 'cert', 'clientId', 'username', 'password')

# Data parameter constants

DEVICE_SECTION_PARAMETER = "device"

DEVICE_NAME_PARAMETER = "deviceName"
DEVICE_TYPE_PARAMETER = "deviceType"

ATTRIBUTES_PARAMETER = "attributes"
TELEMETRY_PARAMETER = "telemetry"
TIMESERIES_PARAMETER = "timeseries"
TELEMETRY_TIMESTAMP_PARAMETER = "ts"
TELEMETRY_VALUES_PARAMETER = "values"
METADATA_PARAMETER = "metadata"

SEND_ON_CHANGE_PARAMETER = "sendDataOnlyOnChange"
# TTL value in milliseconds
SEND_ON_CHANGE_TTL_PARAMETER = "sendDataOnlyOnChangeTtl"

DEFAULT_SEND_ON_CHANGE_VALUE = False
# TTL value in milliseconds
DEFAULT_SEND_ON_CHANGE_INFINITE_TTL_VALUE = 0

# Messages metadata constants
RECEIVED_TS_PARAMETER = "receivedTs"
CONVERTED_TS_PARAMETER = "convertedTs"
SEND_TO_STORAGE_TS_PARAMETER = "sendToStorageTs"
DATA_RETRIEVING_STARTED = "dataRetrieveStartedTs"

# Size of metadata that will be added to messages in debug mode
# Connector name length should be added to the size of the metadata
DEBUG_METADATA_TEMPLATE_SIZE = len(dumps({
    "metadata": {
        RECEIVED_TS_PARAMETER: int(time() * 1000),
        CONNECTOR_PARAMETER: "",
        "publishedTs": int(time() * 1000)}}
    , separators=(',', ':'), skipkeys=True).encode("utf-8"))

# Report strategy parameters
REPORT_STRATEGY_PARAMETER = "reportStrategy"
REPORT_PERIOD_PARAMETER = "reportPeriod"
TYPE_PARAMETER = "type"
AGGREGATION_FUNCTION_PARAMETER = "aggregationFunction"

class ReportStrategy(Enum):
    ON_REPORT_PERIOD = "ON_REPORT_PERIOD"
    ON_CHANGE = "ON_CHANGE"
    ON_CHANGE_OR_REPORT_PERIOD = "ON_CHANGE_OR_REPORT_PERIOD"
    ON_RECEIVED = "ON_RECEIVED"

    @classmethod
    def from_string(cls, value: str):
        for strategy in cls:
            if strategy.value.lower() == value.lower():
                return strategy
        raise ValueError("Invalid report strategy value: %r" % value)

DEFAULT_REPORT_STRATEGY_CONFIG = {
    TYPE_PARAMETER: ReportStrategy.ON_RECEIVED.value,
    REPORT_PERIOD_PARAMETER: 10000
}

# RPC parameter constants

RPC_ID_PARAMETER = "id"
RPC_METHOD_PARAMETER = "method"
RPC_PARAMS_PARAMETER = "params"
DATA_PARAMETER = "data"

# Attribute constants
ATTRIBUTES_FOR_REQUEST = ["RemoteLoggingLevel", "general_configuration", "storage_configuration", "grpc_configuration", "logs_configuration", "active_connectors"]
