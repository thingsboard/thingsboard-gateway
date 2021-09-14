#      Copyright 2021. ThingsBoard
#
#      Licensed under the Apache License, Version 2.0 (the "License");
#      you may not use this file except in compliance with the License.
#      You may obtain a copy of the License at
#
#          http://www.apache.org/licenses/LICENSE-2.0
#
#      Unless required by applicable law or agreed to in writing, software
#      distributed under the License is distributed on an "AS IS" BASIS,
#      WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#      See the License for the specific language governing permissions and
#      limitations under the License.

from thingsboard_gateway.gateway.constants import *

# Connector constants

LAST_PREFIX = "last_"
NEXT_PREFIX = "next_"

CHECK_POSTFIX = "_check"
POLL_PERIOD_POSTFIX = "PollPeriod"

TIMESERIES_PARAMETER = "timeseries"

MASTER_PARAMETER = "master"
AVAILABLE_FUNCTIONS_PARAMETER = "available_functions"

CONNECTION_ATTEMPT_PARAMETER = "connection_attempt"
LAST_CONNECTION_ATTEMPT_TIME_PARAMETER = "last_connection_attempt_time"

# Configuration parameters

RPC_SECTION = "rpc"

BYTE_ORDER_PARAMETER = "byteOrder"
WORD_ORDER_PARAMETER = "wordOrder"
SEND_DATA_ONLY_ON_CHANGE_PARAMETER = "sendDataOnlyOnChange"
CONNECT_ATTEMPT_COUNT_PARAMETER = "connectAttemptCount"
CONNECT_ATTEMPT_TIME_MS_PARAMETER = "connectAttemptTimeMs"
WAIT_AFTER_FAILED_ATTEMPTS_MS_PARAMETER = "waitAfterFailedAttemptsMs"

FUNCTION_CODE_PARAMETER = "functionCode"

ADDRESS_PARAMETER = "address"
OBJECTS_COUNT_PARAMETER = "objectsCount"

UNIT_ID_PARAMETER = "unitId"

HOST_PARAMETER = "host"
PORT_PARAMETER = "port"
BAUDRATE_PARAMETER = "baudrate"
TIMEOUT_PARAMETER = "timeout"
METHOD_PARAMETER = "method"
STOPBITS_PARAMETER = "stopbits"
BYTESIZE_PARAMETER = "bytesize"
PARITY_PARAMETER = "parity"
STRICT_PARAMETER = "strict"
TYPE_PARAMETER = "type"

PAYLOAD_PARAMETER = "payload"
TAG_PARAMETER = "tag"
