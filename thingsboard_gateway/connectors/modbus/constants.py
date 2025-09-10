#      Copyright 2025. ThingsBoard
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

from thingsboard_gateway.gateway.constants import *  # noqa


class PymodbusDefaults:
    TcpPort = 502
    TlsPort = 802
    UdpPort = 502
    Backoff = 0.3
    CloseCommOnError = False
    HandleLocalEcho = False
    Retries = 3
    RetryOnEmpty = False
    RetryOnInvalid = False
    Timeout = 3
    Reconnects = 0
    TransactionId = 0
    Strict = True
    ProtocolId = 0
    Slave = 0x00
    Baudrate = 19200
    Parity = "N"
    Bytesize = 8
    Stopbits = 1
    ZeroMode = False
    IgnoreMissingSlaves = False
    ReadSize = 1024
    BroadcastEnable = False
    ReconnectDelay = 1000 * 60 * 5
    Count = 1


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
IDENTITY_SECTION = "identity"
SECURITY_SECTION = "security"

BYTE_ORDER_PARAMETER = "byteOrder"
WORD_ORDER_PARAMETER = "wordOrder"
CONNECT_ATTEMPT_COUNT_PARAMETER = "connectAttemptCount"
CONNECT_ATTEMPT_TIME_MS_PARAMETER = "connectAttemptTimeMs"
WAIT_AFTER_FAILED_ATTEMPTS_MS_PARAMETER = "waitAfterFailedAttemptsMs"

DELAY_BETWEEN_REQUESTS_MS_PARAMETER = "delayBetweenRequestsMs"

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
TYPE_PARAMETER = "type"
REPACK_PARAMETER = "repack"
SERIAL_CONNECTION_TYPE_PARAMETER = "serial"

RETRIES_PARAMETER = "retries"

PAYLOAD_PARAMETER = "payload"
TAG_PARAMETER = "tag"

COILS_INITIALIZER = "coils_initializer"
HOLDING_REGISTERS = "holding_registers"
INPUT_REGISTERS = "input_registers"
DISCRETE_INPUTS = "discrete_inputs"
AVAILABLE_DATA_TYPES_REGEX = "string|bytes|bits|16int|16uint|16float|32int|32uint|32float|64int|64uint|64float"
GET_PATTERN_REGEX = rf'^type=(?:{AVAILABLE_DATA_TYPES_REGEX});functionCode=[1-4];objectsCount=\d+;address=\d+;'
SET_PATTERN_REGEX = rf'^type=(?:{AVAILABLE_DATA_TYPES_REGEX});functionCode=(?:5|6|15|16);'rf'objectsCount=\d+;address=\d+;value=.+;$'
GET_RPC_EXPECTED_SCHEMA = "get type=<type>;functionCode=<functionCode>;objectsCount=<objectsCount>;address=<address>;"
SET_RPC_EXPECTED_SCHEMA = "set type=<type>;functionCode=<functionCode>;objectsCount=<objectsCount>;address=<address>;value=<value>;"

FUNCTION_TYPE = {
    COILS_INITIALIZER: 'co',
    HOLDING_REGISTERS: 'hr',
    INPUT_REGISTERS: 'ir',
    DISCRETE_INPUTS: 'di'
}

FUNCTION_CODE_SLAVE_INITIALIZATION = {
    HOLDING_REGISTERS: (6, 16),
    COILS_INITIALIZER: (5, 15),
    INPUT_REGISTERS: (6, 16),
    DISCRETE_INPUTS: (5, 15)
}

FUNCTION_CODE_READ = {
    HOLDING_REGISTERS: 3,
    COILS_INITIALIZER: 1,
    INPUT_REGISTERS: 4,
    DISCRETE_INPUTS: 2
}

# Default values

TIMEOUT = 30
