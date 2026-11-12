#     Copyright 2026. ThingsBoard
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

RESERVED_GET_RPC_SCHEMA = 'oid=<oid>;[timeout=<seconds>;]'
RESERVED_SET_RPC_SCHEMA = 'oid=<oid>;[type=<type>;][timeout=<seconds>;]value=<value>;'
RESERVED_GET_RPC_PATTERN = (r'^oid=(?P<oid>\.?\d+(?:\.\d+)*)'
                            r'(?:;timeout=(?P<timeout>\d+(?:\.\d+)?))?;?$')
RESERVED_SET_RPC_PATTERN = (r'^oid=(?P<oid>\.?\d+(?:\.\d+)*);'
                            r'(?:type=(?P<type>[A-Za-z0-9]+);)?'
                            r'(?:timeout=(?P<timeout>\d+(?:\.\d+)?);)?'
                            r'value=(?P<value>.+?);?$')

# SNMPv3 protocol names accepted in the config (upper case, without "-" and "_"),
# mapped to puresnmp plugin identifiers
V3_AUTH_PROTOCOLS = {
    'MD5': 'md5',
    'SHA': 'sha1',
    'SHA1': 'sha1',
}
V3_PRIV_PROTOCOLS = {
    'DES': 'des',
    'AES': 'aes',
    'AES128': 'aes',
}
V3_CLIENT_MAX_AGE_SECONDS = 90

DEFAULT_TRAP_LISTENER_ADDRESS = '0.0.0.0'
DEFAULT_TRAP_LISTENER_PORT = 162
TRAP_LISTENER_CLOSE_TIMEOUT_SECONDS = 5

SNMP_V1_MESSAGE_VERSION = 0
SNMP_V2C_MESSAGE_VERSION = 1
SNMP_V3_MESSAGE_VERSION = 3

SYS_UPTIME_OID = '1.3.6.1.2.1.1.3.0'
SNMP_TRAP_OID = '1.3.6.1.6.3.1.1.4.1.0'
SNMP_GENERIC_TRAPS_OID = '1.3.6.1.6.3.1.1.5'
SNMP_TRAP_ADDRESS_OID = '1.3.6.1.6.3.18.1.3.0'
SNMP_TRAP_ENTERPRISE_OID = '1.3.6.1.6.3.1.1.4.3.0'

SNMP_V1_TRAP_PDU_TAG = 0xA4
SNMP_V1_ENTERPRISE_SPECIFIC_TRAP = 6

V3_TIME_WINDOW_SECONDS = 150
USM_AUTH_PARAMETERS_INDEX = 4

BER_CONSTRUCTED_FLAG = 0x20
BER_LONG_LENGTH_FLAG = 0x80

# Error-status names from RFC 3416, puresnmp has no message for some of them (e.g. 16 and 17)
SNMP_ERROR_STATUS_NAMES = {
    1: 'tooBig',
    2: 'noSuchName',
    3: 'badValue',
    4: 'readOnly',
    5: 'genErr',
    6: 'noAccess',
    7: 'wrongType',
    8: 'wrongLength',
    9: 'wrongEncoding',
    10: 'wrongValue',
    11: 'noCreation',
    12: 'inconsistentValue',
    13: 'resourceUnavailable',
    14: 'commitFailed',
    15: 'undoFailed',
    16: 'authorizationError',
    17: 'notWritable',
    18: 'inconsistentName',
}
