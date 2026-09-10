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

DEFAULT_POLL_PERIOD = 10_000
DEFAULT_RETRY_DELAY = 1.0
DEFAULT_MAX_DELAY = 30.0
DEFAULT_HEARTBEAT_INTERVAL = 0
DEFAULT_MAX_RETRIES = 3
DEFAULT_AUTO_RECONNECT = True
UPLINK_PREFIX = 'uplink'
DOWNLINK_PREFIX = 'downlink'
CONNECTOR_TYPE = 's7'

RESERVED_RPC_TYPE_PATTERN = r'^type=(?P<type>tag|data|vm);'

RESERVED_GET_TAG_RPC_SCHEMA = 'type=tag;tag=<S7 tag address>;'
RESERVED_SET_TAG_RPC_SCHEMA = 'type=tag;tag=<S7 tag address>;value=<value>;'
RESERVED_GET_TAG_RPC_PATTERN = r'^type=tag;tag=(?P<tag>[^;]+);$'
RESERVED_SET_TAG_RPC_PATTERN = r'^type=tag;tag=(?P<tag>[^;]+);value=(?P<value>.+);$'

RESERVED_GET_DATA_RPC_SCHEMA = r'type=data;dbNumber=\d;start=\d;dataType=<type>;size=\d;[bit=\d;]'
RESERVED_SET_DATA_RPC_SCHEMA = (r'type=data;dbNumber=\d;start=\d;dataType=<type>;'
                                r'[size=\d;][bit=\d;]value=<value>;')
RESERVED_GET_DATA_RPC_PATTERN = (r'^type=data;dbNumber=(?P<dbNumber>\d+);start=(?P<start>\d+);'
                                 r'dataType=(?P<dataType>\w+);size=(?P<size>\d+);(bit=(?P<bit>\d+);)?$')
RESERVED_SET_DATA_RPC_PATTERN = (r'^type=data;dbNumber=(?P<dbNumber>\d+);start=(?P<start>\d+);'
                                 r'dataType=(?P<dataType>\w+);(size=(?P<size>\d+);)?(bit=(?P<bit>\d+);)?'
                                 r'value=(?P<value>.+);$')

RESERVED_GET_VM_RPC_SCHEMA = 'type=vm;vmAddress=<LOGO VM address>;'
RESERVED_SET_VM_RPC_SCHEMA = 'type=vm;vmAddress=<LOGO VM address>;value=<value>;'
RESERVED_GET_VM_RPC_PATTERN = r'^type=vm;vmAddress=(?P<vmAddress>[^;]+);$'
RESERVED_SET_VM_RPC_PATTERN = r'^type=vm;vmAddress=(?P<vmAddress>[^;]+);value=(?P<value>.+);$'

RESERVED_GET_RPC_SCHEMA = ("type=tag;tag=<S7 tag address>; OR "
                           r"type=data;dbNumber=\d;start=\d;dataType=<type>;size=\d;[bit=\d;] OR "
                           "type=vm;vmAddress=<LOGO VM address>;")
RESERVED_SET_RPC_SCHEMA = ("type=tag;tag=<S7 tag address>;value=<value>; OR "
                           r"type=data;dbNumber=\d;start=\d;dataType=<type>;[size=\d;][bit=\d;]value=<value>; OR "
                           "type=vm;vmAddress=<LOGO VM address>;value=<value>;")
