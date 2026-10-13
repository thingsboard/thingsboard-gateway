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

RESERVED_SET_RPC_SCHEMA = ('nodeId=<id>;type=hex|string|bool|8int|8uint|16int|16uint|32int|32uint|64int|64uint|32float;'
                           '[byteorder=big|little;][isExtendedId=true|false;]value=<value>;')
RESERVED_SET_RPC_PATTERN = (r'^nodeId=(?P<nodeId>0[xX][0-9A-Fa-f]+|[1-9]\d*|0);'
                            r'type=(?P<type>hex|string|bool|32float|(?P<intSize>8|16|32|64)(?P<unsigned>u?)int);'
                            r'(?:byteorder=(?P<byteorder>big|little);)?'
                            r'(?:isExtendedId=(?P<isExtendedId>[Tt]rue|[Ff]alse);)?'
                            r'value=(?P<value>.+?);?$')
