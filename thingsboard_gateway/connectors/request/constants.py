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

RESERVED_GET_RPC_SCHEMA = 'requestUrlExpression=<url>;[httpMethod=<method>;]'
RESERVED_SET_RPC_SCHEMA = 'requestUrlExpression=<url>;[httpMethod=<method>;]value=<value>;'
RESERVED_GET_RPC_PATTERN = (r'^requestUrlExpression=(?P<requestUrlExpression>[^;]+)'
                            r'(;httpMethod=(?P<httpMethod>[A-Za-z]+))?;?$')
RESERVED_SET_RPC_PATTERN = (r'^requestUrlExpression=(?P<requestUrlExpression>[^;]+);'
                            r'(httpMethod=(?P<httpMethod>[A-Za-z]+);)?'
                            r'value=(?P<value>.+?);?$')
