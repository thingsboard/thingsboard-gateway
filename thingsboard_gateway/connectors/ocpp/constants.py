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

RESERVED_GET_RPC_SCHEMA = ('component=<component>;[componentInstance=<instance>;][evseId=<id>;]'
                           'variable=<variable>;[variableInstance=<instance>;][timeout=<seconds>;]')
RESERVED_SET_RPC_SCHEMA = ('component=<component>;[componentInstance=<instance>;][evseId=<id>;]'
                           'variable=<variable>;[variableInstance=<instance>;][timeout=<seconds>;]value=<value>;')
RESERVED_GET_RPC_PATTERN = (r'^component=(?P<component>[^;]+);'
                            r'(?:componentInstance=(?P<componentInstance>[^;]+);)?'
                            r'(?:evseId=(?P<evseId>\d+);)?'
                            r'variable=(?P<variable>[^;]+)'
                            r'(?:;variableInstance=(?P<variableInstance>[^;]+))?'
                            r'(?:;timeout=(?P<timeout>\d+(?:\.\d+)?))?;?$')
RESERVED_SET_RPC_PATTERN = (r'^component=(?P<component>[^;]+);'
                            r'(?:componentInstance=(?P<componentInstance>[^;]+);)?'
                            r'(?:evseId=(?P<evseId>\d+);)?'
                            r'variable=(?P<variable>[^;]+);'
                            r'(?:variableInstance=(?P<variableInstance>[^;]+);)?'
                            r'(?:timeout=(?P<timeout>\d+(?:\.\d+)?);)?'
                            r'value=(?P<value>.+?);?$')
