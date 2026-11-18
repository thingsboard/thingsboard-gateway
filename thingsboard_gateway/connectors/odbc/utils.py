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

from thingsboard_gateway.tb_utility.tb_utility import TBUtility


def resolve_env_placeholders(connection_str, log, connector_name):
    connection_str, missing_placeholders = TBUtility.substitute_env_placeholders(connection_str)
    for placeholder in missing_placeholders:
        log.debug("[%s] No environment variable found for placeholder '%s'", connector_name, placeholder)
    return connection_str
