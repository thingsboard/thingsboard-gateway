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

from datetime import timedelta

from thingsboard_gateway.connectors.snmp.constants import SNMP_ERROR_STATUS_NAMES
from thingsboard_gateway.tb_utility.tb_utility import TBUtility


def resolve_env_placeholders(value, parameter_name):
    if not isinstance(value, str):
        return value

    resolved_value, missing_variables = TBUtility.substitute_env_placeholders(value)
    if missing_variables:
        variables = ', '.join(f'"{variable}"' for variable in missing_variables)
        raise ValueError(f'"{parameter_name}" uses environment variable(s) {variables} that are not set')

    return resolved_value


def format_snmp_error(error):
    error_status = getattr(error, 'error_status', None)
    error_status_name = SNMP_ERROR_STATUS_NAMES.get(error_status)
    if error_status_name is None:
        return str(error)

    offending_oid = getattr(error, 'offending_oid', None) or 'unknown'
    return f'{error_status_name} (status-code: {error_status}) on OID {offending_oid}'


def to_json_safe(value):
    if isinstance(value, dict):
        return {str(to_json_safe(key)): to_json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [to_json_safe(item) for item in value]
    if isinstance(value, (bytes, bytearray)):
        try:
            text = bytes(value).decode('utf-8')
        except UnicodeDecodeError:
            return bytes(value).hex()
        if all(char.isprintable() or char in '\r\n\t' for char in text):
            return text
        return bytes(value).hex()
    if isinstance(value, timedelta):
        return value.total_seconds()
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    return str(value)
