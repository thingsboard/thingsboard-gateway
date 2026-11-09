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

from dataclasses import dataclass

from puresnmp.credentials import V3, Auth, Priv

from thingsboard_gateway.connectors.snmp.constants import V3_AUTH_PROTOCOLS, V3_PRIV_PROTOCOLS
from thingsboard_gateway.connectors.snmp.utils import resolve_env_placeholders

# RFC 3414 requires passphrases of at least 8 characters, some agents reject shorter ones
MIN_KEY_LENGTH = 8


@dataclass
class SNMPv3Security:
    credentials: V3
    context_name: bytes
    context_engine_id: bytes
    has_privacy: bool


def parse_v3_security(security_config) -> SNMPv3Security:
    if not isinstance(security_config, dict):
        raise ValueError('"security" section is required for SNMPv3')

    username = resolve_env_placeholders(security_config.get('username'), 'security.username')
    if not username:
        raise ValueError('"security.username" is required for SNMPv3')

    auth = _parse_protocol_with_key(security_config, 'authProtocol', 'authKey', V3_AUTH_PROTOCOLS)
    priv = _parse_protocol_with_key(security_config, 'privProtocol', 'privKey', V3_PRIV_PROTOCOLS)
    if priv is not None and auth is None:
        raise ValueError('"security.privProtocol" requires "security.authProtocol", '
                         'SNMPv3 has no privacy without authentication')

    credentials = V3(str(username),
                     auth=Auth(*auth) if auth is not None else None,
                     priv=Priv(*priv) if priv is not None else None)

    return SNMPv3Security(credentials=credentials,
                          context_name=str(security_config.get('contextName') or '').encode(),
                          context_engine_id=_parse_context_engine_id(security_config.get('contextEngineId')),
                          has_privacy=priv is not None)


def _parse_protocol_with_key(security_config, protocol_parameter, key_parameter, protocols):
    protocol = security_config.get(protocol_parameter)
    key = resolve_env_placeholders(security_config.get(key_parameter), f'security.{key_parameter}')
    if not protocol and not key:
        return None

    if not protocol:
        raise ValueError(f'"security.{key_parameter}" is set, but "security.{protocol_parameter}" is missing')
    if not key:
        raise ValueError(f'"security.{key_parameter}" is required when "security.{protocol_parameter}" is set')

    method = protocols.get(str(protocol).upper().replace('-', '').replace('_', ''))
    if method is None:
        raise ValueError(f'Unsupported "security.{protocol_parameter}": {protocol!r}, '
                         f'supported values: {", ".join(protocols)}')

    key = str(key)
    if len(key) < MIN_KEY_LENGTH:
        raise ValueError(f'"security.{key_parameter}" must be at least {MIN_KEY_LENGTH} characters long, '
                         f'as RFC 3414 requires')

    return key.encode(), method


def _parse_context_engine_id(context_engine_id):
    if not context_engine_id:
        return b''

    value = str(context_engine_id).strip().replace(':', '')
    if value.lower().startswith('0x'):
        value = value[2:]

    try:
        return bytes.fromhex(value)
    except ValueError:
        raise ValueError(f'"security.contextEngineId" must be a hex string, got {context_engine_id!r}') from None
