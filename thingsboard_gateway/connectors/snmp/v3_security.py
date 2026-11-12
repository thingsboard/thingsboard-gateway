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

from dataclasses import dataclass, replace
from time import monotonic

from puresnmp.adt import Message, PlainMessage, ScopedPDU
from puresnmp.credentials import V3, Auth, Priv
from puresnmp.exc import SnmpError
from puresnmp.plugins import auth as auth_plugins
from puresnmp.plugins import priv as privacy_plugins
from puresnmp.util import localise_key
from puresnmp_plugins.mpm.v3 import V3MPM
from puresnmp_plugins.security import usm
from x690.util import get_value_slice

from thingsboard_gateway.connectors.snmp.constants import (
    USM_AUTH_PARAMETERS_INDEX,
    V3_AUTH_PROTOCOLS,
    V3_PRIV_PROTOCOLS,
    V3_TIME_WINDOW_SECONDS
)
from thingsboard_gateway.connectors.snmp.utils import resolve_env_placeholders, validate_ber_structure

# RFC 3414 requires passphrases of at least 8 characters, some agents reject shorter ones
MIN_KEY_LENGTH = 8


@dataclass
class SNMPv3Security:
    credentials: V3
    context_name: bytes
    context_engine_id: bytes
    has_privacy: bool


class SNMPRemoteEngines:
    def __init__(self):
        self.__timings = {}

    def is_in_time_window(self, engine_id, engine_boots, engine_time):
        timing = self.__timings.get(engine_id)
        if (timing is None
                or engine_boots > timing['engine_boots']
                or (engine_boots == timing['engine_boots'] and engine_time > timing['engine_time'])):
            timing = {'engine_boots': engine_boots, 'engine_time': engine_time, 'updated_at': monotonic()}
            self.__timings[engine_id] = timing

        if engine_boots < timing['engine_boots']:
            return False

        local_engine_time = timing['engine_time'] + int(monotonic() - timing['updated_at'])
        return engine_time >= local_engine_time - V3_TIME_WINDOW_SECONDS


# TODO: refactor after puresnmp update (exhuma/puresnmp#162), classes below override puresnmp_plugins USM and V3MPM
class SNMPv3UserSecurityModel(usm.UserSecurityModel):
    def process_incoming_message(self, message, credentials, whole_msg):
        if not isinstance(credentials, V3):
            raise SnmpError('Supplied credentials is not a V3 instance')

        security_parameters = usm.USMSecurityParameters.decode(message.security_parameters)
        if security_parameters.user_name != credentials.username.encode('utf-8'):
            raise usm.UnknownUser(f'Unknown user {security_parameters.user_name!r}')

        if security_parameters.authoritative_engine_id not in self.local_config:
            raise usm.UnknownEngineId(f'Unexpected engine-id {security_parameters.authoritative_engine_id!r}')

        self.__verify_authentication(message, credentials, security_parameters, whole_msg)
        usm.check_security_level(message, credentials)
        message = self.__decrypt_message(message, credentials, security_parameters)
        usm.validate_usm_message(message)
        return message

    @staticmethod
    def __verify_authentication(message, credentials, security_parameters, whole_msg):
        if not message.header.flags.auth:
            return

        if credentials.auth is None:
            raise usm.UnsupportedSecurityLevel('Message requires authentication but auth-method is missing')

        is_authentic = auth_plugins.create(credentials.auth.method).authenticate_incoming_message(
            credentials.auth.key,
            _reset_received_digest(whole_msg),
            security_parameters.auth_params,
            security_parameters.authoritative_engine_id)
        if not is_authentic:
            raise usm.AuthenticationError('Incoming message could not be authenticated!')

    @staticmethod
    def __decrypt_message(message, credentials, security_parameters):
        if isinstance(message, PlainMessage):
            return message

        if credentials.priv is None:
            raise SnmpError('Attempting to decrypt a message without priv object')

        try:
            decrypted = privacy_plugins.create(credentials.priv.method).decrypt_data(
                localise_key(credentials, security_parameters.authoritative_engine_id),
                security_parameters.authoritative_engine_id,
                security_parameters.authoritative_engine_boots,
                security_parameters.authoritative_engine_time,
                security_parameters.priv_params,
                message.scoped_pdu.value)
            validate_ber_structure(decrypted)
            return replace(message, scoped_pdu=ScopedPDU.decode(decrypted))
        except Exception as e:
            raise usm.DecryptionError(f'Unable to decrypt message ({e})') from e


class SNMPv3MessageProcessingModel(V3MPM):
    def __init__(self, transport_handler, lcd):
        super().__init__(transport_handler, lcd)
        self.security_model = SNMPv3UserSecurityModel()

    def decode(self, whole_msg, credentials):
        message = Message.decode(whole_msg)
        return self.security_model.process_incoming_message(message, credentials, whole_msg).scoped_pdu.data


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


def _reset_received_digest(whole_msg):
    message = get_value_slice(whole_msg)
    version = get_value_slice(whole_msg, message.bounds.start)
    header = get_value_slice(whole_msg, version.next_value_index)
    security_parameters = get_value_slice(whole_msg, header.next_value_index)
    index = get_value_slice(whole_msg, security_parameters.bounds.start).bounds.start
    for _ in range(USM_AUTH_PARAMETERS_INDEX):
        index = get_value_slice(whole_msg, index).next_value_index

    auth_parameters = get_value_slice(whole_msg, index).bounds
    return (whole_msg[:auth_parameters.start]
            + bytes(auth_parameters.stop - auth_parameters.start)
            + whole_msg[auth_parameters.stop:])
