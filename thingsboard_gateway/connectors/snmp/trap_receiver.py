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

import asyncio
from dataclasses import dataclass
from socket import gethostbyname

from puresnmp.adt import Message
from puresnmp.pdu import GetResponse, InformRequest, PDUContent, Trap
from puresnmp_plugins.security import usm
from x690 import decode
from x690.types import Integer, ObjectIdentifier, OctetString, Sequence, UnknownType

from thingsboard_gateway.connectors.snmp.constants import (
    SNMP_GENERIC_TRAPS_OID,
    SNMP_TRAP_ADDRESS_OID,
    SNMP_TRAP_ENTERPRISE_OID,
    SNMP_TRAP_OID,
    SNMP_V1_ENTERPRISE_SPECIFIC_TRAP,
    SNMP_V1_MESSAGE_VERSION,
    SNMP_V1_TRAP_PDU_TAG,
    SNMP_V2C_MESSAGE_VERSION,
    SNMP_V3_MESSAGE_VERSION,
    SYS_UPTIME_OID
)
from thingsboard_gateway.connectors.snmp.utils import to_json_safe, validate_ber_structure
from thingsboard_gateway.connectors.snmp.v3_security import SNMPRemoteEngines, SNMPv3UserSecurityModel


@dataclass
class SNMPTrap:
    version: int
    community: str
    trap_oid: str
    values: dict
    inform_pdu: InformRequest = None


class SNMPTrapReceiver(asyncio.DatagramProtocol):
    def __init__(self, devices, logger, callback):
        super().__init__()
        self.__log = logger
        self.__callback = callback
        self.__devices_by_ip = self.__group_devices_by_ip(devices)
        self.__transport = None
        self.__remote_engines = SNMPRemoteEngines()

    @property
    def has_devices(self):
        return bool(self.__devices_by_ip)

    def connection_made(self, transport):
        self.__transport = transport

    def datagram_received(self, data, addr):
        source_ip = addr[0]
        devices = self.__devices_by_ip.get(source_ip)
        if not devices:
            self.__log.debug("SNMP trap from unknown address %s skipped", source_ip)
            return

        try:
            validate_ber_structure(data)
            message, _ = decode(data, enforce_type=Sequence)
            version = message[0].pythonize()
            if version == SNMP_V3_MESSAGE_VERSION:
                trap, devices = self.__decode_v3_trap(data, devices, source_ip)
            elif version == SNMP_V2C_MESSAGE_VERSION:
                trap = self.__decode_v2c_trap(message, devices, addr)
            elif version == SNMP_V1_MESSAGE_VERSION:
                trap = self.__decode_v1_trap(message)
            else:
                self.__log.debug("SNMP trap message version %s from %s is not supported", version, source_ip)
                return
        except Exception as e:
            self.__log.warning("Failed to decode SNMP trap from %s: %s", source_ip, e)
            return

        if trap is None:
            return

        for device in devices:
            try:
                device_data = self.__get_device_data(device, trap)
                if device_data:
                    self.__callback(device, device_data)
            except Exception as e:
                self.__log.error("Failed to process SNMP trap %s for device \"%s\": %r",
                                 trap.trap_oid, device["deviceName"], e)

    def __decode_v1_trap(self, message):
        community = message[1].pythonize().decode('utf-8', errors='replace')
        pdu = message[2]
        if not isinstance(pdu, UnknownType) or pdu.tag != SNMP_V1_TRAP_PDU_TAG:
            self.__log.debug("SNMPv1 message with %s is not a trap", type(pdu).__name__)
            return None

        enterprise, index = decode(pdu.value, enforce_type=ObjectIdentifier)
        agent_address, index = decode(pdu.value, index)
        generic_trap, index = decode(pdu.value, index, enforce_type=Integer)
        specific_trap, index = decode(pdu.value, index, enforce_type=Integer)
        _, index = decode(pdu.value, index)
        varbinds, _ = decode(pdu.value, index, enforce_type=Sequence)

        enterprise_oid = enterprise.pythonize()
        generic_trap_type = generic_trap.pythonize()
        if generic_trap_type == SNMP_V1_ENTERPRISE_SPECIFIC_TRAP:
            trap_oid = f'{enterprise_oid}.0.{specific_trap.pythonize()}'
        else:
            trap_oid = f'{SNMP_GENERIC_TRAPS_OID}.{generic_trap_type + 1}'

        values = {oid.pythonize(): value.pythonize() for oid, value in varbinds}
        values[SNMP_TRAP_ADDRESS_OID] = agent_address.pythonize()
        values[SNMP_TRAP_ENTERPRISE_OID] = enterprise_oid
        return SNMPTrap(version=SNMP_V1_MESSAGE_VERSION, community=community, trap_oid=trap_oid, values=values)

    def __decode_v2c_trap(self, message, devices, addr):
        community = message[1].pythonize().decode('utf-8', errors='replace')
        trap = self.__trap_from_pdu(message[2], SNMP_V2C_MESSAGE_VERSION, community)
        if trap is not None and trap.inform_pdu is not None and self.__is_community_accepted(devices, community):
            self.__send_inform_response(trap, addr)
        return trap

    @staticmethod
    def __is_community_accepted(devices, community):
        return any(community == entry.get("community", device.get("community"))
                   for device in devices for entry in device["trap_entries"])

    def __send_inform_response(self, trap, addr):
        response_pdu = GetResponse(PDUContent(trap.inform_pdu.value.request_id, trap.inform_pdu.value.varbinds))
        response = Sequence([Integer(SNMP_V2C_MESSAGE_VERSION), OctetString(trap.community.encode('utf-8')),
                             response_pdu])
        self.__transport.sendto(bytes(response), addr)

    def __decode_v3_trap(self, data, devices, source_ip):
        message = Message.decode(data)
        validate_ber_structure(message.security_parameters)
        security_parameters = usm.USMSecurityParameters.decode(message.security_parameters)
        if not security_parameters.authoritative_engine_id:
            self.__log.warning("SNMPv3 inform from %s is not supported by SNMP connector, "
                               "configure the device to send SNMPv3 traps instead", source_ip)
            return None, []

        trap = None
        trap_devices = []
        for device in self.__get_v3_user_devices(devices, security_parameters.user_name, source_ip):
            plain_message = self.__process_v3_message(message, data, security_parameters, device, source_ip)
            if plain_message is None:
                continue

            if isinstance(plain_message.scoped_pdu.data, InformRequest):
                self.__log.warning("SNMPv3 inform from %s is not supported by SNMP connector, "
                                   "configure the device to send SNMPv3 traps instead", source_ip)
                return None, []

            if self.__is_replayed_v3_message(message, security_parameters, source_ip):
                return None, []

            device_trap = self.__trap_from_pdu(plain_message.scoped_pdu.data, SNMP_V3_MESSAGE_VERSION, None)
            if device_trap is None:
                continue

            trap = device_trap
            trap_devices.append(device)

        return trap, trap_devices

    def __get_v3_user_devices(self, devices, user_name, source_ip):
        username = user_name.decode('utf-8', errors='replace')
        user_devices = [device for device in devices
                        if device.get("v3_security") is not None
                        and device["v3_security"].credentials.username == username]
        if not user_devices:
            self.__log.debug("SNMPv3 trap from %s with user \"%s\" does not match any device", source_ip, username)

        return user_devices

    def __process_v3_message(self, message, data, security_parameters, device, source_ip):
        security_model = SNMPv3UserSecurityModel()
        security_model.set_engine_timing(security_parameters.authoritative_engine_id,
                                         security_parameters.authoritative_engine_boots,
                                         security_parameters.authoritative_engine_time)
        try:
            return security_model.process_incoming_message(message, device["v3_security"].credentials, data)
        except usm.DecryptionError:
            self.__log.warning("SNMPv3 trap from %s rejected for device \"%s\": unable to decrypt message",
                               source_ip, device["deviceName"])
        except Exception as e:
            self.__log.warning("SNMPv3 trap from %s rejected for device \"%s\": %s",
                               source_ip, device["deviceName"], e)

        return None

    def __is_replayed_v3_message(self, message, security_parameters, source_ip):
        if not message.header.flags.auth:
            return False

        engine_id = security_parameters.authoritative_engine_id
        if self.__remote_engines.is_in_time_window(engine_id, security_parameters.authoritative_engine_boots,
                                                   security_parameters.authoritative_engine_time):
            return False

        self.__log.warning("SNMPv3 trap from %s rejected: engine boots/time is older than already received "
                           "from engine ID %s (replayed message or engine reset without boots increment)",
                           source_ip, engine_id.hex())
        return True

    def __trap_from_pdu(self, pdu, version, community):
        if not isinstance(pdu, (Trap, InformRequest)):
            self.__log.debug("SNMP message with %s is not a trap", type(pdu).__name__)
            return None

        values = {varbind.oid.pythonize(): varbind.value.pythonize() for varbind in pdu.value.varbinds}
        trap_oid = values.pop(SNMP_TRAP_OID, None)
        values.pop(SYS_UPTIME_OID, None)
        if not isinstance(trap_oid, str) or not trap_oid:
            raise ValueError('Trap has no valid snmpTrapOID')

        return SNMPTrap(version=version, community=community, trap_oid=trap_oid, values=values,
                        inform_pdu=pdu if isinstance(pdu, InformRequest) else None)

    def __get_device_data(self, device, trap):
        device_data = {}
        community_mismatch = False
        for entry in device["trap_entries"]:
            if entry["oid"].lstrip('.') != trap.trap_oid:
                continue

            if (trap.version != SNMP_V3_MESSAGE_VERSION
                    and trap.community != entry.get("community", device.get("community"))):
                community_mismatch = True
                continue

            value = self.__get_entry_value(entry, trap)
            if value is not None:
                device_data[entry["key"]] = value

        if community_mismatch and not device_data:
            self.__log.debug("SNMP trap %s for device \"%s\" skipped: community does not match",
                             trap.trap_oid, device["deviceName"])

        return device_data

    @staticmethod
    def __get_entry_value(entry, trap):
        value_oid = entry.get("valueOid")
        if value_oid is None:
            return to_json_safe(trap.values)

        value_oid = value_oid.lstrip('.')
        for oid, value in trap.values.items():
            if oid == value_oid or oid.startswith(value_oid + '.'):
                return value

        return None

    def __group_devices_by_ip(self, devices):
        devices_by_ip = {}
        for device in devices:
            device["trap_entries"] = self.__get_trap_entries(device)
            if not device["trap_entries"]:
                continue

            try:
                ip = gethostbyname(device["ip"])
                devices_by_ip.setdefault(ip, []).append(device)
            except OSError as e:
                self.__log.error("Device \"%s\" will not receive SNMP traps, cannot resolve \"%s\": %s",
                                 device["deviceName"], device["ip"], e)

        return devices_by_ip

    def __get_trap_entries(self, device):
        trap_entries = []
        for datatype in ('attributes', 'telemetry'):
            for entry in device.get(datatype, []):
                if str(entry.get("method", "")).lower() != "trap":
                    continue

                try:
                    self.__validate_trap_entry(entry)
                    trap_entries.append(entry)
                except ValueError as e:
                    self.__log.error("Trap entry %r of device \"%s\" skipped: %s", entry, device["deviceName"], e)

        return trap_entries

    @staticmethod
    def __validate_trap_entry(entry):
        for parameter in ("key", "oid"):
            if not isinstance(entry.get(parameter), str) or not entry[parameter]:
                raise ValueError(f'"{parameter}" is required and must be a string')

        if not isinstance(entry.get("valueOid", ""), str):
            raise ValueError('"valueOid" must be a string')
