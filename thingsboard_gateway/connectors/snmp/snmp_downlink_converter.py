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

from ipaddress import IPv4Address, AddressValueError

from puresnmp.types import Integer, OctetString, IpAddress, Counter, Gauge, Counter64, TimeTicks

from thingsboard_gateway.connectors.converter import Converter
from thingsboard_gateway.gateway.statistics.decorators import CollectStatistics
from thingsboard_gateway.tb_utility.tb_utility import TBUtility


class SNMPDownlinkConverter(Converter):
    _INT_HANDLER = lambda v: Integer(int(v))
    _COUNTER_HANDLER = lambda v: Counter(int(v))
    _COUNTER64_HANDLER = lambda v: Counter64(int(v))
    _GAUGE_HANDLER = lambda v: Gauge(int(v))
    _TIMETICKS_HANDLER = lambda v: TimeTicks(int(v))
    _OCTETSTRING_HANDLER = lambda v: OctetString(v.encode() if isinstance(v, str) else v)
    _IPADDRESS_HANDLER = lambda v: IpAddress(IPv4Address(v))

    _TYPE_HANDLERS = {
        'INTEGER': _INT_HANDLER,
        'INT': _INT_HANDLER,
        'COUNTER': _COUNTER_HANDLER,
        'COUNTER32': _COUNTER_HANDLER,
        'COUNTER64': _COUNTER64_HANDLER,
        'GAUGE': _GAUGE_HANDLER,
        'GAUGE32': _GAUGE_HANDLER,
        'TIMETICKS': _TIMETICKS_HANDLER,
        'OCTETSTRING': _OCTETSTRING_HANDLER,
        'STRING': _OCTETSTRING_HANDLER,
        'STR': _OCTETSTRING_HANDLER,
        'IPADDRESS': _IPADDRESS_HANDLER,
    }

    def __init__(self, config):
        self.__config = config

    @CollectStatistics(start_stat_type='allReceivedBytesFromTB',
                       end_stat_type='allBytesSentToDevices')
    def convert(self, config, data):
        mappings = config.get("mappings")
        if mappings is not None:
            return self.__convert_mappings(config, mappings, data)

        value = data.get("params")
        if value is None:
            return Integer(0)

        return self.__convert_value(value, config.get("type", "OCTETSTRING"))

    def __convert_mappings(self, config, mappings, data):
        default_type = config.get("type", "OCTETSTRING")
        converted_mappings = {}

        if isinstance(mappings, list):
            for mapping in mappings:
                oid = mapping.get("oid")
                value = self.__resolve_mapping_value(mapping.get("value"), data)
                snmp_type = mapping.get("type", default_type)
                converted_mappings[oid] = self.__convert_value(value, snmp_type)

            return converted_mappings

        for oid, mapping_value in mappings.items():
            if isinstance(mapping_value, dict):
                value = self.__resolve_mapping_value(mapping_value.get("value"), data)
                snmp_type = mapping_value.get("type", default_type)
            else:
                value = self.__resolve_mapping_value(mapping_value, data)
                snmp_type = default_type

            converted_mappings[oid] = self.__convert_value(value, snmp_type)

        return converted_mappings

    @staticmethod
    def __resolve_mapping_value(value_expression, data):
        if not isinstance(value_expression, str):
            return value_expression

        tags = TBUtility.get_values(value_expression, data, 'params', get_tag=True)
        values = TBUtility.get_values(value_expression, data, 'params', expression_instead_none=True)

        resolved_value = value_expression
        for (tag, value) in zip(tags, values):
            resolved_value = resolved_value.replace('${' + tag + '}', str(value))

        return resolved_value

    def __convert_value(self, value, snmp_type):
        snmp_type = snmp_type.upper()
        handler = self._TYPE_HANDLERS.get(snmp_type)

        if handler:
            try:
                return handler(value)
            except (ValueError, TypeError, AddressValueError):
                raise ValueError(f"Cannot convert value '{value}' to type '{snmp_type}'")

        raise ValueError(f"Unsupported SNMP type: {snmp_type}")
