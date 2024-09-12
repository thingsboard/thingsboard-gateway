#     Copyright 2024. ThingsBoard
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

from datetime import timezone
from time import time

from thingsboard_gateway.connectors.converter import Converter
from asyncua.ua.uatypes import VariantType

from thingsboard_gateway.gateway.constants import TELEMETRY_PARAMETER, ATTRIBUTES_PARAMETER, DEVICE_NAME_PARAMETER, \
    DEVICE_TYPE_PARAMETER, TELEMETRY_VALUES_PARAMETER, TELEMETRY_TIMESTAMP_PARAMETER
from thingsboard_gateway.gateway.statistics.statistics_service import StatisticsService

DATA_TYPES = {
    'attributes': ATTRIBUTES_PARAMETER,
    'timeseries': TELEMETRY_PARAMETER
}

VARIANT_TYPE_HANDLERS = {
    VariantType.ExtensionObject: lambda data: str(data),
    VariantType.DateTime: lambda data: data.replace(
        tzinfo=timezone.utc).isoformat() if data.tzinfo is None else data.isoformat(),
    VariantType.StatusCode: lambda data: data.name,
    VariantType.QualifiedName: lambda data: data.to_string(),
    VariantType.NodeId: lambda data: data.to_string(),
    VariantType.ExpandedNodeId: lambda data: data.to_string(),
    VariantType.ByteString: lambda data: data.hex(),
    VariantType.XmlElement: lambda data: data.decode('utf-8'),
    VariantType.Guid: lambda data: str(data),
    VariantType.DiagnosticInfo: lambda data: data.to_string(),
    VariantType.Null: lambda data: None
}


class OpcuaJsonUplinkConverter(Converter):
    def __init__(self, config, logger):
        self._log = logger
        self.__config = config

    def __get_parsed_value(self, val):
        value = val.Value.Value

        if isinstance(value, list):
            value = [str(item) for item in value]
        elif value is not None and not isinstance(value, (int, float, str, bool, dict, type(None))):
            handler = VARIANT_TYPE_HANDLERS.get(val.Value.VariantType,
                                                lambda value: str(value) if not hasattr(value,
                                                                                      'to_string') else value.to_string())
            value = handler(value)

        if value is None and val.StatusCode.is_bad():
            msg = "Bad status code: %r for node: %r with description %r" % (val.StatusCode.name,
                                                                            val.data_type,
                                                                            val.StatusCode.doc)
            self._log.warning(msg)
            value = msg

        return value

    def __get_data_values(self, config, val):
        try:
            value = self.__get_parsed_value(val)

            data = {
                'Value': {
                    'value': value,
                    'VariantType': val.Value.VariantType._name_,
                    'IsArray': val.Value.IsArray if hasattr(val.Value, 'IsArray') else 'Unknown',
                },
                'SourceTimestamp': val.SourceTimestamp.isoformat(),
                'ServerTimestamp': val.ServerTimestamp.isoformat(),
                'SourcePicoseconds': val.SourcePicoseconds,
                'ServerPicoseconds': val.ServerPicoseconds,
                'DataType': {
                    'Identifier': val.data_type.Identifier,
                    'NamespaceIndex': val.data_type.NamespaceIndex,
                    'NodeIdType': '{}:{}'.format(val.data_type.NodeIdType._name_, val.data_type.NodeIdType._name_),
                },
                'Encoding': val.Encoding,
                'PlatformKey': config.get('key'),
                'Node': config['node'].__str__() if config.get('node') else 'Can\'t get node string representation',
            }

            return data
        except Exception as e:
            self._log.error('Cannot get data values: %s' % str(e))
            return {}

    def convert(self, configs, values):
        StatisticsService.count_connector_message(self._log.name, 'convertersMsgProcessed')

        try:
            if not isinstance(configs, list):
                configs = [configs]
            if not isinstance(values, list):
                values = [values]

            result_data = {
                DEVICE_NAME_PARAMETER: self.__config['device_name'],
                DEVICE_TYPE_PARAMETER: self.__config['device_type'],
                ATTRIBUTES_PARAMETER: [],
                TELEMETRY_PARAMETER: []}
            telemetry_datapoints = {}

            telemetry_datapoints_count = 0
            attributes_datapoints_count = 0

            basic_timestamp = int(time() * 1000)

            for val, config in zip(values, configs):
                if not val:
                    continue

                data = self.__get_data_values(config, val)

                section = DATA_TYPES[config['section']]

                if val.SourceTimestamp is not None:
                    if abs(basic_timestamp - val.SourceTimestamp.timestamp()) > 3600:
                        self._log.warning("Timestamps are not in sync for incoming value: %r. "
                                          "Value timestamp: %s, current timestamp: %s",
                                          val, val.SourceTimestamp.timestamp(), basic_timestamp)
                    else:
                        basic_timestamp = val.SourceTimestamp.timestamp() * 1000

                timestamp = basic_timestamp

                if val.SourceTimestamp and section == TELEMETRY_PARAMETER:
                    telemetry_datapoints_count += 1
                    if timestamp in telemetry_datapoints:
                        telemetry_datapoints[timestamp].update({config['key']: data})
                    else:
                        telemetry_datapoints[timestamp] = {config['key']: data}
                else:
                    attributes_datapoints_count += 1
                    result_data[section].append({config['key']: data})

            if telemetry_datapoints:
                result_data[TELEMETRY_PARAMETER].extend(
                    {TELEMETRY_TIMESTAMP_PARAMETER: timestamp, TELEMETRY_VALUES_PARAMETER: datapoints}
                    for timestamp, datapoints in telemetry_datapoints.items()
                )

            StatisticsService.count_connector_message(self._log.name, 'convertersAttrProduced',
                                                      count=attributes_datapoints_count)
            StatisticsService.count_connector_message(self._log.name, 'convertersTsProduced',
                                                      count=telemetry_datapoints_count)

            return result_data
        except Exception as e:
            self._log.exception(e)
            StatisticsService.count_connector_message(self._log.name, 'convertersMsgDropped')
