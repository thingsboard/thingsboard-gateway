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

from time import time
from datetime import timezone

from thingsboard_gateway.connectors.opcua.opcua_converter import OpcUaConverter
from asyncua.ua.uatypes import LocalizedText, VariantType

from thingsboard_gateway.gateway.constants import TELEMETRY_PARAMETER, ATTRIBUTES_PARAMETER, DEVICE_NAME_PARAMETER, \
    DEVICE_TYPE_PARAMETER, TELEMETRY_VALUES_PARAMETER, TELEMETRY_TIMESTAMP_PARAMETER

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


class OpcUaUplinkConverter(OpcUaConverter):
    def __init__(self, config, logger):
        self._log = logger
        self.__config = config

    def convert(self, configs, values):
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

        for val, config in zip(values, configs):
            if not val:
                continue

            data = val.Value.Value

            if isinstance(data, list):
                data = [str(item) for item in data]
            elif data is not None and not isinstance(data, (int, float, str, bool, dict, type(None))):
                handler = VARIANT_TYPE_HANDLERS.get(val.Value.VariantType,
                                                    lambda data: str(data) if not hasattr(data, 'to_string') else data.to_string())
                data = handler(data)

            if data is None and val.StatusCode.is_bad():
                msg = "Bad status code: %r for node: %r with description %r" % (val.StatusCode.name,
                                                                                val.data_type,
                                                                                val.StatusCode.doc)
                self._log.warning(msg)
                data = msg

            section = DATA_TYPES[config['section']]
            if val.SourceTimestamp and section == TELEMETRY_PARAMETER:
                timestamp = int(val.SourceTimestamp.timestamp() * 1000)
                if timestamp in telemetry_datapoints:
                    telemetry_datapoints[timestamp].update({config['key']: data})
                else:
                    telemetry_datapoints[timestamp] = {config['key']: data}
            else:
                result_data[section].append({config['key']: data})

        if telemetry_datapoints:
            result_data[TELEMETRY_PARAMETER].extend(
                {TELEMETRY_TIMESTAMP_PARAMETER: timestamp, TELEMETRY_VALUES_PARAMETER: datapoints}
                for timestamp, datapoints in telemetry_datapoints.items()
            )

        return result_data
