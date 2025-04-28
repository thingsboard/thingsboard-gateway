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
from concurrent.futures import ThreadPoolExecutor
from datetime import timezone
from time import time

from thingsboard_gateway.connectors.converter import Converter
from asyncua.ua.uatypes import VariantType

from thingsboard_gateway.connectors.opcua.opcua_converter import OpcUaConverter
from thingsboard_gateway.gateway.constants import TELEMETRY_PARAMETER, ATTRIBUTES_PARAMETER
from thingsboard_gateway.gateway.entities.converted_data import ConvertedData
from thingsboard_gateway.gateway.entities.telemetry_entry import TelemetryEntry
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

ERROR_MSG_TEMPLATE = "Bad status code: {} for node: {} with description {}"

class OpcuaJsonUplinkConverter(Converter):
    def __init__(self, config, logger):
        self._log = logger
        self.__config = config

    @staticmethod
    def process_datapoint(config, val, basic_timestamp):
        try:
            error = None
            data = val.Value.Value
            if isinstance(data, list):
                data = [str(item) for item in data]
            else:
                handler = VARIANT_TYPE_HANDLERS.get(val.Value.VariantType, lambda d: str(d) if not hasattr(d, 'to_string') else d.to_string())
                value = handler(data)
                try:
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
                except Exception as e:
                    data = str(e)
                    error = True

            if data is None and val.StatusCode.is_bad():
                data = str.format(ERROR_MSG_TEMPLATE,val.StatusCode.name, val.data_type, val.StatusCode.doc)
                error = True

            timestamp_location = config.get('timestampLocation', 'gateway').lower()
            timestamp = basic_timestamp  # Default timestamp
            if timestamp_location == 'sourcetimestamp' and val.SourceTimestamp is not None:
                timestamp = val.SourceTimestamp.timestamp() * 1000
            elif timestamp_location == 'servertimestamp' and val.ServerTimestamp is not None:
                timestamp = val.ServerTimestamp.timestamp() * 1000

            section = DATA_TYPES[config['section']]
            if section == TELEMETRY_PARAMETER:
                return TelemetryEntry({config['key']: data}, ts=timestamp), error
            elif section == ATTRIBUTES_PARAMETER:
                return {config['key']: data}, error
        except Exception as e:
            return None, str(e)

    def convert(self, configs, values) -> ConvertedData:
        StatisticsService.count_connector_message(self._log.name, 'convertersMsgProcessed')
        basic_timestamp = int(time() * 1000)
        basic_timestamp = round(basic_timestamp / 100) * 100

        is_debug_enabled = self._log.isEnabledFor(5)

        try:
            if not isinstance(configs, list):
                configs = [configs]
            if not isinstance(values, list):
                values = [values]

            if is_debug_enabled:
                start_iteration = basic_timestamp
            converted_data = ConvertedData(device_name=self.__config['device_name'], device_type=self.__config['device_type'])

            max_workers = min(8, len(values))

            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                results = list(executor.map(self.process_datapoint, configs, values, [basic_timestamp] * len(values)))

            telemetry_batch = []
            attributes_batch = []

            if is_debug_enabled:
                filling_start_time = int(time() * 1000)

            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                future_telemetry = executor.submit(self.fill_telemetry, results)
                future_attributes = executor.submit(self.fill_attributes, results)
                telemetry_batch = future_telemetry.result()
                attributes_batch = future_attributes.result()

            converted_data.add_to_telemetry(telemetry_batch)
            for attr in attributes_batch:
                converted_data.add_to_attributes(attr)

            if is_debug_enabled:
                converted_data_fill_time = int(time() * 1000) - filling_start_time
            total_datapoints_in_converted_data = converted_data.telemetry_datapoints_count + converted_data.attributes_datapoints_count

            # if is_debug_enabled:
            #     self._log.trace("Iteration took %d ms", int(time() * 1000) - start_iteration)
            #     self._log.trace("Filling took %d ms", converted_data_fill_time)
            #     self._log.trace("Average time per iteration: %2f ms", (float(int(time() * 1000)) - start_iteration) / float(len(values)))
            #     self._log.trace("Average filling time: %2f ms", float(converted_data_fill_time) / float(total_datapoints_in_converted_data))
            #     self._log.trace("Total datapoints in converted data: %d", total_datapoints_in_converted_data)

            StatisticsService.count_connector_message(self._log.name, 'convertersAttrProduced', count=converted_data.attributes_datapoints_count)
            StatisticsService.count_connector_message(self._log.name, 'convertersTsProduced', count=converted_data.telemetry_datapoints_count)

            return converted_data
        except Exception as e:
            self._log.exception("Error occurred while converting data: ", exc_info=e)
            StatisticsService.count_connector_message(self._log.name, 'convertersMsgDropped')

    @staticmethod
    def fill_telemetry(results):
        telemetry_batch = []
        for result, error in results:
            if isinstance(result, TelemetryEntry):
                telemetry_batch.append(result)
        return telemetry_batch

    @staticmethod
    def fill_attributes(results):
        attributes_batch = []
        for result, error in results:
            if isinstance(result, dict):
                attributes_batch.append(result)
        return attributes_batch
