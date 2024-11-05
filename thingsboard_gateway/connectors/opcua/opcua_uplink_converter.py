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

from asyncua.ua.uatypes import VariantType

from thingsboard_gateway.connectors.opcua.opcua_converter import OpcUaConverter
from thingsboard_gateway.gateway.constants import TELEMETRY_PARAMETER, ATTRIBUTES_PARAMETER, REPORT_STRATEGY_PARAMETER
from thingsboard_gateway.gateway.entities.converted_data import ConvertedData
from thingsboard_gateway.gateway.entities.report_strategy_config import ReportStrategyConfig
from thingsboard_gateway.gateway.entities.telemetry_entry import TelemetryEntry
from thingsboard_gateway.gateway.statistics.statistics_service import StatisticsService
from thingsboard_gateway.tb_utility.tb_utility import TBUtility

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


class OpcUaUplinkConverter(OpcUaConverter):
    def __init__(self, config, logger):
        self._log = logger
        self.__config = config

    def process_datapoint(self, config, val, basic_timestamp, device_report_strategy):
        try:
            error = None
            data = val.Value.Value
            if isinstance(data, list):
                data = [str(item) for item in data]
            else:
                handler = VARIANT_TYPE_HANDLERS.get(val.Value.VariantType, lambda d: d if not hasattr(d, 'to_string') else d.to_string())
                data = handler(data)

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
            datapoint_key = TBUtility.convert_key_to_datapoint_key(config['key'], device_report_strategy, config, self._log)
            if section == TELEMETRY_PARAMETER:
                return TelemetryEntry({datapoint_key: data}, ts=timestamp), error
            elif section == ATTRIBUTES_PARAMETER:
                return {datapoint_key: data}, error
        except Exception as e:
            return None, str(e)

    def convert(self, configs, values) -> ConvertedData:
        StatisticsService.count_connector_message(self._log.name, 'convertersMsgProcessed')
        basic_timestamp = int(time() * 1000)

        try:
            if not isinstance(configs, list):
                configs = [configs]
            if not isinstance(values, list):
                values = [values]

            converted_data = ConvertedData(device_name=self.__config['device_name'], device_type=self.__config['device_type'])

            device_report_strategy = None
            try:
                device_report_strategy = ReportStrategyConfig(self.__config.get(REPORT_STRATEGY_PARAMETER))
            except ValueError as e:
                self._log.trace("Report strategy config is not specified for device %s: %s", self.__config['device_name'], e)

            telemetry_batch = []
            attributes_batch = []

            for config, val in zip(configs, values):
                result, error = self.process_datapoint(config, val, basic_timestamp, device_report_strategy)
                if result is not None:
                    if isinstance(result, TelemetryEntry):
                        telemetry_batch.append(result)
                    elif isinstance(result, dict):
                        attributes_batch.append(result)

            converted_data.add_to_telemetry(telemetry_batch)
            for attr in attributes_batch:
                converted_data.add_to_attributes(attr)

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
