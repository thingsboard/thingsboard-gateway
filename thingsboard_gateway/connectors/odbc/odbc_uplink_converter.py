#     Copyright 2025. ThingsBoard
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

from thingsboard_gateway.connectors.odbc.odbc_converter import OdbcConverter
from thingsboard_gateway.gateway.constants import TIMESERIES_PARAMETER, ATTRIBUTES_PARAMETER
from thingsboard_gateway.gateway.entities.converted_data import ConvertedData
from thingsboard_gateway.gateway.entities.report_strategy_config import ReportStrategyConfig
from thingsboard_gateway.gateway.entities.telemetry_entry import TelemetryEntry
from thingsboard_gateway.gateway.statistics.decorators import CollectStatistics
from thingsboard_gateway.gateway.statistics.statistics_service import StatisticsService
from thingsboard_gateway.tb_utility.tb_utility import TBUtility


class OdbcUplinkConverter(OdbcConverter):

    DATATYPES = (TIMESERIES_PARAMETER, ATTRIBUTES_PARAMETER)

    def __init__(self, logger):
        self._log = logger

    @CollectStatistics(start_stat_type='receivedBytesFromDevices',
                       end_stat_type='convertedBytesFromDevice')
    def convert(self, config, data):
        if isinstance(config, str) and config == "*":
            if "deviceName" in data and (('timeseries' in data or 'telemetry' in data) and 'attributes' in data):
                converted_data = ConvertedData(data['deviceName'], data.get('deviceType', 'default'))
                if "timeseries" in data:
                    converted_data.add_to_telemetry(data['timeseries'])
                if "telemetry" in data:
                    converted_data.add_to_telemetry(data['telemetry'])

                converted_data.add_to_attributes(data['attributes'])
                return converted_data
            else:
                self._log.error("Failed to convert SQL data to TB format: no configuration provided")
                return ConvertedData(None, None)

        device_report_strategy = None
        try:
            device_report_strategy = ReportStrategyConfig(config if isinstance(config, dict) else None)
        except ValueError as e:
            self._log.trace("Device report strategy configuration is not provided: %s", str(e))

        converted_data = ConvertedData(None, None)
        for datatype in self.DATATYPES:
            for config_item in config.get(datatype, []):
                try:
                    full_key = None
                    value = None
                    if isinstance(config_item, str):
                        if isinstance(data, dict):
                            for key, value in data.items():
                                full_key = TBUtility.convert_key_to_datapoint_key(key, device_report_strategy, {}, self._log)
                                if full_key:
                                    if datatype == TIMESERIES_PARAMETER:
                                        converted_data.add_to_telemetry(TelemetryEntry({full_key: value}, data.get('ts')))
                                    elif datatype == ATTRIBUTES_PARAMETER:
                                        converted_data.add_to_attributes(full_key, value)
                            continue
                        elif isinstance(data, list):
                            for item in data:
                                for key, value in item.items():
                                    full_key = TBUtility.convert_key_to_datapoint_key(key, device_report_strategy, {}, self._log)
                                    if full_key:
                                        if datatype == TIMESERIES_PARAMETER:
                                            converted_data.add_to_telemetry(TelemetryEntry({full_key: value}, item.get('ts')))
                                        elif datatype == ATTRIBUTES_PARAMETER:
                                            converted_data.add_to_attributes(full_key, value)
                            continue
                    elif isinstance(config_item, dict):
                        if "nameExpression" in config_item:
                            name = eval(config_item["nameExpression"], globals(), data)
                        else:
                            name = config_item["name"]
                        full_key = TBUtility.convert_key_to_datapoint_key(name, device_report_strategy, config_item, self._log)

                        if "column" in config_item:
                            value = data[config_item["column"]]
                        elif "value" in config_item:
                            value = eval(config_item["value"], globals(), data)
                        else:
                            StatisticsService.count_connector_message(self._log.name, 'convertersMsgDropped')
                            self._log.error("Failed to convert SQL data to TB format: no column/value configuration item")
                            continue

                    if not full_key:
                        StatisticsService.count_connector_message(self._log.name, 'convertersMsgDropped')
                        self._log.error("Failed to convert SQL data to TB format: no full key")
                        continue

                    if datatype == TIMESERIES_PARAMETER:
                        converted_data.add_to_telemetry(TelemetryEntry({full_key: value}, data.get('ts')))
                    elif datatype == ATTRIBUTES_PARAMETER:
                        converted_data.add_to_attributes(full_key, value)

                    else:
                        StatisticsService.count_connector_message(self._log.name, 'convertersMsgDropped')
                        self._log.error("Failed to convert SQL data to TB format: unexpected configuration type '%s'",
                                  type(config_item))
                except Exception as e:
                    StatisticsService.count_connector_message(self._log.name, 'convertersMsgDropped')
                    self._log.error("Failed to convert SQL data to TB format: %s", str(e))

        return converted_data
