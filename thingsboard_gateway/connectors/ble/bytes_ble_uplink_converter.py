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

from pprint import pformat
from re import findall

from thingsboard_gateway.connectors.ble.ble_uplink_converter import BLEUplinkConverter
from thingsboard_gateway.gateway.constants import REPORT_STRATEGY_PARAMETER
from thingsboard_gateway.gateway.entities.converted_data import ConvertedData
from thingsboard_gateway.gateway.entities.report_strategy_config import ReportStrategyConfig
from thingsboard_gateway.gateway.entities.telemetry_entry import TelemetryEntry
from thingsboard_gateway.gateway.statistics.decorators import CollectStatistics
from thingsboard_gateway.gateway.statistics.statistics_service import StatisticsService
from thingsboard_gateway.tb_utility.tb_utility import TBUtility


class BytesBLEUplinkConverter(BLEUplinkConverter):
    def __init__(self, config, logger):
        self._log = logger
        self.__config = config

    @CollectStatistics(start_stat_type='receivedBytesFromDevices',
                       end_stat_type='convertedBytesFromDevice')
    def convert(self, config, data):
        converted_data = ConvertedData(device_name=self.__config['deviceName'],
                                        device_type=self.__config['deviceType'])

        if data is None:
            return converted_data

        device_report_strategy = None
        try:
            device_report_strategy = ReportStrategyConfig(self.__config.get(REPORT_STRATEGY_PARAMETER))
        except ValueError as e:
            self._log.trace("Report strategy config is not specified for device %s: %s", self.__config['deviceName'], e)

        try:
            for section in ('telemetry', 'attributes'):
                for item in data[section]:
                    try:
                        expression_arr = findall(r'\[[^\s][0-9:]*]', item['valueExpression'])
                        decoded_data = item['valueExpression']

                        for exp in expression_arr:
                            indexes = exp[1:-1].split(':')

                            data_to_replace = ''
                            if len(indexes) == 2:
                                from_index, to_index = indexes
                                concat_arr = item['data'][
                                             int(from_index) if from_index != '' else None:int(
                                                 to_index) if to_index != '' else None]
                                for sub_item in concat_arr:
                                    data_to_replace += str(sub_item)
                            else:
                                data_to_replace += str(item['data'][int(indexes[0])])

                            decoded_data = decoded_data.replace(exp, data_to_replace)

                        if item.get('key') is not None:
                            datapoint_key = TBUtility.convert_key_to_datapoint_key(item['key'], device_report_strategy,
                                                                                   item, self._log)
                            if section == 'attributes':
                                converted_data.add_to_attributes(datapoint_key, decoded_data)
                            else:
                                telemetry_entry = TelemetryEntry({datapoint_key: decoded_data})
                                converted_data.add_to_telemetry(telemetry_entry)
                        else:
                            self._log.error('Key for %s not found in config: %s', config['type'], config[section])
                    except Exception as e:
                        self._log.exception('\nException caught when processing data for %s\n\n%s', pformat(config), e)

        except Exception as e:
            StatisticsService.count_connector_message(self._log.name, 'convertersMsgDropped')
            self._log.exception(e)

        self._log.debug('Converted data: %s', converted_data)
        StatisticsService.count_connector_message(self._log.name, 'convertersAttrProduced',
                                                  count=converted_data.attributes_datapoints_count)
        StatisticsService.count_connector_message(self._log.name, 'convertersTsProduced',
                                                  count=converted_data.telemetry_datapoints_count)
        return converted_data
