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

import time
from re import findall

from simplejson import dumps

from thingsboard_gateway.connectors.mqtt.mqtt_uplink_converter import MqttUplinkConverter
from thingsboard_gateway.gateway.constants import REPORT_STRATEGY_PARAMETER
from thingsboard_gateway.gateway.entities.converted_data import ConvertedData
from thingsboard_gateway.gateway.entities.report_strategy_config import ReportStrategyConfig
from thingsboard_gateway.gateway.entities.telemetry_entry import TelemetryEntry
from thingsboard_gateway.gateway.statistics.decorators import CollectStatistics
from thingsboard_gateway.gateway.statistics.statistics_service import StatisticsService
from thingsboard_gateway.tb_utility.tb_utility import TBUtility
from thingsboard_gateway.connectors.mqtt.utils import Utils


class BytesMqttUplinkConverter(MqttUplinkConverter):
    SUPPORTS_BYTES_PAYLOAD = True

    def __init__(self, config, logger):
        self.__config = config.get('converter')
        self._log = logger

    @property
    def config(self):
        return self.__config

    @config.setter
    def config(self, value):
        self.__config = value

    @CollectStatistics(start_stat_type='receivedBytesFromDevices',
                       end_stat_type='convertedBytesFromDevice')
    def convert(self, topic, data):
        StatisticsService.count_connector_message(self._log.name, 'convertersMsgProcessed')

        datatypes = {"attributes": "attributes",
                     "timeseries": "telemetry"}

        device_name = self.parse_data(self.__config['deviceInfo']['deviceNameExpression'], data)

        device_report_strategy = None
        try:
            device_report_strategy = ReportStrategyConfig(self.__config.get(REPORT_STRATEGY_PARAMETER))
        except ValueError as e:
            self._log.trace("Report strategy config is not specified for device %s: %s", device_name, e)

        device_type = self.parse_data(self.__config['deviceInfo']['deviceProfileExpression'], data)
        converted_data = ConvertedData(device_name=device_name, device_type=device_type)
        timestamp = int(time.time() * 1000)
        try:
            for datatype in datatypes:
                for datatype_config in self.__config.get(datatype, []):
                    if datatype_config.get('keySource', 'message') == 'topic':
                        key = Utils.get_value_from_topic(topic, datatype_config['key'])
                    else:
                        key = self.parse_data(datatype_config['key'],
                                              data,
                                              hex_mode=datatype_config.get('hexMode', False))
                    value = self.parse_data(datatype_config['value'],
                                            data,
                                            hex_mode=datatype_config.get('hexMode', False))
                    datapoint_key = TBUtility.convert_key_to_datapoint_key(key,
                                                                           device_report_strategy,
                                                                           datatype_config,
                                                                           self._log)
                    if datatype == 'timeseries':
                        converted_data.add_to_telemetry(TelemetryEntry({datapoint_key: value}, ts=timestamp))
                    else:
                        converted_data.add_to_attributes({datapoint_key: value})
        except Exception as e:
            self._log.error('Error in converter, for config: \n%s\n and message: \n%s\n %s',
                            dumps(self.__config),
                            str(data), e)
            StatisticsService.count_connector_message(self._log.name, 'convertersMsgDropped')

        self._log.debug('Converted data: %s', converted_data)

        StatisticsService.count_connector_message(self._log.name, 'convertersAttrProduced',
                                                  count=converted_data.attributes_datapoints_count)
        StatisticsService.count_connector_message(self._log.name, 'convertersTsProduced',
                                                  count=converted_data.telemetry_datapoints_count)

        return converted_data

    @staticmethod
    def parse_data(expression: str, data: list, hex_mode: bool = False) -> str:
        expression_arr = findall(r'\[\S[0-9:]*]', expression)
        converted_data = expression

        for exp in expression_arr:
            indexes = exp[1:-1].split(':')
            data_to_replace = ''

            if len(indexes) == 2:
                from_index = int(indexes[0]) if indexes[0] else None
                to_index = int(indexes[1]) if indexes[1] else None
                slice_data = data[from_index:to_index]
            else:
                slice_data = [data[int(indexes[0])]]

            if hex_mode:
                data_to_replace = ''.join(f'{int(b):02x}' for b in slice_data)
            else:
                data_to_replace = ''.join(str(b) for b in slice_data.decode('utf-8'))

            converted_data = converted_data.replace(exp, data_to_replace)

        return converted_data
