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

from thingsboard_gateway.connectors.knx.knx_converter import KNXConverter
from thingsboard_gateway.gateway.entities.converted_data import ConvertedData
from thingsboard_gateway.gateway.entities.report_strategy_config import ReportStrategyConfig
from thingsboard_gateway.gateway.statistics.statistics_service import StatisticsService
from thingsboard_gateway.tb_utility.tb_utility import TBUtility


class KNXUplinkConverter(KNXConverter):
    def __init__(self, config, logger):
        self.__log = logger
        self.__config = config

    def convert(self, data):
        StatisticsService.count_connector_message(self.__log.name, 'convertersMsgProcessed')
        device_name = self.__get_device_name(data)
        device_type = self.__get_device_type(data)

        converted_data = ConvertedData(device_name=device_name, device_type=device_type)

        device_report_strategy = self._get_device_report_strategy(self.__config.get('reportStrategy'),
                                                                  device_name)

        for section in ('attributes', 'timeseries'):
            for config in self.__config.get(section, []):
                try:
                    converted_value = data.get(config.get('groupAddress'))['response']
                    if converted_value is not None:
                        datapoint_key = TBUtility.convert_key_to_datapoint_key(config['key'],
                                                                               device_report_strategy,
                                                                               config,
                                                                               self.__log)

                        if section == 'attributes':
                            converted_data.add_to_attributes({datapoint_key: converted_value})
                        elif section == 'timeseries':
                            converted_data.add_to_telemetry({datapoint_key: converted_value})
                    else:
                        self.__log.warning('Received None value for group address %s', config.get('groupAddress'))
                except Exception as e:
                    self.__log.error('Error converting data: %s', e)

        StatisticsService.count_connector_message(self.__log.name,
                                                  'convertersAttrProduced',
                                                  count=converted_data.attributes_datapoints_count)
        StatisticsService.count_connector_message(self.__log.name,
                                                  'convertersTsProduced',
                                                  count=converted_data.telemetry_datapoints_count)

        self.__log.trace('Converted data: %s', data)
        return converted_data

    def __get_device_name(self, data):
        device_info = self.__config['deviceInfo']

        if device_info.get('deviceNameExpressionSource') == 'expression':
            group_address, converted_data = self.__find_converted_data_by_key('deviceName', data)
            return self.__replace_expression_with_value(device_info['deviceNameExpression'],
                                                        group_address,
                                                        converted_data)

        return device_info['deviceNameExpression']

    def __get_device_type(self, data):
        device_info = self.__config['deviceInfo']

        if device_info.get('deviceProfileExpressionSource') == 'expression':
            group_address, converted_data = self.__find_converted_data_by_key('deviceType', data)
            return self.__replace_expression_with_value(device_info['deviceTypeExpression'],
                                                        group_address,
                                                        converted_data)

        return device_info['deviceProfileNameExpression']

    @staticmethod
    def __find_converted_data_by_key(key, data):
        for group_address, value in data.items():
            if key in value['keys']:
                return group_address, value['response']

    @staticmethod
    def __replace_expression_with_value(expression, group_address, data):
        expression = expression.replace('${' + str(group_address) + '}', str(data))
        return expression

    def _get_device_report_strategy(self, report_strategy, device_name):
        try:
            return ReportStrategyConfig(report_strategy)
        except ValueError as e:
            self.__log.trace("Report strategy config is not specified for device %s: %s", device_name, e)
