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


from bacpypes3.basetypes import DateTime

from thingsboard_gateway.connectors.bacnet.bacnet_converter import AsyncBACnetConverter
from thingsboard_gateway.connectors.bacnet.entities.uplink_converter_config import UplinkConverterConfig
from thingsboard_gateway.gateway.entities.converted_data import ConvertedData
from thingsboard_gateway.gateway.entities.report_strategy_config import ReportStrategyConfig
from thingsboard_gateway.gateway.statistics.statistics_service import StatisticsService
from thingsboard_gateway.tb_utility.tb_utility import TBUtility


class AsyncBACnetUplinkConverter(AsyncBACnetConverter):
    def __init__(self, config: UplinkConverterConfig, logger):
        self.__log = logger
        self.__config = config

    def convert(self, data):
        if len(data):
            StatisticsService.count_connector_message(self.__log.name, 'convertersMsgProcessed')
            converted_data = ConvertedData(device_name=self.__config.device_name, device_type=self.__config.device_type)
            converted_data_append_methods = {
                'attributes': converted_data.add_to_attributes,
                'telemetry': converted_data.add_to_telemetry
            }

            device_report_strategy = self._get_device_report_strategy(self.__config.report_strategy,
                                                                      self.__config.device_name)

            for config, value in zip(self.__config.objects_to_read, data):
                if isinstance(value, Exception):
                    self.__log.error("Error reading object for key \"%s\", objectId: \"%s\", and propertyId: \"%s\". Error: %s",
                                     config.get('key'),
                                     config.get('objectId',
                                                config.get("objectType", "None") + ":" + str(config.get("objectId", "None"))
                                                ),
                                     config.get('propertyId'),
                                     value)
                    continue
                try:
                    if isinstance(value, DateTime):
                        value = value.isoformat()

                    datapoint_key = TBUtility.convert_key_to_datapoint_key(config['key'],
                                                                           device_report_strategy,
                                                                           config,
                                                                           self.__log)
                    converted_data_append_methods[config['type']]({datapoint_key: round(value, 2) if isinstance(value, float) else value})
                except Exception as e:
                    self.__log.error("Error converting datapoint with key %s: %s", config.get('key'), e)

            StatisticsService.count_connector_message(self.__log.name,
                                                      'convertersAttrProduced',
                                                      count=converted_data.attributes_datapoints_count)
            StatisticsService.count_connector_message(self.__log.name,
                                                      'convertersTsProduced',
                                                      count=converted_data.telemetry_datapoints_count)

            self.__log.debug("Converted data: %s", converted_data)
            return converted_data

    def _get_device_report_strategy(self, report_strategy, device_name):
        try:
            return ReportStrategyConfig(report_strategy)
        except ValueError as e:
            self.__log.trace("Report strategy config is not specified for device %s: %s", device_name, e)
