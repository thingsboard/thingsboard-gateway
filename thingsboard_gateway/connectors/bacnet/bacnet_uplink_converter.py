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
from bacpypes3.constructeddata import AnyAtomic, Array
from bacpypes3.basetypes import ErrorType, PriorityValue, ObjectPropertyReference

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

    def convert(self, config, data):
        StatisticsService.count_connector_message(self.__log.name, 'convertersMsgProcessed')
        converted_data = ConvertedData(device_name=self.__config.device_name, device_type=self.__config.device_type)
        converted_data_append_methods = {
            'attributes': converted_data.add_to_attributes,
            'timeseries': converted_data.add_to_telemetry
        }

        device_report_strategy = self._get_device_report_strategy(self.__config.report_strategy,
                                                                  self.__config.device_name)

        for item_config in config:
            try:
                values_group = self.__find_values(data,
                                                  item_config['objectId'],
                                                  item_config['objectType'],
                                                  item_config['propertyId'])

                converted_values = self.__convert_data(values_group)
                if len(converted_values) > 0:
                    data_key, unsed_values = self.__get_data_key_name(item_config['key'], converted_values)

                    if len(unsed_values) == 1:
                        datapoint_key = TBUtility.convert_key_to_datapoint_key(data_key,
                                                                               device_report_strategy,
                                                                               item_config,
                                                                               self.__log)
                        converted_data_append_methods[item_config['type']]({datapoint_key: round(unsed_values[0]['value'], 2) if isinstance(unsed_values[0]['value'], float) else str(unsed_values[0]['value'])})  # noqa
                    else:
                        for item in unsed_values:
                            datapoint_key = TBUtility.convert_key_to_datapoint_key(f'{data_key}.{item["propName"]}',
                                                                                   device_report_strategy,
                                                                                   item_config,
                                                                                   self.__log)
                            converted_data_append_methods[item_config['type']]({datapoint_key: round(item['value'], 2) if isinstance(item['value'], float) else str(item['value'])}) # noqa
            except Exception as e:
                self.__log.error("Error converting data for item %s: %s", item_config, e)
                StatisticsService.count_connector_message(self.__log.name, 'convertersError', count=1)
                continue

        StatisticsService.count_connector_message(self.__log.name,
                                                  'convertersAttrProduced',
                                                  count=converted_data.attributes_datapoints_count)
        StatisticsService.count_connector_message(self.__log.name,
                                                  'convertersTsProduced',
                                                  count=converted_data.telemetry_datapoints_count)

        self.__log.debug("Converted data: %s", converted_data)
        return converted_data

    def __convert_data(self, data):
        converted_data = []

        for value_obj in data:
            try:
                object_id, value_prop_id, _, value = value_obj
                if isinstance(value, Exception) or isinstance(value, ErrorType):
                    self.__log.error("Error converting object with objectId: \"%s\", and propertyId: \"%s\". Error: %s",
                                     object_id,
                                     value_prop_id,
                                     value)
                    continue

                if isinstance(value, DateTime):
                    value = value.isoformat()
                elif isinstance(value, AnyAtomic):
                    value = str(value.get_value())
                elif isinstance(value, ObjectPropertyReference):
                    result = {
                        'objectId': str(value.objectIdentifier),
                        'propertyId': str(value.propertyIdentifier),
                        'propertyArrayIndex': str(value.propertyArrayIndex)
                    }
                    value = result
                elif isinstance(value, Array):
                    result = []
                    for item in value:
                        if isinstance(item, PriorityValue):
                            result.append(str(getattr(item, item._choice)))
                        else:
                            result.append(item)

                    value = result

                converted_data.append({'propName': str(value_prop_id), 'value': value})
            except Exception as e:
                self.__log.error("Error converting datapoint %s: %s", value_obj, e)

        return converted_data

    def _get_device_report_strategy(self, report_strategy, device_name):
        try:
            return ReportStrategyConfig(report_strategy)
        except ValueError as e:
            self.__log.trace("Report strategy config is not specified for device %s: %s", device_name, e)

    def __find_values(self, data, object_id, object_type, property_id):
        required_object_type = TBUtility.kebab_case_to_camel_case(object_type)
        return list(filter(
            lambda value: value[0][-1] == object_id and
            TBUtility.kebab_case_to_camel_case(str(value[0][0])) == required_object_type and
            TBUtility.kebab_case_to_camel_case(str(value[1])) in property_id,
            data))

    def __get_data_key_name(self, key_expression, data):
        unused_keys = []

        for value_item in data:
            key_camel_case = TBUtility.kebab_case_to_camel_case(value_item['propName'])
            if key_camel_case in key_expression:
                key_expression = key_expression.replace('${' + key_camel_case + '}', str(value_item['value']))
            else:
                unused_keys.append(value_item)

        return key_expression, unused_keys
