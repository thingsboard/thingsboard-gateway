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

import json
from re import findall
from time import time

from thingsboard_gateway.connectors.xmpp.xmpp_converter import XmppConverter
from thingsboard_gateway.gateway.constants import REPORT_STRATEGY_PARAMETER
from thingsboard_gateway.gateway.entities.converted_data import ConvertedData
from thingsboard_gateway.gateway.entities.report_strategy_config import ReportStrategyConfig
from thingsboard_gateway.gateway.entities.telemetry_entry import TelemetryEntry
from thingsboard_gateway.gateway.statistics.decorators import CollectStatistics
from thingsboard_gateway.tb_utility.tb_utility import TBUtility
from thingsboard_gateway.gateway.statistics.statistics_service import StatisticsService


class XmppUplinkConverter(XmppConverter):
    def __init__(self, config, logger):
        self._log = logger
        self.__config = config
        self._datatypes = {"attributes": "attributes",
                           "timeseries": "telemetry"}

    def _convert_json(self, val):
        try:
            data = json.loads(val)

            device_name_tags = TBUtility.get_values(self.__config.get("deviceNameExpression"), data,
                                                    get_tag=True)
            device_name_values = TBUtility.get_values(self.__config.get("deviceNameExpression"), data,
                                                      expression_instead_none=True)

            device_name = self.__config.get("deviceNameExpression")
            for (device_name_tag, device_name_value) in zip(device_name_tags, device_name_values):
                is_valid_key = "${" in self.__config.get("deviceNameExpression") and "}" in \
                               self.__config.get("deviceNameExpression")
                device_name = device_name.replace('${' + str(device_name_tag) + '}',
                                                                              str(device_name_value)) \
                    if is_valid_key else device_name_tag

            device_type_tags = TBUtility.get_values(self.__config.get("deviceTypeExpression"), data,
                                                    get_tag=True)
            device_type_values = TBUtility.get_values(self.__config.get("deviceTypeExpression"), data,
                                                      expression_instead_none=True)

            device_type = self.__config.get("deviceTypeExpression")
            for (device_type_tag, device_type_value) in zip(device_type_tags, device_type_values):
                is_valid_key = "${" in self.__config.get("deviceTypeExpression") and "}" in \
                               self.__config.get("deviceTypeExpression")
                device_type = device_type.replace('${' + str(device_type_tag) + '}',
                                                                              str(device_type_value)) \
                    if is_valid_key else device_type_tag

            device_report_strategy = self._get_device_report_strategy(device_name)

            converted_data = ConvertedData(device_name=device_name, device_type=device_type)

            for datatype in self._datatypes:
                for datatype_config in self.__config.get(datatype, []):
                    values = TBUtility.get_values(datatype_config["value"], data, "json",
                                                  expression_instead_none=False)
                    values_tags = TBUtility.get_values(datatype_config["value"], data, "json",
                                                       get_tag=True)

                    keys = TBUtility.get_values(datatype_config["key"], data, "json",
                                                expression_instead_none=False)
                    keys_tags = TBUtility.get_values(datatype_config["key"], data, get_tag=True)

                    full_key = datatype_config["key"]
                    for (key, key_tag) in zip(keys, keys_tags):
                        is_valid_key = "${" in datatype_config["key"] and "}" in \
                                       datatype_config["key"]
                        full_key = full_key.replace('${' + str(key_tag) + '}',
                                                    str(key)) if is_valid_key else key_tag

                    full_value = datatype_config["value"]
                    for (value, value_tag) in zip(values, values_tags):
                        is_valid_value = "${" in datatype_config["value"] and "}" in \
                                         datatype_config["value"]

                        full_value = full_value.replace('${' + str(value_tag) + '}',
                                                        str(value)) if is_valid_value else value

                    if full_key != 'None' and full_value != 'None':
                        datapoint_key = TBUtility.convert_key_to_datapoint_key(full_key, device_report_strategy,
                                                                               datatype_config, self._log)

                        if datatype == 'attributes':
                            converted_data.add_to_attributes(datapoint_key, full_value)
                        else:
                            ts = data.get('ts', data.get('timestamp'))
                            telemetry_entry = TelemetryEntry({datapoint_key: full_value}, ts)
                            converted_data.add_to_telemetry(telemetry_entry)

            return converted_data
        except Exception as e:
            StatisticsService.count_connector_message(self._log.name, 'convertersMsgDropped')
            self._log.exception(e)
            return None

    def _get_value(self, data, config, key):
        expression_arr = findall(r'\[\S[\d:]*]', config[key])
        if expression_arr:
            indexes = expression_arr[0][1:-1].split(':')
            try:
                if len(indexes) == 2:
                    from_index, to_index = indexes
                    return data[int(from_index) if from_index != '' else None:int(to_index) if to_index != '' else None]
                else:
                    index = int(indexes[0])
                    return data[index]
            except IndexError:
                StatisticsService.count_connector_message(self._log.name, 'convertersMsgDropped')
                self._log.error('deviceName expression invalid (index out of range)')
                return None
        else:
            return config[key]

    def _get_device_report_strategy(self, device_name):
        device_report_strategy = None
        try:
            device_report_strategy = ReportStrategyConfig(self.__config.get(REPORT_STRATEGY_PARAMETER))
        except ValueError as e:
            self._log.trace("Report strategy config is not specified for device %s: %s", device_name, e)

        return device_report_strategy

    def _convert_text(self, val):
        device_name = self._get_value(val, self.__config, 'deviceNameExpression')
        device_type = self._get_value(val, self.__config, 'deviceTypeExpression')

        try:
            if device_name and device_type:
                device_report_strategy = self._get_device_report_strategy(device_name)
                converted_data = ConvertedData(device_name=device_name, device_type=device_type)

                for datatype in self._datatypes:
                    for datatype_config in self.__config.get(datatype, []):
                        key = self._get_value(val, datatype_config, 'key')
                        datapoint_key = TBUtility.convert_key_to_datapoint_key(key, device_report_strategy,
                                                                               datatype_config, self._log)
                        value = self._get_value(val, datatype_config, 'value')

                        if datatype == 'attributes':
                            converted_data.add_to_attributes(datapoint_key, value)
                        else:
                            ts = int(time()) * 1000
                            telemetry_entry = TelemetryEntry({datapoint_key: value}, ts)
                            converted_data.add_to_telemetry(telemetry_entry)

                return converted_data
        except Exception as e:
            StatisticsService.count_connector_message(self._log.name, 'convertersMsgDropped')
            self._log.exception(e)

    @CollectStatistics(start_stat_type='receivedBytesFromDevices',
                       end_stat_type='convertedBytesFromDevice')
    def convert(self, config, val):
        # convert data if it is json format
        result = self._convert_json(val)
        if result:
            StatisticsService.count_connector_message(self._log.name, 'convertersAttrProduced',
                                                      count=result.attributes_datapoints_count)
            StatisticsService.count_connector_message(self._log.name, 'convertersTsProduced',
                                                      count=result.telemetry_datapoints_count)
            return result

        # convert data using slices if it is text format
        result = self._convert_text(val)
        if result:
            StatisticsService.count_connector_message(self._log.name, 'convertersAttrProduced',
                                                      count=result.attributes_datapoints_count)
            StatisticsService.count_connector_message(self._log.name, 'convertersTsProduced',
                                                      count=result.telemetry_datapoints_count)
            return result

        # if none of above
        return None
