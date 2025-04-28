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


from simplejson import dumps

from thingsboard_gateway.connectors.rest.rest_converter import RESTConverter
from thingsboard_gateway.gateway.constants import REPORT_STRATEGY_PARAMETER
from thingsboard_gateway.gateway.entities.converted_data import ConvertedData
from thingsboard_gateway.gateway.entities.report_strategy_config import ReportStrategyConfig
from thingsboard_gateway.gateway.entities.telemetry_entry import TelemetryEntry
from thingsboard_gateway.gateway.statistics.decorators import CollectStatistics
from thingsboard_gateway.tb_utility.tb_utility import TBUtility
from thingsboard_gateway.gateway.statistics.statistics_service import StatisticsService


class JsonRESTUplinkConverter(RESTConverter):

    def __init__(self, config, logger):
        self._log = logger
        self.__config = config

    @CollectStatistics(start_stat_type='receivedBytesFromDevices',
                       end_stat_type='convertedBytesFromDevice')
    def convert(self, config, data):
        datatypes = {"attributes": "attributes",
                     "timeseries": "telemetry"}
        device_name = None
        device_type = None

        try:
            device_info = self.__config.get("deviceInfo")
            if device_info.get("deviceNameExpression") is not None:
                if device_info.get("deviceNameExpressionSource") == "constant":
                    device_name = device_info.deviceNameExpression
                else:
                    device_name_tags = TBUtility.get_values(device_info.get("deviceNameExpression"), data, get_tag=True)
                    device_name_values = TBUtility.get_values(device_info.get("deviceNameExpression"), data,
                                                              expression_instead_none=True)
                    device_name = device_info.get("deviceNameExpression")
                    for (device_name_tag, device_name_value) in zip(device_name_tags, device_name_values):
                        is_valid_key = "${" in device_info.get("deviceNameExpression") and "}" in \
                                       device_info.get("deviceNameExpression")
                        device_name = device_name.replace('${' + str(device_name_tag) + '}',
                                                          str(device_name_value)) \
                            if is_valid_key else device_name_tag
            else:
                self._log.error("The expression for looking \"device name\" not found in config %s",
                                dumps(device_info))

            if device_info.get("deviceProfileExpression") is not None:
                if device_info.get("deviceProfileExpressionSource") == "constant":
                    device_type = device_info.get("deviceProfileExpression")
                else:
                    device_profile_tags = TBUtility.get_values(device_info.get("deviceProfileExpression"), data,
                                                               get_tag=True)
                    device_profile_values = TBUtility.get_values(device_info.get("deviceProfileExpression"), data,
                                                                 expression_instead_none=True)
                    device_type = device_info.get("deviceProfileExpression")
                    for (device_profile_tag, device_profile_value) in zip(device_profile_tags, device_profile_values):
                        is_valid_key = "${" in device_info.get("deviceProfileExpression") and "}" in \
                                       device_info.get("deviceProfileExpression")
                        device_type = device_type.replace('${' + str(device_profile_tag) + '}',
                                                          str(device_profile_value)) \
                            if is_valid_key else device_profile_tag
            else:
                self._log.error("The expression for looking \"device profile\" not found in config %s",
                                dumps(device_info))
        except Exception as e:
            StatisticsService.count_connector_message(self._log.name, 'convertersMsgDropped')
            self._log.error('Error in converter, for config: \n%s\n and message: \n%s\n %s', dumps(self.__config), data,
                            e)

        converted_data = ConvertedData(device_name=device_name, device_type=device_type)
        device_report_strategy = None
        try:
            device_report_strategy = ReportStrategyConfig(self.__config.get(REPORT_STRATEGY_PARAMETER))
        except ValueError as e:
            self._log.trace("Report strategy config is not specified for device %s: %s", device_name, e)

        try:
            for datatype in datatypes:
                for datatype_config in self.__config.get(datatype, []):
                    values = TBUtility.get_values(datatype_config["value"], data, datatype_config["type"],
                                                  expression_instead_none=False)
                    values_tags = TBUtility.get_values(datatype_config["value"], data, datatype_config["type"],
                                                       get_tag=True)

                    keys = TBUtility.get_values(datatype_config["key"], data, datatype_config["type"],
                                                expression_instead_none=False)
                    keys_tags = TBUtility.get_values(datatype_config["key"], data, get_tag=True)

                    full_key = datatype_config['key']
                    for (key_tag, key) in zip(keys_tags, keys):
                        is_valid_key = "${" in datatype_config["key"] and "}" in \
                                       datatype_config["key"]
                        full_key = full_key.replace('${' + str(key_tag) + '}',
                                                    str(key)) if is_valid_key else key

                    full_value = datatype_config['value']
                    for (value_tag, value) in zip(values_tags, values):
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
                            ts = TBUtility.resolve_different_ts_formats(data=data, config=datatype_config, logger=self._log)
                            telemetry_entry = TelemetryEntry({datapoint_key: full_value}, ts)
                            converted_data.add_to_telemetry(telemetry_entry)
        except Exception as e:
            StatisticsService.count_connector_message(self._log.name, 'convertersMsgDropped')
            self._log.error('Error in converter, for config: \n%s\n and message: \n%s\n %s', dumps(self.__config),
                            str(data), e)
        self._log.info("Converted data: %s", converted_data)

        StatisticsService.count_connector_message(self._log.name, 'convertersAttrProduced',
                                                  count=converted_data.attributes_datapoints_count)
        StatisticsService.count_connector_message(self._log.name, 'convertersTsProduced',
                                                  count=converted_data.telemetry_datapoints_count)

        return converted_data