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

from simplejson import dumps
from time import time

from thingsboard_gateway.connectors.ocpp.ocpp_converter import OcppConverter
from thingsboard_gateway.gateway.constants import REPORT_STRATEGY_PARAMETER
from thingsboard_gateway.gateway.entities.converted_data import ConvertedData
from thingsboard_gateway.gateway.entities.report_strategy_config import ReportStrategyConfig
from thingsboard_gateway.gateway.entities.telemetry_entry import TelemetryEntry
from thingsboard_gateway.gateway.statistics.statistics_service import StatisticsService
from thingsboard_gateway.tb_utility.tb_utility import TBUtility


class OcppUplinkConverter(OcppConverter):
    def __init__(self, config, logger):
        self._log = logger
        self.__config = config

    def get_device_name(self, config):
        try:
            if self.__config.get("deviceNameExpression") is not None:
                device_name_tags = TBUtility.get_values(self.__config.get("deviceNameExpression"), config,
                                                        get_tag=True)
                device_name_values = TBUtility.get_values(self.__config.get("deviceNameExpression"), config,
                                                          expression_instead_none=True)

                device_name = self.__config.get("deviceNameExpression")
                for (device_name_tag, device_name_value) in zip(device_name_tags, device_name_values):
                    is_valid_key = "${" in self.__config.get("deviceNameExpression") and "}" in \
                                   self.__config.get("deviceNameExpression")
                    device_name = device_name.replace('${' + str(device_name_tag) + '}',
                                                      str(device_name_value)) \
                        if is_valid_key else device_name_tag

                return device_name

            else:
                self._log.error("The expression for looking \"deviceName\" not found in config %s", dumps(self.__config))

        except Exception as e:
            self._log.error('Error in converter, for config: \n%s\n and message: \n%s\n %s', dumps(self.__config),
                            config, e)

    def get_device_type(self, config):
        try:
            if self.__config.get("deviceTypeExpression") is not None:
                device_type_tags = TBUtility.get_values(self.__config.get("deviceTypeExpression"), config,
                                                        get_tag=True)
                device_type_values = TBUtility.get_values(self.__config.get("deviceTypeExpression"), config,
                                                          expression_instead_none=True)
                device_type = self.__config.get("deviceTypeExpression")

                for (device_type_tag, device_type_value) in zip(device_type_tags, device_type_values):
                    is_valid_key = "${" in self.__config.get("deviceTypeExpression") and "}" in \
                                   self.__config.get("deviceTypeExpression")
                    device_type = device_type.replace('${' + str(device_type_tag) + '}',
                                                      str(device_type_value)) \
                        if is_valid_key else device_type_tag

                return device_type
        except Exception as e:
            self._log.error('Error in converter, for config: \n%s\n and message: \n%s\n %s', dumps(self.__config),
                            config, e)

    def convert(self, config, data):
        datatypes = {"attributes": "attributes",
                     "timeseries": "telemetry"}

        device_name = config['deviceName']
        device_type = config['deviceType']

        converted_data = ConvertedData(device_name=device_name, device_type=device_type)

        device_report_strategy = None
        try:
            device_report_strategy = ReportStrategyConfig(self.__config.get(REPORT_STRATEGY_PARAMETER))
        except ValueError as e:
            self._log.trace("Report strategy config is not specified for device %s: %s", device_name, e)

        try:
            for datatype in datatypes:
                for datatype_config in self.__config.get(datatype, []):
                    if config['messageType'] in datatype_config['messageTypeFilter'].split(','):
                        values = TBUtility.get_values(datatype_config["value"], data,
                                                      expression_instead_none=True)
                        values_tags = TBUtility.get_values(datatype_config["value"], data,
                                                           get_tag=True)

                        keys = TBUtility.get_values(datatype_config["key"], data,
                                                    expression_instead_none=True)
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
                                                            str(value)) if is_valid_value else str(value)

                        datapoint_key = TBUtility.convert_key_to_datapoint_key(full_key, device_report_strategy,
                                                                               datatype_config, self._log)
                        if datatype == 'attributes':
                            converted_data.add_to_attributes(datapoint_key, full_value)
                        else:
                            ts = data.get('ts', data.get('timestamp'))
                            telemetry_entry = TelemetryEntry({datapoint_key: full_value}, ts)
                            converted_data.add_to_telemetry(telemetry_entry)
        except Exception as e:
            StatisticsService.count_connector_message(self._log.name, 'convertersMsgDropped')
            self._log.error('Error in converter, for config: \n%s\n and message: \n%s\n %s', dumps(self.__config),
                            str(data), e)

        self._log.debug('Converted data: %s', converted_data)
        StatisticsService.count_connector_message(self._log.name, 'convertersAttrProduced',
                                                  count=converted_data.attributes_datapoints_count)
        StatisticsService.count_connector_message(self._log.name, 'convertersTsProduced',
                                                  count=converted_data.telemetry_datapoints_count)

        return converted_data
