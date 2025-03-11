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

from re import search
from time import time

from simplejson import dumps

from thingsboard_gateway.connectors.mqtt.mqtt_uplink_converter import MqttUplinkConverter
from thingsboard_gateway.gateway.constants import REPORT_STRATEGY_PARAMETER, \
    RECEIVED_TS_PARAMETER, CONVERTED_TS_PARAMETER
from thingsboard_gateway.gateway.entities.attributes import Attributes
from thingsboard_gateway.gateway.entities.converted_data import ConvertedData
from thingsboard_gateway.gateway.entities.report_strategy_config import ReportStrategyConfig
from thingsboard_gateway.gateway.entities.telemetry_entry import TelemetryEntry
from thingsboard_gateway.gateway.statistics.decorators import CollectStatistics
from thingsboard_gateway.tb_utility.tb_utility import TBUtility
from thingsboard_gateway.gateway.statistics.statistics_service import StatisticsService


class JsonMqttUplinkConverter(MqttUplinkConverter):
    CONFIGURATION_OPTION_USE_EVAL = "useEval"

    def __init__(self, config, logger):
        self._log = logger
        self.__config = config.get('converter')
        self.__use_eval = self.__config.get(self.CONFIGURATION_OPTION_USE_EVAL, False)

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

        if isinstance(data, list):
            converted_data = []
            for item in data:
                converted_data.append(self._convert_single_item(topic, item))
            self._log.debug(converted_data)
            return converted_data
        else:
            return self._convert_single_item(topic, data)

    def _convert_single_item(self, topic, data):
        datatypes = {"attributes": "attributes",
                     "timeseries": "telemetry"}

        device_name = self.parse_device_name(topic, data, self.__config)

        converted_data = ConvertedData(device_name=device_name,
                                       device_type=self.parse_device_type(topic, data, self.__config),
                                       metadata={RECEIVED_TS_PARAMETER: int(time() * 1000)})
        device_report_strategy = None
        try:
            device_report_strategy = ReportStrategyConfig(self.__config.get(REPORT_STRATEGY_PARAMETER))
        except ValueError as e:
            self._log.trace("Report strategy config is not specified for device %s: %s", device_name, e)

        try:
            for datatype in datatypes:
                timestamp = data.get("ts", data.get("timestamp")) if datatype == 'timeseries' else None
                for datatype_config in self.__config.get(datatype, []):
                    if isinstance(datatype_config, str) and datatype_config == "*":
                        if datatype == "attributes":
                            converted_data.add_to_attributes(Attributes(data))
                        else:
                            telemetry_entry = TelemetryEntry(data, timestamp)
                            converted_data.add_to_telemetry(telemetry_entry)
                    else:
                        values = TBUtility.get_values(datatype_config["value"], data, datatype_config["type"],
                                                      expression_instead_none=False)
                        values_tags = TBUtility.get_values(datatype_config["value"], data, datatype_config["type"],
                                                           get_tag=True)

                        keys = TBUtility.get_values(datatype_config["key"], data, datatype_config["type"],
                                                    expression_instead_none=False)
                        keys_tags = TBUtility.get_values(datatype_config["key"], data, get_tag=True)

                        full_key = datatype_config["key"]
                        for (key, key_tag) in zip(keys, keys_tags):
                            is_valid_key = "${" in datatype_config["key"] and "}" in datatype_config["key"]
                            full_key = full_key.replace('${' + str(key_tag) + '}', str(key)) if is_valid_key else key_tag

                        full_value = datatype_config["value"]
                        for (value, value_tag) in zip(values, values_tags):
                            is_valid_value = "${" in datatype_config["value"] and "}" in datatype_config["value"]
                            full_value = full_value.replace('${' + str(value_tag) + '}', str(value)) if is_valid_value else value

                        if full_key != 'None' and full_value != 'None':
                            converted_key = TBUtility.convert_key_to_datapoint_key(full_key, device_report_strategy, datatype_config, self._log)
                            converted_value = TBUtility.convert_data_type(full_value, datatype_config["type"], self.__use_eval)
                            if datatype == "attributes":
                                converted_data.add_to_attributes(converted_key, converted_value)
                            else:
                                telemetry_entry = TelemetryEntry({converted_key: converted_value}, timestamp)
                                converted_data.add_to_telemetry(telemetry_entry)
        except Exception as e:
            self._log.error('Error in converter, for config: \n%s\n and message: \n%s\n %s', dumps(self.__config),
                            str(data), e)
            StatisticsService.count_connector_message(self._log.name, 'convertersMsgDropped')

        self._log.debug("Converted data: %s", converted_data)

        StatisticsService.count_connector_message(self._log.name, 'convertersAttrProduced',
                                                  count=converted_data.attributes_datapoints_count)
        StatisticsService.count_connector_message(self._log.name, 'convertersTsProduced',
                                                  count=converted_data.telemetry_datapoints_count)
        return converted_data

    @staticmethod
    def create_data_record(key, value, timestamp):
        value_item = {key: value}
        return {"ts": timestamp, 'values': value_item} if timestamp else value_item

    def parse_device_name(self, topic, data, config):
        return self.parse_device_info(
            topic, data, config, "deviceNameExpressionSource", "deviceNameExpression")

    def parse_device_type(self, topic, data, config):
        return self.parse_device_info(
            topic, data, config, "deviceProfileExpressionSource", "deviceProfileExpression")

    def parse_device_info(self, topic, data, config, expression_source, expression):
        result = None
        device_info = config.get('deviceInfo', {})

        expression = device_info.get('deviceNameExpression') if expression == 'deviceNameExpression' \
            else device_info.get('deviceProfileExpression')

        try:
            if device_info.get(expression_source) == 'message' or device_info.get(expression_source) == 'constant':
                result_tags = TBUtility.get_values(expression, data, get_tag=True)
                result_values = TBUtility.get_values(expression, data, expression_instead_none=True)

                result = expression
                for (result_tag, result_value) in zip(result_tags, result_values):
                    is_valid_key = "${" in expression and "}" in expression
                    result = result.replace('${' + str(result_tag) + '}',
                                            str(result_value)) if is_valid_key else result_tag
            elif device_info.get(expression_source) == 'topic':
                search_result = search(expression, topic)
                if search_result is not None:
                    result = search_result.group(0)
                else:
                    self._log.debug(
                        "Regular expression result is None. deviceNameTopicExpression parameter will be interpreted "
                        "as a deviceName\n Topic: %s\nRegex: %s", topic, expression)
                    result = expression
            else:
                self._log.error("The expression for looking \"deviceName\" not found in config %s", dumps(config))
        except Exception as e:
            self._log.error('Error in converter, for config: \n%s\n and message: \n%s\n %s', dumps(config), data, e)
        return result
