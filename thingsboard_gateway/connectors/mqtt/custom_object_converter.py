#     Copyright 2021. ThingsBoard
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

from thingsboard_gateway.connectors.mqtt.mqtt_uplink_converter import MqttUplinkConverter, log
from thingsboard_gateway.tb_utility.tb_utility import TBUtility


class CustomObjectConverter(MqttUplinkConverter):
    def __init__(self, config):
        self.__config = config.get('converter')

    def convert(self, config, data):
        datatypes = {"attributes": "attributes",
                     "timeseries": "telemetry"}
        dict_result = {"deviceName": None, "deviceType": None, "attributes": [], "telemetry": []}
        try:
            if self.__config.get("deviceNameJsonExpression") is not None:
                dict_result["deviceName"] = TBUtility.get_value(self.__config.get("deviceNameJsonExpression"), data, expression_instead_none=True)
            elif self.__config.get("deviceNameTopicExpression") is not None:
                search_result = search(self.__config["deviceNameTopicExpression"], config)
                if search_result is not None:
                    dict_result["deviceName"] = search_result.group(0)
                else:
                    log.debug(
                        "Regular expression result is None. deviceNameTopicExpression parameter will be interpreted as a deviceName\n Topic: %s\nRegex: %s",
                        config, self.__config.get("deviceNameTopicExpression"))
                    dict_result["deviceName"] = self.__config.get("deviceNameTopicExpression")
            else:
                log.error("The expression for looking \"deviceName\" not found in config %s", dumps(self.__config))
            if self.__config.get("deviceTypeJsonExpression") is not None:
                dict_result["deviceType"] = TBUtility.get_value(self.__config.get("deviceTypeJsonExpression"), data, expression_instead_none=True)
            elif self.__config.get("deviceTypeTopicExpression") is not None:
                search_result = search(self.__config["deviceTypeTopicExpression"], config)
                if search_result is not None:
                    dict_result["deviceType"] = search_result.group(0)
                else:
                    log.debug("Regular expression result is None. deviceTypeTopicExpression will be interpreted as a deviceType\n Topic: %s\nRegex: %s", config,
                              self.__config.get("deviceTypeTopicExpression"))
                    dict_result["deviceType"] = self.__config.get("deviceTypeTopicExpression")
            else:
                log.error("The expression for looking \"deviceType\" not found in config %s", dumps(self.__config))
        except Exception as e:
            log.error('Error in converter, for config: \n%s\n and message: \n%s\n', dumps(self.__config), data)
            log.exception(e)
        try:
            for datatype in datatypes:
                dict_result[datatypes[datatype]] = []
                for datatype_config in self.__config.get(datatype, []):
                    full_key = self.__parse_single_value(datatype_config["key"], data, "string", is_key=True)
                    if datatype_config["type"] == "json":
                        full_value = self.__parse_object_value(datatype_config["value"], data)
                    else:
                        full_value = self.__parse_single_value(datatype_config["value"], data, datatype_config["type"])
                    if datatype == 'timeseries' and (data.get("ts") is not None or data.get("timestamp") is not None):
                        dict_result[datatypes[datatype]].append(
                            {"ts": data.get('ts', data.get('timestamp', int(time()))), 'values': {full_key: full_value}})
                    else:
                        dict_result[datatypes[datatype]].append({full_key: full_value})
        except Exception as e:
            log.error('Error in converter, for config: \n%s\n and message: \n%s\n', dumps(self.__config), str(data))
            log.exception(e)
        return dict_result

    def __parse_single_value(self, expression, data, result_type, is_key = False):
        value = TBUtility.get_value(expression, data, result_type, expression_instead_none=True)
        value_tag = TBUtility.get_value(expression, data, result_type, get_tag=True)
        if "${" not in str(value) and "}" not in str(value):
            is_valid_value = isinstance(value, str) and "${" in expression and "}" in expression
            fallback_value = value_tag if is_key else value
            full_value = expression.replace('${' + str(value_tag) + '}', str(value)) if is_valid_value else fallback_value
            return full_value
        return None

    def __parse_object_value(self, expression, data):
        if not isinstance(expression, dict):
            raise RuntimeError(f"Cannot parse non-dict to an object. Got {type(expression)}")
        result_object = {}
        for (k, v) in expression.items():
            key = self.__parse_single_value(k, data, "string", is_key=True)
            value = self.__parse_object_value(v, data) if isinstance(v, dict) else self.__parse_single_value(v, data, "same")
            result_object[key] = value
        return result_object
