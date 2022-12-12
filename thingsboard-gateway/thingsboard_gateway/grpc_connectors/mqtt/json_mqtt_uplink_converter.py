#     Copyright 2022. ThingsBoard
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


class JsonGrpcMqttUplinkConverter(MqttUplinkConverter):
    def __init__(self, config):
        self.__config = config.get('converter')

    def convert(self, config, data):
        datatypes = {"attributes": "attributes",
                     "timeseries": "telemetry"}
        dict_result = {"deviceName": None, "deviceType": None, "attributes": {}, "telemetry": []}

        try:
            if self.__config.get("deviceNameJsonExpression") is not None:
                device_name_tags = TBUtility.get_values(self.__config.get("deviceNameJsonExpression"), data,
                                                        get_tag=True)
                device_name_values = TBUtility.get_values(self.__config.get("deviceNameJsonExpression"), data,
                                                          expression_instead_none=True)

                dict_result['deviceName'] = self.__config.get("deviceNameJsonExpression")
                for (device_name_tag, device_name_value) in zip(device_name_tags, device_name_values):
                    is_valid_key = "${" in self.__config.get("deviceNameJsonExpression") and "}" in \
                                   self.__config.get("deviceNameJsonExpression")
                    dict_result['deviceName'] = dict_result['deviceName'].replace('${' + str(device_name_tag) + '}',
                                                                                  str(device_name_value)) \
                        if is_valid_key else device_name_tag

            elif self.__config.get("deviceNameTopicExpression") is not None:
                search_result = search(self.__config["deviceNameTopicExpression"], config)
                if search_result is not None:
                    dict_result["deviceName"] = search_result.group(0)
                else:
                    log.debug(
                        "Regular expression result is None. deviceNameTopicExpression parameter will be interpreted "
                        "as a deviceName\n Topic: %s\nRegex: %s",
                        config, self.__config.get("deviceNameTopicExpression"))
                    dict_result["deviceName"] = self.__config.get("deviceNameTopicExpression")
            else:
                log.error("The expression for looking \"deviceName\" not found in config %s", dumps(self.__config))

            if self.__config.get("deviceTypeJsonExpression") is not None:
                device_type_tags = TBUtility.get_values(self.__config.get("deviceTypeJsonExpression"), data,
                                                        get_tag=True)
                device_type_values = TBUtility.get_values(self.__config.get("deviceTypeJsonExpression"), data,
                                                          expression_instead_none=True)

                dict_result["deviceType"] = self.__config.get("deviceTypeJsonExpression")
                for (device_type_tag, device_type_value) in zip(device_type_tags, device_type_values):
                    is_valid_key = "${" in self.__config.get("deviceTypeJsonExpression") and "}" in \
                                   self.__config.get("deviceTypeJsonExpression")
                    dict_result["deviceType"] = dict_result["deviceType"].replace('${' + str(device_type_tag) + '}',
                                                                                  str(device_type_value)) \
                        if is_valid_key else device_type_tag

            elif self.__config.get("deviceTypeTopicExpression") is not None:
                search_result = search(self.__config["deviceTypeTopicExpression"], config)
                if search_result is not None:
                    dict_result["deviceType"] = search_result.group(0)
                else:
                    log.debug(
                        "Regular expression result is None. deviceTypeTopicExpression will be interpreted as "
                        "a deviceType\n Topic: %s\nRegex: %s",
                        config,
                        self.__config.get("deviceTypeTopicExpression"))
                    dict_result["deviceType"] = self.__config.get("deviceTypeTopicExpression")
            else:
                log.error("The expression for looking \"deviceType\" not found in config %s", dumps(self.__config))
        except Exception as e:
            log.error('Error in converter, for config: \n%s\n and message: \n%s\n', dumps(self.__config), data)
            log.exception(e)

        try:
            for datatype in datatypes:
                # dict_result[datatypes[datatype]] = {}
                for datatype_config in self.__config.get(datatype, []):
                    values = TBUtility.get_values(datatype_config["value"], data, datatype_config["type"],
                                                  expression_instead_none=False)
                    values_tags = TBUtility.get_values(datatype_config["value"], data, datatype_config["type"],
                                                       get_tag=True)

                    keys = TBUtility.get_values(datatype_config["key"], data, datatype_config["type"],
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
                        if datatype == 'timeseries' and (
                                data.get("ts") is not None or data.get("timestamp") is not None):
                            dict_result[datatypes[datatype]].append(
                                {"ts": data.get('ts', data.get('timestamp', int(time()))),
                                 'values': {full_key: full_value}})
                        else:
                            if isinstance(dict_result[datatypes[datatype]], list):
                                dict_result[datatypes[datatype]] = {}
                            dict_result[datatypes[datatype]][full_key] = full_value
        except Exception as e:
            log.error('Error in converter, for config: \n%s\n and message: \n%s\n', dumps(self.__config), str(data))
            log.exception(e)
        return dict_result
