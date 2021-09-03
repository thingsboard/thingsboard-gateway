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


class JsonMqttUplinkConverter(MqttUplinkConverter):
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
                    log.debug("Regular expression result is None. deviceNameTopicExpression parameter will be interpreted as a deviceName\n Topic: %s\nRegex: %s", config, self.__config.get("deviceNameTopicExpression"))
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
                    log.debug("Regular expression result is None. deviceTypeTopicExpression will be interpreted as a deviceType\n Topic: %s\nRegex: %s", config, self.__config.get("deviceTypeTopicExpression"))
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
                    value = datatype_config['value'] if '${' not in datatype_config['value'] and '}' not in \
                                                        datatype_config['value'] else data[
                        datatype_config['value'][2:-1]]

                    key = datatype_config['key'] if '${' not in datatype_config['key'] and '}' not in \
                                                    datatype_config['key'] else data[
                        datatype_config['key'][2:-1]]
                    if datatype == 'timeseries' and (data.get("ts") is not None or data.get("timestamp") is not None):
                        dict_result[datatypes[datatype]].append({"ts": data.get('ts', data.get('timestamp', int(time()))), 'values': {key: value}})
                    else:
                        dict_result[datatypes[datatype]].append({key: value})
        except Exception as e:
            log.error('Error in converter, for config: \n%s\n and message: \n%s\n', dumps(self.__config), str(data))
            log.exception(e)
        return dict_result
