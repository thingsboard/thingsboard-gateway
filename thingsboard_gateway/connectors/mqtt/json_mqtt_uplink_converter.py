#     Copyright 2020. ThingsBoard
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
from re import search
from thingsboard_gateway.connectors.mqtt.mqtt_uplink_converter import MqttUplinkConverter, log
from thingsboard_gateway.tb_utility.tb_utility import TBUtility


class JsonMqttUplinkConverter(MqttUplinkConverter):
    def __init__(self, config):
        self.__config = config.get('converter')

    def convert(self, config, data):
        dict_result = {"deviceName": None, "deviceType": None,"attributes": [], "telemetry": []}
        try:
            if self.__config.get("deviceNameJsonExpression") is not None:
                dict_result["deviceName"] = TBUtility.get_value(self.__config.get("deviceNameJsonExpression"), data)
            elif self.__config.get("deviceNameTopicExpression") is not None:
                search_result = search(self.__config["deviceNameTopicExpression"], config)
                if search_result is not None:
                    dict_result["deviceName"] = search_result.group(0)
                else:
                    log.error("Regular expression result is None, please check the expression.\n Topic: %s\nRegex: %s", self.__config("deviceNameTopicExpression"), config)
            else:
                log.error("The expression for looking \"deviceName\" not found in config %s", dumps(self.__config))
            if self.__config.get("deviceTypeJsonExpression") is not None:
                dict_result["deviceType"] = TBUtility.get_value(self.__config.get("deviceTypeJsonExpression"), data)
            elif self.__config.get("deviceTypeTopicExpression") is not None:
                search_result = search(self.__config("deviceTypeTopicExpression"), config)
                if search_result is not None:
                    dict_result["deviceType"] = search_result.group(0)
                else:
                    log.error("Regular expression result is None, please check the expression.\n Topic: %s\nRegex: %s", self.__config("deviceTypeTopicExpression"), config)
            else:
                log.error("The expression for looking \"deviceType\" not found in config %s", dumps(self.__config))
            dict_result["attributes"] = []
            dict_result["telemetry"] = []
        except Exception as e:
            log.error('Error in converter, for config: \n%s\n and message: \n%s\n', dumps(self.__config), data)
            log.exception(e)
        try:
            if self.__config.get("attributes"):
                for attribute in self.__config.get("attributes"):
                    dict_result["attributes"].append({attribute["key"]: TBUtility.get_value(attribute["value"],
                                                                                                 data,
                                                                                                 attribute["type"])})
        except Exception as e:
            log.error('Error in converter, for config: \n%s\n and message: \n%s\n', dumps(self.__config), data)
            log.exception(e)
        try:
            if self.__config.get("timeseries"):
                for ts in self.__config.get("timeseries"):
                    dict_result["telemetry"].append({ts["key"]: TBUtility.get_value(ts["value"],
                                                                                         data,
                                                                                         ts["type"])})
        except Exception as e:
            log.error('Error in converter, for config: \n%s\n and message: \n%s\n', dumps(self.__config), data)
            log.exception(e)
        return dict_result
