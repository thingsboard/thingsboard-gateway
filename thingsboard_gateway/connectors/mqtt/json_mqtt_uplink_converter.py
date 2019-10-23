#     Copyright 2019. ThingsBoard
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

from json import dumps
from re import search
from thingsboard_gateway.connectors.mqtt.mqtt_uplink_converter import MqttUplinkConverter, log
from thingsboard_gateway.tb_utility.tb_utility import TBUtility


class JsonMqttUplinkConverter(MqttUplinkConverter):
    def __init__(self, config):
        self.__config = config.get('converter')
        self.dict_result = {}

    def convert(self, topic, body):
        try:
            if self.__config.get("deviceNameJsonExpression") is not None:
                self.dict_result["deviceName"] = TBUtility.get_value(self.__config.get("deviceNameJsonExpression"), body)
            elif self.__config.get("deviceNameTopicExpression") is not None:
                self.dict_result["deviceName"] = search(self.__config["deviceNameTopicExpression"], topic)
            else:
                log.error("The expression for looking \"deviceName\" not found in config %s", dumps(self.__config))
            if self.__config.get("deviceTypeJsonExpression") is not None:
                self.dict_result["deviceType"] = TBUtility.get_value(self.__config.get("deviceTypeJsonExpression"), body)
            elif self.__config.get("deviceTypeTopicExpression") is not None:
                self.dict_result["deviceType"] = search(self.__config["deviceTypeTopicExpression"], topic)
            else:
                log.error("The expression for looking \"deviceType\" not found in config %s", dumps(self.__config))
            self.dict_result["attributes"] = []
            self.dict_result["telemetry"] = []
        except Exception as e:
            log.error('Error in converter, for config: \n%s\n and message: \n%s\n', dumps(self.__config), body)
            log.exception(e)
        try:
            if self.__config.get("attributes"):
                for attribute in self.__config.get("attributes"):
                    self.dict_result["attributes"].append({attribute["key"]: TBUtility.get_value(attribute["value"],
                                                                                                 body,
                                                                                                 attribute["type"])})
        except Exception as e:
            log.error('Error in converter, for config: \n%s\n and message: \n%s\n', dumps(self.__config), body)
            log.exception(e)
        try:
            if self.__config.get("timeseries"):
                for ts in self.__config.get("timeseries"):
                    self.dict_result["telemetry"].append({ts["key"]: TBUtility.get_value(ts["value"],
                                                                                         body,
                                                                                         ts["type"])})
        except Exception as e:
            log.error('Error in converter, for config: \n%s\n and message: \n%s\n', dumps(self.__config), body)
            log.exception(e)
        return self.dict_result
