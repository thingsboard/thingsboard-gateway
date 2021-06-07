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
from thingsboard_gateway.connectors.mqtt.mqtt_uplink_converter import MqttUplinkConverter, log
import re


class ShellyConverter(MqttUplinkConverter):
    def __init__(self, config):
        self.__config = config.get('converter')
        self.dict_result = {}

    def convert(self, topic, body):
        log.info("DATA: " + str(body) +  ", Topic: " + topic + ", Config:" + dumps(self.__config)) 

        # Search the device name from the topic
        #prog = re.compile(self.__config["deviceNameTopicExpression"])
        topicSplit = topic.split("/")
        deviceName = topicSplit[1]

        deviceConverter = deviceName.split("-")[0]
        try:
            # Shellyht
            if deviceConverter == "shellyht":
                # Get list of only the telemetry we want to post
                #telemetryKeys = self.__config["extension-config"].keys()

                # Get last identifier from topic to be able to compare against telemetryKeys
                _topicTelemetry = topic.split("/")[-1] 

                self.dict_result["telemetry"] = [] # Telemetry template
                self.dict_result["deviceType"] = self.__config["deviceTypeTopicExpression"] # pass from config
                self.dict_result["deviceName"] = deviceName # Get the full string
                
                self.dict_result["telemetry"].append({ _topicTelemetry : str(body) })

                log.info(dumps(self.dict_result))

                return self.dict_result
            
            # Shellyem3 and Shellyem
            elif deviceConverter == "shellyem3" or deviceConverter == "shellyem":

                _phase = topicSplit[3]
                _elType = topicSplit[-1]

                self.dict_result["telemetry"] = []
                self.dict_result["deviceType"] = self.__config["deviceTypeTopicExpression"] 
                self.dict_result["deviceName"] = deviceName

                if "relay" in topicSplit:
                    tel = {("Relay" + str(_phase)) : str(body)}
                    self.dict_result["telemetry"].append(tel)
                    return self.dict_result

                self.dict_result["telemetry"].append({("L" + str(int(_phase) + 1) + "_") + str(_elType)  : str(body) })
                
                log.info(dumps(self.dict_result))

                return self.dict_result

            # Shelly1pm
            elif deviceConverter == "shelly1pm":

                self.dict_result["telemetry"] = []
                self.dict_result["deviceType"] = self.__config["deviceTypeTopicExpression"] 
                self.dict_result["deviceName"] = deviceName

                key = ""

                if "temperature" == topicSplit[-1].split('_')[0]:

                    _key = "deviceTemperature"
                    self.dict_result["telemetry"].append({_key : str(body)})
                    return self.dict_result

                _phase = topicSplit[3]
                _elType = topicSplit[-1]

                if str(topicSplit[-1]) == "0":
                    telemetry = {("Relay" + str(_elType)) : str(body)}
                    self.dict_result["telemetry"].append(telemetry)
                    return self.dict_result
                    
                telemetry = {("L" + str(int(_phase) + 1) + "_") + str(_elType)  : str(body)}

                self.dict_result["telemetry"].append(telemetry)
                return self.dict_result

            # Shelly2.5
            elif deviceConverter == "shellyswitch25":

                self.dict_result["telemetry"] = []
                self.dict_result["deviceType"] = self.__config["deviceTypeTopicExpression"] 
                self.dict_result["deviceName"] = deviceName

                if "temperature" == topicSplit[-1].split('_')[0]:

                    _key = "deviceTemperature"
                    self.dict_result["telemetry"].append({_key : str(body)})
                    return self.dict_result
                
                _phase = topicSplit[3]
                _elType = topicSplit[-1]

                if len(topicSplit[-1]) < 2:
                    telemetry = {("Relay" + str(_elType)) : str(body)}
                    self.dict_result["telemetry"].append(telemetry)
                    return self.dict_result
                    
                telemetry = {("L" + str(int(_phase) + 1) + "_") + str(_elType)  : str(body)}

                self.dict_result["telemetry"].append(telemetry)
                return self.dict_result

        except Exception as e:
            log.exception('Error in converter, for config: \n%s\n and message: \n%s\n', dumps(self.__config), body)
            log.exception(e)
