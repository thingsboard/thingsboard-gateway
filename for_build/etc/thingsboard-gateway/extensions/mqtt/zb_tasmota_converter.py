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

from simplejson import dumps, loads
from thingsboard_gateway.connectors.mqtt.mqtt_uplink_converter import MqttUplinkConverter, log
from thingsboard_gateway.tb_utility.tb_utility import TBUtility
from jsonpath_rw import parse
import re


class ZbTasmotaConverter(MqttUplinkConverter):
    def __init__(self, config):
        self.__config = config.get('converter')
        self.dict_result = {}

    def convert(self, topic, body):
        log.info("DATA: " + str(body) +  ", Topic: " + topic + ", Config:" + dumps(self.__config)) 

        # MAP
        # This map is neccessary in order to have some protocol on what device is what 
        # Every Zigbee Device name should start with single byte integer eg: 0_Kitchen_TempHumi
        # The integer will be stored as string becouse of .split() func
        deviceMap = {
                "0": "ZigbeeButton",
                "1": "ZigbeeTempHumi"
        }
        body = loads(dumps(body)) # We are expecting JSON input

        # get device name and device id and device Data
        # NOTE: this parses the mqtt.json Json Expressions and looks for the name in message
        deviceName = TBUtility.get_value(self.__config["deviceNameJsonExpression"], body)

        deviceData = body[deviceName]
        deviceConverter = deviceName.split("_")[0]
        try:
            self.dict_result["attributes"] = [] # Attributes template
            self.dict_result["telemetry"] = [] # Telemetry template
            # ZigbeeButton
            if deviceConverter == "0":

                self.dict_result["deviceType"] = deviceMap[deviceConverter]
                self.dict_result["deviceName"] = deviceName
                
                # Telemetry
                self.dict_result["telemetry"].append({"linkQuality": str(deviceData["LinkQuality"])})
                if deviceData.get("Power"):
                    self.dict_result["telemetry"].append({"buttonPress": str(deviceData["Power"])})

                # Attributes
                self.dict_result["attributes"].append({"zigbeeShortAddress": str(deviceData["Device"])}) # Always in the message
                if deviceData.get("BatteryPercentage"):
                    self.dict_result["attributes"].append({"batteryPercentage": str(deviceData["BatteryPercentage"])})


                return self.dict_result
            # ZigbeeTempHumi
            if deviceConverter == "1":

                self.dict_result["deviceType"] = deviceMap[deviceConverter]
                self.dict_result["deviceName"] = deviceName
                
                # Telemetry
                self.dict_result["telemetry"].append({"linkQuality": str(deviceData["LinkQuality"])})
                if deviceData.get("Temperature"):
                    self.dict_result["telemetry"].append({"temperature": str(deviceData["Temperature"])})
                if deviceData.get("Humidity"):
                    self.dict_result["telemetry"].append({"humidity": str(deviceData["Humidity"])})
                
                # Attributes
                self.dict_result["attributes"].append({"zigbeeShortAddress": str(deviceData["Device"])})
                if deviceData.get("BatteryPercentage"):
                    self.dict_result["attributes"].append({"batteryPercentage": str(deviceData["BatteryPercentage"])})

                return self.dict_result
            log.debug(dumps(self.dict_result))
            
        except KeyError as ke:
            log.warning("Key was not supplied: %s" % str(ke))

        except Exception as e:
            log.exception('Error in converter, for config: \n%s\n and message: \n%s\n', dumps(self.__config), body)
            log.exception(e)
