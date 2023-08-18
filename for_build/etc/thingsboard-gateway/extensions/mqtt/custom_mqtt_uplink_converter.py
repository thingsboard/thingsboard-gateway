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

from simplejson import dumps

from thingsboard_gateway.connectors.mqtt.mqtt_uplink_converter import MqttUplinkConverter, log


class CustomMqttUplinkConverter(MqttUplinkConverter):
    def __init__(self, config):
        self.__config = config.get('converter')

    @property
    def config(self):
        return self.__config

    def convert(self, topic, body):
        try:
            dict_result = {}
            dict_result["deviceName"] = topic.split("/")[
                -1]  # getting all data after last '/' symbol in this case: if topic = 'devices/temperature/sensor1' device name will be 'sensor1'.
            dict_result["deviceType"] = "Thermostat"  # just hardcode this
            dict_result["telemetry"] = []  # template for telemetry array
            bytes_to_read = body.replace("0x", "")  # Replacing the 0x (if '0x' in body), needs for converting to bytearray
            converted_bytes = bytearray.fromhex(bytes_to_read)  # Converting incoming data to bytearray
            if self.__config.get("extension-config") is not None:
                for telemetry_key in self.__config["extension-config"]:  # Processing every telemetry key in config for extension
                    value = 0
                    for _ in range(self.__config["extension-config"][telemetry_key]):  # reading every value with value length from config
                        value = value * 256 + converted_bytes.pop(0)  # process and remove byte from processing
                    telemetry_to_send = {telemetry_key.replace("Bytes", ""): value}  # creating telemetry data for sending into Thingsboard
                    dict_result["telemetry"].append(telemetry_to_send)  # adding data to telemetry array
            else:
                dict_result["telemetry"] = {"data": int(body, 0)}  # if no specific configuration in config file - just send data which received
            return dict_result

        except Exception as e:
            log.exception('Error in converter, for config: \n%s\n and message: \n%s\n', dumps(self.__config), body)
            log.exception(e)
