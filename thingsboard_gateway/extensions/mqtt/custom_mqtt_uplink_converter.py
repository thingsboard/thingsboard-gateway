# ------------------------------------------------------------------------------
#      Copyright 2025. ThingsBoard
#  #
#      Licensed under the Apache License, Version 2.0 (the "License");
#      you may not use this file except in compliance with the License.
#      You may obtain a copy of the License at
#  #
#          http://www.apache.org/licenses/LICENSE-2.0
#  #
#      Unless required by applicable law or agreed to in writing, software
#      distributed under the License is distributed on an "AS IS" BASIS,
#      WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#      See the License for the specific language governing permissions and
#      limitations under the License.
#
# ------------------------------------------------------------------------------


from simplejson import dumps

from thingsboard_gateway.connectors.mqtt.mqtt_uplink_converter import MqttUplinkConverter
from thingsboard_gateway.gateway.entities.converted_data import ConvertedData
from thingsboard_gateway.gateway.entities.telemetry_entry import TelemetryEntry
from thingsboard_gateway.tb_utility.tb_utility import TBUtility


class CustomMqttUplinkConverter(MqttUplinkConverter):
    def __init__(self, config, logger):
        self._log = logger
        self.__config = config.get('converter')

    @property
    def config(self):
        return self.__config

    def convert(self, topic, body):
        try:
            converted_data = ConvertedData(device_name=topic.split("/")[-1],   # getting all data after last '/' symbol in this case: if topic = 'devices/temperature/sensor1' device name will be 'sensor1'.
                                           device_type="Thermostat")  # Device profile name for devices
            bytes_to_read = body.replace("0x", "")  # Replacing the 0x (if '0x' in body), needs for converting to bytearray
            converted_bytes = bytearray.fromhex(bytes_to_read)  # Converting incoming data to bytearray
            extension_config_key = "extensionConfig" if self.__config.get("extensionConfig") is not None else "extension-config"
            if self.__config.get(extension_config_key) is not None:
                for telemetry_key in self.__config[extension_config_key]:  # Processing every telemetry key in config for extension
                    value = 0
                    for _ in range(self.__config[extension_config_key][telemetry_key]):  # reading every value with value length from config
                        value = value * 256 + converted_bytes.pop(0)  # process and remove byte from processing
                    datapoint_key = TBUtility.convert_key_to_datapoint_key(telemetry_key.replace("Bytes", ""), None, {}, self._log)
                    TelemetryEntry({datapoint_key: value})  # creating telemetry entry
                    converted_data.add_to_telemetry(datapoint_key)  # adding telemetry entry to telemetry array for sending data to platform
            else:
                datapoint_key = TBUtility.convert_key_to_datapoint_key("data", None, {}, self._log)
                telemetry_entry = TelemetryEntry({datapoint_key: int(body, 0)})
                converted_data.add_to_telemetry(telemetry_entry)  # if no specific configuration in config file - just send data which received
            return converted_data

        except Exception as e:
            self._log.exception('Error in converter, for config: \n%s\n and message: \n%s\n', dumps(self.__config), body)
            self._log.exception(e)
