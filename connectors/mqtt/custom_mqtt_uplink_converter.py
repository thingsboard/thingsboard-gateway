import logging
from json import load, loads, dumps
from connectors.mqtt.mqtt_uplink_converter import MqttUplinkConverter
from os import path

log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)


class CustomMqttUplinkConverter(MqttUplinkConverter):
    def __init__(self, config):
        self.__config = config.get('converter')

        extension_file_path = "extensions/mqtt/" + self.__config.get('extension')
        if path.exists(extension_file_path):
            try:
                with open(extension_file_path) as extension_file:
                    self.__extension = load(extension_file)
            except Exception as e:
                log.error("When opening extension file, got the following error: %s", e)
            if self.__extension:
                self.dict_result["deviceName"] = self.__extension["deviceName"]
                self.dict_result["deviceType"] = self.__extension["deviceType"]

            else:
                log.error("Extension file is empty. You need to create the extension file to parse data from device.")
        else:
            log.error("Path for custom converter to the extension file: %s - not found.",
                      extension_file_path)

        self.dict_result= {}

    def convert(self, body):
        try:
            telemetry_keys = self.__extension["telemetryKeys"]
            converted = [int(current_byte) for current_byte in
                         bytearray.fromhex(body.replace("0x", ""))]
            self.dict_result["telemetry"] = dict(zip(telemetry_keys, converted))
        except Exception as e:
            log.error('Error in converter, for config: \n%s\n and message: \n%s\n', dumps(self.__config), body)
            log.error(e)

