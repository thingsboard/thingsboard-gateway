import logging
from os import path
from json import load, loads, dumps
from connectors.mqtt.mqtt_uplink_converter import MqttUplinkConverter

log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)


class CustomMqttUplinkConverter(MqttUplinkConverter):
    def __init__(self, config):
        self.__config = config.get('converter')
        self.dict_result = {}

        extension_file_path = "./extensions/mqtt/" + self.__config.get('extension')+".json"
        if path.exists(extension_file_path):
            try:
                with open(extension_file_path) as extension_file:
                    self.__extension = load(extension_file)
            except Exception as e:
                log.error("When opening extension file, got the following error: %s", e)
            if not self.__extension:
                log.error("Extension file is empty. You need to create the extension file to parse data from device.")
            if self.__extension.get("deviceNameTopicExpression") is None:
                log.error("deviceNameTopicExpression in extension\n%s\n not found with config: \n\n%s",
                          loads(self.__extension),
                          loads(self.config))
        else:
            log.error("Path for custom converter to the extension file: %s - not found.",
                      extension_file_path)

    def convert(self, topic, body):
        try:
            self.dict_result["deviceName"] = "ExampleDevice"  # replace with last token in topic;
            self.dict_result["deviceType"] = "Thermostat"  # just hardcode this
            self.dict_result["telemetry"] = []
            bytes_to_read = body.replace("0x", "")
            converted_bytes = bytearray.fromhex(bytes_to_read)
            if self.__config.get("extension-config") is not None:
                for telemetry_key in self.__config["extension-config"]:
                    value = 0
                    for current_byte_position in range(self.__config["extension-config"][telemetry_key]):
                        value = value*256 + converted_bytes.pop(0)
                    telemetry_to_send = {telemetry_key.replace("Bytes", ""): value}
                    self.dict_result["telemetry"].append(telemetry_to_send)
            else:
                self.dict_result["telemetry"] = {"data": int(body, 0)}

        except Exception as e:
            log.error('Error in converter, for config: \n%s\n and message: \n%s\n', dumps(self.__config), body)
            log.error(e)
