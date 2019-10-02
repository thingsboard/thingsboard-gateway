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
            self.dict_result["deviceName"] = self.__extension["deviceName"] # replace with last token in topic;
            self.dict_result["deviceType"] = self.__extension["deviceType"] # just hardcode this
            # replace this genious code with simple code sample where we parse byte by byte;
            converted = [int(current_byte) for current_byte in bytearray.fromhex(body.replace("0x", ""))]
            self.dict_result["telemetry"] = dict(zip(self.__extension["telemetryKeys"], converted))
        except Exception as e:
            log.error('Error in converter, for config: \n%s\n and message: \n%s\n', dumps(self.__config), body)
            log.error(e)
