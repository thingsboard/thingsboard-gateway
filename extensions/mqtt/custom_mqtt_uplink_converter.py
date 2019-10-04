import logging
import re
from json import load, loads, dumps
from connectors.mqtt.mqtt_uplink_converter import MqttUplinkConverter

log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)


class CustomMqttUplinkConverter(MqttUplinkConverter):
    def __init__(self, config):
        self.__config = config.get('converter')
        self.dict_result = {}

    def convert(self, topic, body):
        try:
            self.dict_result["deviceName"] = topic.split("/")[-1]
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
            return self.dict_result

        except Exception as e:
            log.error('Error in converter, for config: \n%s\n and message: \n%s\n', dumps(self.__config), body)
            log.error(e)
