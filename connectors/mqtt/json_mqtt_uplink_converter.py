from abc import ABC,abstractmethod
import logging
import jsonpath_rw_ext as jp
from re import search
from connectors.mqtt.mqtt_uplink_converter import MqttUplinkConverter

log = logging.getLogger(__name__)


class JsonMqttUplinkConverter(MqttUplinkConverter):

# Example

# Input:
# Topic: "/temperature-sensors/data"
# Body:
# {
#     "serialNumber": "SensorA",
#     "sensorType": "temperature-sensor"
#     "model": "T1000"
#     "t": 42
# }

# Config:
# {
#     "topicFilter": "/temperature-sensors/+",
#     "converter": {
#         "type": "json",
#         "filterExpression": "",
#         "deviceNameJsonExpression": "${$.serialNumber}",
#         "deviceTypeJsonExpression": "${$.sensorType}",
#         "timeout": 60000,
#         "attributes": [
#             {
#                 "type": "string",
#                 "key": "model",
#                 "value": "${$.model}"
#             }
#         ],
#         "timeseries": [
#             {
#                 "type": "double",
#                 "key": "temperature",
#                 "value": "${$.t}"
#             }
#         ]
#     }
# }

# Result:
# {
#     "deviceName": "SensorA",
#     "deviceType": "temperature-sensor",
#     "attributes": {
#         "model": "T1000",
#     },
#     "telemetry": {
#         "temperature": 42,
#     }
# }

    def __init__(self, config):
        self.__config = config
        self.dict_result = {}

    def convert(self, body):
        self.dict_result = {
            "deviceName": self.__get_value(self.__config["deviceNameJsonExpression"], body),
            "deviceType": self.__get_value(self.__config["deviceTypeJsonExpression"], body),
            "attributes": [],
            "telemetry": []
        }
        for attribute in self.__config.get("attributes"):
            self.dict_result["attributes"].append({attribute["key"]: self.__get_value(attribute["value"],
                                                                                      body,
                                                                                      attribute["type"])})

        for timeseria in self.__config.get("timeseries"):
            self.dict_result["telemetry"].append({timeseria["key"]: self.__get_value(timeseria["value"],
                                                                                     body,
                                                                                     timeseria["type"])})

    @staticmethod
    def __get_value(expression, body, type="string"):
        p1 = search(r'\${', expression)
        p2 = search(r'}', expression)
        if p1 is not None and p2 is not None:
            p1 = p1.end()
            p2 = p2.start()
        else:
            p1 = 0
            p2 = len(expression)
        target_str = str(expression[p1:p2])
        if type == "string":
            value = expression[0: min(abs(p1-2), 0)] + str(jp.match1(target_str.split()[0], body)) + expression[p2+1:len(expression)]
        else:
            value = jp.match1(target_str.split()[0], body)
        log.debug(f"For expression {expression} parse value: {str(value)}")
        return value
