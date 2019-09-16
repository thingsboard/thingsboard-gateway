from abc import ABC,abstractmethod
import logging
import jsonpath_rw_ext as jp
from re import match
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
        self.dict_patterns = {}


    def convert(self, body):
        self.dict_result = {
            "deviceName": jp.match1(self.__config["deviceNameJsonExpression"], body),
            "deviceType": jp.match1(self.__config["deviceTypeJsonExpression"], body),
            "attributes":[]
        }
        for attribute in self.__config.get("attributes"):
            log.debug(attribute)
            self.dict_result["attributes"].append({attribute["key"]: jp.match1(attribute["value"], body)})
        log.debug(self.dict_result)


