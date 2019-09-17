import logging
import jsonpath_rw_ext as jp
from json import dumps
from re import search
from time import time
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
        try:
            self.dict_result = {
                "deviceName": self.__get_value(self.__config.get("deviceNameJsonExpression"), body),
                "deviceType": self.__get_value(self.__config.get("deviceTypeJsonExpression"), body),
                "attributes": [],
                "telemetry": []
            }
        except Exception as e:
            log.error('Error in converter, for config: %s', dumps(self.__config))
            log.exception(e)
            return {}
        try:
            if self.__config.get("attributes"):
                for attribute in self.__config.get("attributes"):
                    self.dict_result["attributes"].append({attribute["key"]: self.__get_value(attribute["value"],
                                                                                                body,
                                                                                                attribute["type"])})
        except Exception as e:
            log.error('Error in converter, for config: %s', dumps(self.__config))
            log.exception(e)
            return {}
        try:
            if self.__config.get("timeseries"):
                for timeseria in self.__config.get("timeseries"):
                    self.dict_result["telemetry"].append({timeseria["key"]: self.__get_value(timeseria["value"],
                                                                                                body,
                                                                                                timeseria["type"])})
        except Exception as e:
            log.error('Error in converter, for config: %s', dumps(self.__config))
            log.exception(e)
            return {}
        # Check that results of conversion are valid.
        # For example: deviceName is not empty, deviceType is not empty.
        # At least one of attributes or telemetry is present.
        #TODO Release as utility

        if not self.dict_result.get("deviceName") or self.dict_result.get("deviceName") is None:
            log.error('deviceName is empty')
            return {}
        if not self.dict_result.get("deviceType") or self.dict_result.get("deviceType") is None:
            log.error('deviceType is empty')
            return {}
        if not self.dict_result["attributes"] and not self.dict_result["telemetry"]:
            log.error('There is no telemetry and attributes')
            return {}
        return self.dict_result

    @staticmethod
    def __get_value(expression, body, type="string"):
        if not expression:
            return ''
        p1 = search(r'\${', expression)
        p2 = search(r'}', expression)
        if p1 is not None and p2 is not None:
            p1 = p1.end()
            p2 = p2.start()
        else:
            p1 = 0
            p2 = len(expression)
        target_str = str(expression[p1:p2])
        value = True
        try:
            if type == "string":
                value = jp.match1(target_str.split()[0], body)
                full_value = expression[0: min(abs(p1-2), 0)] + value + expression[p2+1:len(expression)]
            else:
                full_value = jp.match1(target_str.split()[0], body)

        except TypeError as e:
            if value is None:
                log.error('Value is None - Cannot find the pattern: %s in %s', expression, dumps(body))
            return None
        except Exception as e:
            log.error(e)
            return None
        return full_value
