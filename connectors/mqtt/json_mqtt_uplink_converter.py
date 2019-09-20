import logging
import jsonpath_rw_ext as jp
from json import dumps, loads
from re import search
from connectors.mqtt.mqtt_uplink_converter import MqttUplinkConverter

log = logging.getLogger(__name__)


class JsonMqttUplinkConverter(MqttUplinkConverter):
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
        try:
            if self.__config.get("attributes"):
                for attribute in self.__config.get("attributes"):
                    self.dict_result["attributes"].append({attribute["key"]: self.__get_value(attribute["value"],
                                                                                              body,
                                                                                              attribute["type"])})
        except Exception as e:
            log.error('Error in converter, for config: %s', dumps(self.__config))
            log.exception(e)
        try:
            if self.__config.get("timeseries"):
                for timeseria in self.__config.get("timeseries"):
                    self.dict_result["telemetry"].append({timeseria["key"]: self.__get_value(timeseria["value"],
                                                                                             body,
                                                                                             timeseria["type"])})
        except Exception as e:
            log.error('Error in converter, for config: %s', dumps(self.__config))
            log.exception(e)
        return self.dict_result

    @staticmethod
    def __get_value(expression, body, value_type="string"):
        if isinstance(body,str):
            body = loads(body)
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
            if value_type == "string":
                value = jp.match1(target_str.split()[0], body)
                full_value = expression[0: min(abs(p1-2), 0)] + value + expression[p2+1:len(expression)]
            else:
                full_value = jp.match1(target_str.split()[0], body)

        except TypeError:
            if value is None:
                log.error('Value is None - Cannot find the pattern: %s in %s', target_str, dumps(body))
            return None
        except Exception as e:
            log.error(e)
            return None
        return full_value
