import logging
import jsonpath_rw_ext as jp
from json import dumps, loads
from re import search
from connectors.mqtt.mqtt_uplink_converter import MqttUplinkConverter
from tb_utility.tb_utility import TBUtility

log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)


class JsonMqttUplinkConverter(MqttUplinkConverter):
    def __init__(self, config):
        self.__config = config.get('converter')
        self.dict_result = {}

    def convert(self, topic, body):
        try:
            # TODO: handle topic expressions: deviceNameTopicExpression and deviceTypeTopicExpression
            self.dict_result = {
                "deviceName": TBUtility.get_value(self.__config.get("deviceNameJsonExpression"), body),
                "deviceType": TBUtility.get_value(self.__config.get("deviceTypeJsonExpression"), body),
                "attributes": [],
                "telemetry": []
            }
        except Exception as e:
            log.error('Error in converter, for config: \n%s\n and message: \n%s\n', dumps(self.__config), body)
            log.exception(e)
        try:
            if self.__config.get("attributes"):
                for attribute in self.__config.get("attributes"):
                    self.dict_result["attributes"].append({attribute["key"]: TBUtility.get_value(attribute["value"],
                                                                                                 body,
                                                                                                 attribute["type"])})
        except Exception as e:
            log.error('Error in converter, for config: \n%s\n and message: \n%s\n', dumps(self.__config), body)
            log.exception(e)
        try:
            if self.__config.get("timeseries"):
                for timeseria in self.__config.get("timeseries"):
                    self.dict_result["telemetry"].append({timeseria["key"]: TBUtility.get_value(timeseria["value"],
                                                                                                body,
                                                                                                timeseria["type"])})
        except Exception as e:
            log.error('Error in converter, for config: \n%s\n and message: \n%s\n', dumps(self.__config), body)
            log.exception(e)
        return self.dict_result
