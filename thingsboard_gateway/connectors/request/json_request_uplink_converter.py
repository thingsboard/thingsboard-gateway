#      Copyright 2020. ThingsBoard
#
#      Licensed under the Apache License, Version 2.0 (the "License");
#      you may not use this file except in compliance with the License.
#      You may obtain a copy of the License at
#
#          http://www.apache.org/licenses/LICENSE-2.0
#
#      Unless required by applicable law or agreed to in writing, software
#      distributed under the License is distributed on an "AS IS" BASIS,
#      WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#      See the License for the specific language governing permissions and
#      limitations under the License.

from simplejson import dumps, loads
from thingsboard_gateway.connectors.request.request_converter import RequestConverter, log
from thingsboard_gateway.tb_utility.tb_utility import TBUtility


class JsonRequestUplinkConverter(RequestConverter):
    def __init__(self, config):
        self.__config = config.get('converter')

    def convert(self, config, data):
        if type(data) in (bytes, str):
            data = loads(data)
        dict_result = {"deviceName": None, "deviceType": None,"attributes": [], "telemetry": []}
        try:
            if self.__config.get("deviceNameJsonExpression") is not None:
                dict_result["deviceName"] = TBUtility.get_value(self.__config.get("deviceNameJsonExpression"), data, expression_instead_none=True)
            else:
                log.error("The expression for looking \"deviceName\" not found in config %s", dumps(self.__config))
            if self.__config.get("deviceTypeJsonExpression") is not None:
                dict_result["deviceType"] = TBUtility.get_value(self.__config.get("deviceTypeJsonExpression"), data, expression_instead_none=True)
            else:
                log.error("The expression for looking \"deviceType\" not found in config %s", dumps(self.__config))
            dict_result["attributes"] = []
            dict_result["telemetry"] = []
        except Exception as e:
            log.exception(e)
        try:
            if self.__config.get("attributes"):
                for attribute in self.__config.get("attributes"):
                    attribute_value = TBUtility.get_value(attribute["value"], data, attribute["type"])
                    if attribute_value is not None:
                        dict_result["attributes"].append({attribute["key"]: attribute_value})
                    else:
                        log.debug("%s key not found in response: %s", attribute["value"].replace("${", '"').replace("}", '"'), data)
        except Exception as e:
            log.error('Error in converter, for config: \n%s\n and message: \n%s\n', dumps(self.__config), data)
            log.exception(e)
        try:
            if self.__config.get("telemetry"):
                for ts in self.__config.get("telemetry"):
                    ts_value = TBUtility.get_value(ts["value"], data, ts["type"])
                    if ts_value is not None:
                        dict_result["telemetry"].append({ts["key"]: ts_value})
                    else:
                        log.debug("%s key not found in response: %s", ts["value"].replace("${", '"').replace("}", '"'), data)
        except Exception as e:
            log.exception(e)
        return dict_result
