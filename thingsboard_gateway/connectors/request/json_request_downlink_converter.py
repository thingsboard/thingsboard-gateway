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

from simplejson import dumps
from thingsboard_gateway.connectors.request.request_converter import RequestConverter, log


class JsonRequestDownlinkConverter(RequestConverter):
    def __init__(self, config):
        self.__config = config

    def convert(self, config, data):
        try:
            if data["data"].get("id") is None:
                attribute_key = list(data["data"].keys())[0]
                attribute_value = list(data["data"].values())[0]

                result = {"url": self.__config["requestUrlExpression"].replace("${attributeKey}", attribute_key)\
                                                                      .replace("${attributeValue}", attribute_value)\
                                                                      .replace("${deviceName}", data["device"]),
                          "data": self.__config["valueExpression"].replace("${attributeKey}", attribute_key)\
                                                                  .replace("${attributeValue}", attribute_value)\
                                                                  .replace("${deviceName}", data["device"])}
            else:
                request_id = str(data["data"]["id"])
                method_name = data["data"]["method"]
                params = dumps(data["data"]["params"]) or str(data["data"]["params"])

                result = {"url": self.__config["requestUrlExpression"].replace("${requestId}", request_id)\
                                                                      .replace("${methodName}", method_name)\
                                                                      .replace("${params}", params)\
                                                                      .replace("${deviceName}", data["device"]),
                          "data": self.__config["valueExpression"].replace("${requestId}", request_id)\
                                                                  .replace("${methodName}", method_name)\
                                                                  .replace("${params}", params)\
                                                                  .replace("${deviceName}", data["device"])}
            return result
        except Exception as e:
            log.exception(e)
