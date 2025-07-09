#     Copyright 2025. ThingsBoard
#
#     Licensed under the Apache License, Version 2.0 (the "License");
#     you may not use this file except in compliance with the License.
#     You may obtain a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#     Unless required by applicable law or agreed to in writing, software
#     distributed under the License is distributed on an "AS IS" BASIS,
#     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#     See the License for the specific language governing permissions and
#     limitations under the License.

import ast
from urllib.parse import quote

from simplejson import dumps

from thingsboard_gateway.connectors.request.request_converter import RequestConverter
from thingsboard_gateway.gateway.statistics.decorators import CollectStatistics
from thingsboard_gateway.tb_utility.tb_utility import TBUtility


class JsonRequestDownlinkConverter(RequestConverter):
    def __init__(self, config, logger):
        self.__log = logger
        self.__config = config

    @CollectStatistics(start_stat_type='allReceivedBytesFromTB',
                       end_stat_type='allBytesSentToDevices')
    def convert(self, config, data):
        try:
            if data["data"].get("id") is None:
                attribute_key = list(data["data"].keys())[0]
                attribute_value = list(data["data"].values())[0]

                result = {
                    "url": self.__config["requestUrlExpression"].replace("${attributeKey}", quote(attribute_key))
                    .replace("${attributeValue}", quote(str(attribute_value)))
                    .replace("${deviceName}", quote(data["device"])),
                    "data": self.__config["requestValueExpression"].replace("${attributeKey}", attribute_key)
                    .replace("${attributeValue}", str(attribute_value))
                    .replace("${deviceName}", data["device"])
                }
            else:
                request_id = str(data["data"]["id"])
                method_name = data["data"]["method"]

                result = {
                    "url": self.__config["requestUrlExpression"].replace("${requestId}", request_id)
                    .replace("${methodName}", method_name)
                    .replace("${deviceName}", quote(data["device"])),
                    "data": self.__config["requestValueExpression"].replace("${requestId}", request_id)
                    .replace("${methodName}", method_name)
                    .replace("${deviceName}", data["device"])
                }

            result['url'] = TBUtility.replace_params_tags(result['url'], data)

            data_tags = TBUtility.get_values(config.get('requestValueExpression'), data['data'], 'params',
                                             get_tag=True)
            data_values = TBUtility.get_values(config.get('requestValueExpression'), data['data'], 'params',
                                               expression_instead_none=True)

            for (tag, value) in zip(data_tags, data_values):
                result['data'] = result["data"].replace('${' + tag + '}', str(value))

            result["data"] = dumps(ast.literal_eval(result["data"]))
            return result
        except Exception as e:
            self.__log.exception(e)
