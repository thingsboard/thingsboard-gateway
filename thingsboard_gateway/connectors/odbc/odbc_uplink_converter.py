#     Copyright 2021. ThingsBoard
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

from thingsboard_gateway.connectors.converter import log
from thingsboard_gateway.connectors.odbc.odbc_converter import OdbcConverter


class OdbcUplinkConverter(OdbcConverter):
    def convert(self, config, data):
        if isinstance(config, str) and config == "*":
            return data

        converted_data = {}
        for config_item in config:
            try:
                if isinstance(config_item, str):
                    converted_data[config_item] = data[config_item]
                elif isinstance(config_item, dict):
                    if "nameExpression" in config_item:
                        name = eval(config_item["nameExpression"], globals(), data)
                    else:
                        name = config_item["name"]

                    if "column" in config_item:
                        converted_data[name] = data[config_item["column"]]
                    elif "value" in config_item:
                        converted_data[name] = eval(config_item["value"], globals(), data)
                    else:
                        log.error("Failed to convert SQL data to TB format: no column/value configuration item")
                else:
                    log.error("Failed to convert SQL data to TB format: unexpected configuration type '%s'",
                              type(config_item))
            except Exception as e:
                log.error("Failed to convert SQL data to TB format: %s", str(e))
        return converted_data

