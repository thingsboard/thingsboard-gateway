#     Copyright 2022. ThingsBoard
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

from simplejson import dumps
from time import time

from thingsboard_gateway.connectors.ocpp.ocpp_converter import OcppConverter, log
from thingsboard_gateway.tb_utility.tb_utility import TBUtility


class OcppUplinkConverter(OcppConverter):
    def __init__(self, config):
        self.__config = config

    def get_device_name(self, config):
        try:
            if self.__config.get("deviceNameExpression") is not None:
                device_name_tags = TBUtility.get_values(self.__config.get("deviceNameExpression"), config,
                                                        get_tag=True)
                device_name_values = TBUtility.get_values(self.__config.get("deviceNameExpression"), config,
                                                          expression_instead_none=True)

                device_name = self.__config.get("deviceNameExpression")
                for (device_name_tag, device_name_value) in zip(device_name_tags, device_name_values):
                    is_valid_key = "${" in self.__config.get("deviceNameExpression") and "}" in \
                                   self.__config.get("deviceNameExpression")
                    device_name = device_name.replace('${' + str(device_name_tag) + '}',
                                                      str(device_name_value)) \
                        if is_valid_key else device_name_tag

                return device_name

            else:
                log.error("The expression for looking \"deviceName\" not found in config %s", dumps(self.__config))

        except Exception as e:
            log.error('Error in converter, for config: \n%s\n and message: \n%s\n', dumps(self.__config), config)
            log.exception(e)

    def get_device_type(self, config):
        try:
            if self.__config.get("deviceTypeExpression") is not None:
                device_type_tags = TBUtility.get_values(self.__config.get("deviceTypeExpression"), config,
                                                        get_tag=True)
                device_type_values = TBUtility.get_values(self.__config.get("deviceTypeExpression"), config,
                                                          expression_instead_none=True)
                device_type = self.__config.get("deviceTypeExpression")

                for (device_type_tag, device_type_value) in zip(device_type_tags, device_type_values):
                    is_valid_key = "${" in self.__config.get("deviceTypeExpression") and "}" in \
                                   self.__config.get("deviceTypeExpression")
                    device_type = device_type.replace('${' + str(device_type_tag) + '}',
                                                      str(device_type_value)) \
                        if is_valid_key else device_type_tag

                return device_type
        except Exception as e:
            log.error('Error in converter, for config: \n%s\n and message: \n%s\n', dumps(self.__config), config)
            log.exception(e)

    def convert(self, config, data):
        datatypes = {"attributes": "attributes",
                     "timeseries": "telemetry"}
        dict_result = {"deviceName": config['deviceName'], "deviceType": config['deviceType'], "attributes": [],
                       "telemetry": []}

        try:
            for datatype in datatypes:
                dict_result[datatypes[datatype]] = []

                for datatype_config in self.__config.get(datatype, []):
                    if config['messageType'] in datatype_config['messageTypeFilter'].split(','):
                        values = TBUtility.get_values(datatype_config["value"], data,
                                                      expression_instead_none=True)
                        values_tags = TBUtility.get_values(datatype_config["value"], data,
                                                           get_tag=True)

                        keys = TBUtility.get_values(datatype_config["key"], data,
                                                    expression_instead_none=True)
                        keys_tags = TBUtility.get_values(datatype_config["key"], data, get_tag=True)

                        full_key = datatype_config["key"]
                        for (key, key_tag) in zip(keys, keys_tags):
                            is_valid_key = "${" in datatype_config["key"] and "}" in \
                                           datatype_config["key"]
                            full_key = full_key.replace('${' + str(key_tag) + '}',
                                                        str(key)) if is_valid_key else key_tag

                        full_value = datatype_config["value"]
                        for (value, value_tag) in zip(values, values_tags):
                            is_valid_value = "${" in datatype_config["value"] and "}" in \
                                             datatype_config["value"]

                            full_value = full_value.replace('${' + str(value_tag) + '}',
                                                            str(value)) if is_valid_value else str(value)

                        if datatype == 'timeseries' and (
                                data.get("ts") is not None or data.get("timestamp") is not None):
                            dict_result[datatypes[datatype]].append(
                                {"ts": data.get('ts', data.get('timestamp', int(time()))),
                                 'values': {full_key: full_value}})
                        else:
                            dict_result[datatypes[datatype]].append({full_key: full_value})
        except Exception as e:
            log.error('Error in converter, for config: \n%s\n and message: \n%s\n', dumps(self.__config), str(data))
            log.exception(e)

        log.debug(dict_result)
        return dict_result
