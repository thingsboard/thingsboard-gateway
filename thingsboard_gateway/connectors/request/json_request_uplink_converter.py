#     Copyright 2024. ThingsBoard
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

from time import time

from simplejson import dumps, loads

from thingsboard_gateway.connectors.request.request_converter import RequestConverter
from thingsboard_gateway.tb_utility.tb_utility import TBUtility
from thingsboard_gateway.gateway.statistics_service import StatisticsService


class JsonRequestUplinkConverter(RequestConverter):
    def __init__(self, config, logger):
        self.__log = logger
        self.__config = config
        self.__datatypes = {"attributes": "attributes",
                            "telemetry": "telemetry"}

    @StatisticsService.CollectStatistics(start_stat_type='receivedBytesFromDevices',
                                         end_stat_type='convertedBytesFromDevice')
    def convert(self, config, data):
        if isinstance(data, (bytes, str)):
            data = loads(data)

        dict_result = {"deviceName": None, "deviceType": None, "attributes": [], "telemetry": []}

        try:
            if self.__config['converter'].get("deviceNameJsonExpression") is not None:
                device_name_tags = TBUtility.get_values(self.__config['converter'].get("deviceNameJsonExpression"),
                                                        data, get_tag=True)
                device_name_values = TBUtility.get_values(self.__config['converter'].get("deviceNameJsonExpression"),
                                                          data, get_tag=False)
                dict_result["deviceName"] = self.__config['converter'].get("deviceNameJsonExpression")
                for (device_name_tag, device_name_value) in zip(device_name_tags, device_name_values):
                    is_valid_key = "${" in self.__config['converter'].get("deviceNameJsonExpression") and "}" in \
                                   self.__config['converter'].get("deviceNameJsonExpression")
                    dict_result['deviceName'] = dict_result['deviceName'].replace('${' + str(device_name_tag) + '}',
                                                                                  str(device_name_value)) \
                        if is_valid_key else device_name_tag
            else:
                self.__log.error("The expression for looking \"deviceName\" not found in config %s",
                                 dumps(self.__config['converter']))
            if self.__config['converter'].get("deviceTypeJsonExpression") is not None:
                device_type_tags = TBUtility.get_values(self.__config['converter'].get("deviceTypeJsonExpression"),
                                                        data,
                                                        get_tag=True)
                device_type_values = TBUtility.get_values(self.__config['converter'].get("deviceTypeJsonExpression"),
                                                          data,
                                                          expression_instead_none=True)
                dict_result["deviceType"] = self.__config['converter'].get("deviceTypeJsonExpression")
                for (device_type_tag, device_type_value) in zip(device_type_tags, device_type_values):
                    is_valid_key = "${" in self.__config['converter'].get("deviceTypeJsonExpression") and "}" in \
                                   self.__config['converter'].get("deviceTypeJsonExpression")
                    dict_result["deviceType"] = dict_result["deviceType"].replace('${' + str(device_type_tag) + '}',
                                                                                  str(device_type_value)) \
                        if is_valid_key else device_type_tag
            else:
                self.__log.error("The expression for looking \"deviceType\" not found in config %s",
                                 dumps(self.__config['converter']))
        except Exception as e:
            self.__log.exception(e)

        try:
            for datatype in self.__datatypes:
                dict_result[self.__datatypes[datatype]] = []
                for datatype_object_config in self.__config["converter"].get(datatype, []):
                    values = TBUtility.get_values(datatype_object_config["value"], data, datatype_object_config["type"],
                                                  expression_instead_none=True)
                    values_tags = TBUtility.get_values(datatype_object_config["value"], data,
                                                       datatype_object_config["type"],
                                                       get_tag=True)

                    keys = TBUtility.get_values(datatype_object_config["key"], data, datatype_object_config["type"],
                                                expression_instead_none=True)
                    keys_tags = TBUtility.get_values(datatype_object_config["key"], data, get_tag=True)

                    full_key = datatype_object_config["key"]
                    for (key, key_tag) in zip(keys, keys_tags):
                        is_valid_key = "${" in datatype_object_config["key"] and "}" in \
                                       datatype_object_config["key"]
                        full_key = full_key.replace('${' + str(key_tag) + '}',
                                                    str(key)) if is_valid_key else key_tag

                    full_value = datatype_object_config["value"]
                    for (value, value_tag) in zip(values, values_tags):
                        is_valid_value = "${" in datatype_object_config["value"] and "}" in \
                                         datatype_object_config["value"]

                        full_value = full_value.replace('${' + str(value_tag) + '}',
                                                        str(value)) if is_valid_value else str(value)

                    if datatype == 'timeseries' and (
                            data.get("ts") is not None or data.get("timestamp") is not None):
                        dict_result[self.__datatypes[datatype]].append(
                            {"ts": data.get('ts', data.get('timestamp', int(time()))),
                             'values': {full_key: full_value}})
                    else:
                        dict_result[self.__datatypes[datatype]].append({full_key: full_value})

        except Exception as e:
            self.__log.exception(e)

        return dict_result
