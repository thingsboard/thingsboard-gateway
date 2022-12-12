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

import json
from re import findall
from time import time

from thingsboard_gateway.connectors.xmpp.xmpp_converter import XmppConverter, log
from thingsboard_gateway.tb_utility.tb_utility import TBUtility
from thingsboard_gateway.gateway.statistics_service import StatisticsService


class XmppUplinkConverter(XmppConverter):
    def __init__(self, config):
        self.__config = config
        self._datatypes = {"attributes": "attributes",
                           "timeseries": "telemetry"}

    def _convert_json(self, val):
        dict_result = {}
        try:
            data = json.loads(val)

            device_name_tags = TBUtility.get_values(self.__config.get("deviceNameExpression"), data,
                                                    get_tag=True)
            device_name_values = TBUtility.get_values(self.__config.get("deviceNameExpression"), data,
                                                      expression_instead_none=True)

            dict_result['deviceName'] = self.__config.get("deviceNameExpression")
            for (device_name_tag, device_name_value) in zip(device_name_tags, device_name_values):
                is_valid_key = "${" in self.__config.get("deviceNameExpression") and "}" in \
                               self.__config.get("deviceNameExpression")
                dict_result['deviceName'] = dict_result['deviceName'].replace('${' + str(device_name_tag) + '}',
                                                                              str(device_name_value)) \
                    if is_valid_key else device_name_tag

            device_type_tags = TBUtility.get_values(self.__config.get("deviceTypeExpression"), data,
                                                    get_tag=True)
            device_type_values = TBUtility.get_values(self.__config.get("deviceTypeExpression"), data,
                                                      expression_instead_none=True)

            dict_result["deviceType"] = self.__config.get("deviceTypeExpression")
            for (device_type_tag, device_type_value) in zip(device_type_tags, device_type_values):
                is_valid_key = "${" in self.__config.get("deviceTypeExpression") and "}" in \
                               self.__config.get("deviceTypeExpression")
                dict_result["deviceType"] = dict_result["deviceType"].replace('${' + str(device_type_tag) + '}',
                                                                              str(device_type_value)) \
                    if is_valid_key else device_type_tag

            for datatype in self._datatypes:
                dict_result[self._datatypes[datatype]] = []
                for datatype_config in self.__config.get(datatype, []):
                    values = TBUtility.get_values(datatype_config["value"], data, "json",
                                                  expression_instead_none=False)
                    values_tags = TBUtility.get_values(datatype_config["value"], data, "json",
                                                       get_tag=True)

                    keys = TBUtility.get_values(datatype_config["key"], data, "json",
                                                expression_instead_none=False)
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
                                                        str(value)) if is_valid_value else value

                    if full_key != 'None' and full_value != 'None':
                        if datatype == 'timeseries' and (
                                data.get("ts") is not None or data.get("timestamp") is not None):
                            dict_result[self._datatypes[datatype]].append(
                                {"ts": data.get('ts', data.get('timestamp', int(time()))),
                                 'values': {full_key: full_value}})
                        else:
                            dict_result[self._datatypes[datatype]].append({full_key: full_value})

            return dict_result
        except json.decoder.JSONDecodeError:
            return None

    @staticmethod
    def _get_value(data, config, key):
        expression_arr = findall(r'\[\S[\d:]*]', config[key])
        if expression_arr:
            indexes = expression_arr[0][1:-1].split(':')
            try:
                if len(indexes) == 2:
                    from_index, to_index = indexes
                    return data[int(from_index) if from_index != '' else None:int(to_index) if to_index != '' else None]
                else:
                    index = int(indexes[0])
                    return data[index]
            except IndexError:
                log.error('deviceName expression invalid (index out of range)')
                return None
        else:
            return config[key]

    def _convert_text(self, val):
        dict_result = {'deviceName': self._get_value(val, self.__config, 'deviceNameExpression'),
                       'deviceType': self._get_value(val, self.__config, 'deviceTypeExpression')}

        if dict_result.get('deviceName') and dict_result.get('deviceType'):
            for datatype in self._datatypes:
                dict_result[self._datatypes[datatype]] = []
                for datatype_config in self.__config.get(datatype, []):
                    key = self._get_value(val, datatype_config, 'key')
                    value = self._get_value(val, datatype_config, 'value')

                    if key and value and datatype == 'timeseries':
                        dict_result[self._datatypes[datatype]].append({'ts': int(time()) * 1000, 'values': {key: value}})
                    else:
                        dict_result[self._datatypes[datatype]].append({key: value})

            return dict_result

        return None

    @StatisticsService.CollectStatistics(start_stat_type='receivedBytesFromDevices',
                                         end_stat_type='convertedBytesFromDevice')
    def convert(self, config, val):
        # convert data if it is json format
        result = self._convert_json(val)
        if result:
            return result

        # convert data using slices if it is text format
        result = self._convert_text(val)
        if result:
            return result

        # if none of above
        return None
