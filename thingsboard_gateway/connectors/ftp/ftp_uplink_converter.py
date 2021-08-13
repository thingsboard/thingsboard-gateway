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

import re
from simplejson import dumps
from time import time

from thingsboard_gateway.connectors.ftp.ftp_converter import FTPConverter
from thingsboard_gateway.connectors.converter import log
from thingsboard_gateway.tb_utility.tb_utility import TBUtility


class FTPUplinkConverter(FTPConverter):
    def __init__(self, config):
        self.__config = config
        self.__data_types = {"attributes": "attributes", "timeseries": "telemetry"}

    def _get_required_data(self, left_symbol, right_symbol):
        dict_result = {"deviceName": None, "deviceType": None, "attributes": [], "telemetry": []}
        get_device_name_from_data = False
        get_device_type_from_data = False

        if left_symbol in self.__config['devicePatternName'] and right_symbol in self.__config['devicePatternName']:
            get_device_name_from_data = True
        else:
            dict_result['deviceName'] = self.__config['devicePatternName']
        if left_symbol in self.__config['devicePatternType'] and right_symbol in self.__config['devicePatternType']:
            get_device_type_from_data = True
        else:
            dict_result['deviceType'] = self.__config['devicePatternType']

        return dict_result, get_device_name_from_data, get_device_type_from_data

    def _convert_table_view_data(self, config, data):
        dict_result, get_device_name_from_data, get_device_type_from_data = self._get_required_data('${', '}')
        try:
            for data_type in self.__data_types:
                for information in self.__config[data_type]:
                    arr = data.split(self.__config['delimiter'])

                    key_index = information['key']
                    val_index = information['value']

                    if '${' in information['key'] and '}' in information['key']:
                        key_index = config['headers'].index(re.sub(r'[^\w]', '', information['key']))

                    if '${' in information['value'] and '}' in information['value']:
                        val_index = config['headers'].index(re.sub(r'[^\w]', '', information['value']))

                    dict_result[self.__data_types[data_type]].append({
                        arr[key_index] if isinstance(key_index, int) else key_index:
                            arr[val_index] if isinstance(val_index, int) else val_index
                    })

                    if get_device_name_from_data:
                        index = config['headers'].index(re.sub(r'[^\w]', '', self.__config['devicePatternName']))
                        dict_result['deviceName'] = arr[index]
                    if get_device_type_from_data:
                        index = config['headers'].index(re.sub(r'[^\w]', '', self.__config['devicePatternType']))
                        dict_result['deviceType'] = arr[index]

        except Exception as e:
            log.error('Error in converter, for config: \n%s\n and message: \n%s\n', dumps(self.__config), data)
            log.exception(e)

        return dict_result

    @staticmethod
    def _get_key_or_value(key, arr):
        if '[' in key and ']' in key:
            split_val_arr = key[1:-1].split(':')
            first_val_index = split_val_arr[0] or 0
            last_val_index = split_val_arr[1] or len(arr)

            return arr[int(first_val_index):int(last_val_index)][0]
        else:
            return key

    def _convert_slices_view_data(self, data):
        dict_result, get_device_name_from_data, get_device_type_from_data = self._get_required_data('[', ']')
        try:
            for data_type in self.__data_types:
                for information in self.__config[data_type]:
                    arr = data.split(self.__config['delimiter'])

                    val = self._get_key_or_value(information['value'], arr)
                    key = self._get_key_or_value(information['key'], arr)

                    dict_result[self.__data_types[data_type]].append({key: val})

                    if get_device_name_from_data:
                        if self.__config['devicePatternName'] == information['value']:
                            dict_result['deviceName'] = val
                    if get_device_type_from_data:
                        if self.__config['devicePatternType'] == information['value']:
                            dict_result['deviceType'] = val
        except Exception as e:
            log.error('Error in converter, for config: \n%s\n and message: \n%s\n', dumps(self.__config), data)
            log.exception(e)

        return dict_result

    def _convert_json_file(self, data):
        dict_result = {"deviceName": None, "deviceType": None, "attributes": [], "telemetry": []}

        try:
            if self.__config.get("devicePatternName") is not None:
                dict_result["deviceName"] = TBUtility.get_value(self.__config.get("devicePatternName"), data,
                                                                expression_instead_none=True)
            else:
                log.error("The expression for looking \"deviceName\" not found in config %s", dumps(self.__config))
            if self.__config.get("devicePatternType") is not None:
                dict_result["deviceType"] = TBUtility.get_value(self.__config.get("devicePatternType"), data,
                                                                expression_instead_none=True)
        except Exception as e:
            log.error('Error in converter, for config: \n%s\n and message: \n%s\n', dumps(self.__config), data)
            log.exception(e)

        try:
            for datatype in self.__data_types:
                dict_result[self.__data_types[datatype]] = []
                for datatype_config in self.__config.get(datatype, []):
                    value = TBUtility.get_value(datatype_config["value"], data, datatype_config["type"],
                                                expression_instead_none=True)
                    value_tag = TBUtility.get_value(datatype_config["value"], data, datatype_config["type"],
                                                    get_tag=True)
                    key = TBUtility.get_value(datatype_config["key"], data, datatype_config["type"],
                                              expression_instead_none=True)
                    key_tag = TBUtility.get_value(datatype_config["key"], data, get_tag=True)
                    if ("${" not in str(value) and "}" not in str(value)) and (
                            "${" not in str(key) and "}" not in str(key)):
                        is_valid_key = isinstance(key, str) and "${" in datatype_config["key"] and "}" in \
                                       datatype_config["key"]
                        is_valid_value = isinstance(value, str) and "${" in datatype_config["value"] and "}" in \
                                         datatype_config["value"]
                        full_key = datatype_config["key"].replace('${' + str(key_tag) + '}',
                                                                  str(key)) if is_valid_key else key_tag
                        full_value = datatype_config["value"].replace('${' + str(value_tag) + '}',
                                                                      str(value)) if is_valid_value else value
                        if datatype == 'timeseries' and (
                                data.get("ts") is not None or data.get("timestamp") is not None):
                            dict_result[self.__data_types[datatype]].append(
                                {"ts": data.get('ts', data.get('timestamp', int(time()))),
                                 'values': {full_key: full_value}})
                        else:
                            dict_result[self.__data_types[datatype]].append({full_key: full_value})
        except Exception as e:
            log.error('Error in converter, for config: \n%s\n and message: \n%s\n', dumps(self.__config), str(data))
            log.exception(e)

        return dict_result

    def convert(self, config, data):
        if config['file_ext'] == 'csv' or (
                config['file_ext'] == 'txt' and self.__config['txt_file_data_view'] == 'TABLE'):
            return self._convert_table_view_data(config, data)
        elif config['file_ext'] == 'txt' and self.__config['txt_file_data_view'] == 'SLICED':
            return self._convert_slices_view_data(data)
        elif config['file_ext'] == 'json':
            return self._convert_json_file(data)
        else:
            raise Exception('Incorrect file extension or file data view mode')
