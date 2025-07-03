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

import re

from simplejson import dumps

from thingsboard_gateway.connectors.ftp.ftp_converter import FTPConverter
from thingsboard_gateway.gateway.constants import REPORT_STRATEGY_PARAMETER
from thingsboard_gateway.gateway.entities.converted_data import ConvertedData
from thingsboard_gateway.gateway.entities.report_strategy_config import ReportStrategyConfig
from thingsboard_gateway.gateway.entities.telemetry_entry import TelemetryEntry
from thingsboard_gateway.gateway.statistics.decorators import CollectStatistics
from thingsboard_gateway.gateway.statistics.statistics_service import StatisticsService
from thingsboard_gateway.tb_utility.tb_utility import TBUtility


class FTPUplinkConverter(FTPConverter):
    def __init__(self, config, logger):
        self._log = logger
        self.__config = config
        self.__data_types = {"attributes": "attributes", "timeseries": "telemetry"}

    def _get_device_report_strategy(self, device_name):
        device_report_strategy = None
        try:
            device_report_strategy = ReportStrategyConfig(self.__config.get(REPORT_STRATEGY_PARAMETER))
        except ValueError as e:
            self._log.trace("Report strategy config is not specified for device %s: %s", device_name, e)

        return device_report_strategy

    def _get_required_data(self, left_symbol, right_symbol):
        device_name = None
        device_type = None
        get_device_name_from_data = False
        get_device_type_from_data = False

        if left_symbol in self.__config['devicePatternName'] and right_symbol in self.__config['devicePatternName']:
            get_device_name_from_data = True
        else:
            device_name = self.__config['devicePatternName']
        if left_symbol in self.__config['devicePatternType'] and right_symbol in self.__config['devicePatternType']:
            get_device_type_from_data = True
        else:
            device_type = self.__config['devicePatternType']

        return device_name, device_type, get_device_name_from_data, get_device_type_from_data

    def _convert_table_view_data(self, config, data):
        device_name, device_type, get_device_name_from_data, get_device_type_from_data = self._get_required_data('${',
                                                                                                                 '}')

        device_report_strategy = self._get_device_report_strategy(device_name)

        converted_data = ConvertedData(device_name=device_name,
                                        device_type=device_type)

        try:
            arr = data.split(self.__config.get('delimiter',';'))
            for data_type in self.__data_types:
                ts = None
                old_ts = ts
                if data_type == 'timeseries':
                    ts = self._retrieve_ts_for_sliced_or_table(self.__config[data_type], arr, config['headers'])
                for information in self.__config[data_type]:
                    try:

                        key_index = information['key']
                        val_index = information['value']

                        if '${' in information['key'] and '}' in information['key']:
                            key_index = config['headers'].index(re.sub(r'[^\w]', '', information['key']))

                        if '${' in information['value'] and '}' in information['value']:
                            val_index = config['headers'].index(re.sub(r'[^\w]', '', information['value']))

                        key = arr[key_index] if isinstance(key_index, int) else key_index
                        value = arr[val_index] if isinstance(val_index, int) else val_index
                        if key == 'ts' and data_type == 'timeseries':
                            continue
                        datapoint_key = TBUtility.convert_key_to_datapoint_key(key, device_report_strategy,
                                                                               information, self._log)

                        if data_type == 'attributes':
                            converted_data.add_to_attributes(datapoint_key, value)
                        else:
                            if information.get('tsField'):
                                old_ts = ts
                                try:
                                    data_to_retrieve_ts = {}
                                    for index, item in enumerate(config['headers']):
                                       data_to_retrieve_ts[item] = arr[index]
                                    ts = TBUtility.resolve_different_ts_formats(data=data_to_retrieve_ts, config=information, logger=self._log)
                                except Exception as e:
                                    self._log.error('Error while retrieving timestamp for key %s: %s', key, e)
                                    ts = old_ts
                            else:
                                ts = old_ts
                            telemetry_entry = TelemetryEntry({datapoint_key: value}, ts)
                            converted_data.add_to_telemetry(telemetry_entry)

                        if get_device_name_from_data:
                            index = config['headers'].index(re.sub(r'[^\w]', '', self.__config['devicePatternName']))
                            converted_data.device_name = arr[index]
                        if get_device_type_from_data:
                            index = config['headers'].index(re.sub(r'[^\w]', '', self.__config['devicePatternType']))
                            converted_data.device_type = arr[index]
                    except Exception as e:
                        StatisticsService.count_connector_message(self._log.name, 'convertersMsgDropped')
                        self._log.error('Error in converter, for config: \n%s\n and message: \n%s\n %s', dumps(information),
                                        data, e)

        except Exception as e:
            StatisticsService.count_connector_message(self._log.name, 'convertersMsgDropped')
            self._log.error('Error in converter, for config: \n%s\n and message: \n%s\n %s', dumps(self.__config), data,
                            e)

        StatisticsService.count_connector_message(self._log.name, 'convertersAttrProduced',
                                                  count=converted_data.attributes_datapoints_count)
        StatisticsService.count_connector_message(self._log.name, 'convertersTsProduced',
                                                  count=converted_data.telemetry_datapoints_count)
        return converted_data

    @staticmethod
    def _get_key_or_value(key, arr):
        if '[' in key and ']' in key:
            split_val_arr = key[1:-1].split(':')
            first_val_index = split_val_arr[0] or 0
            last_val_index = split_val_arr[1] or len(arr)

            return arr[0][int(first_val_index):int(last_val_index)]
        else:
            return key

    def _convert_slices_view_data(self, data):
        device_name, device_type, get_device_name_from_data, get_device_type_from_data = self._get_required_data('[',
                                                                                                                 ']')

        device_report_strategy = self._get_device_report_strategy(device_name)

        converted_data = ConvertedData(device_name=device_name,
                                       device_type=device_type)
        arr = data.split(self.__config.get('delimiter', ','))

        try:
            for data_type in self.__data_types:
                ts = None
                if data_type == 'timeseries':
                    ts = self._retrieve_ts_for_sliced_or_table(self.__config[data_type], arr)
                for information in self.__config[data_type]:

                    try:

                        val = self._get_key_or_value(information['value'], arr)
                        key = self._get_key_or_value(information['key'], arr)
                        if key == 'ts' and data_type == 'timeseries':
                            continue

                        datapoint_key = TBUtility.convert_key_to_datapoint_key(key, device_report_strategy,
                                                                               information, self._log)
                        if data_type == 'attributes':
                            converted_data.add_to_attributes(datapoint_key, val)
                        else:
                            telemetry_entry = TelemetryEntry({datapoint_key: val}, ts)
                            converted_data.add_to_telemetry(telemetry_entry)

                        if get_device_name_from_data:
                            if self.__config['devicePatternName'] == information['value']:
                                converted_data.device_name = val
                        if get_device_type_from_data:
                            if self.__config['devicePatternType'] == information['value']:
                                converted_data.device_type = val
                    except Exception as e:
                        StatisticsService.count_connector_message(self._log.name, 'convertersMsgDropped')
                        self._log.error('Error in converter, for config: \n%s\n and message: \n%s\n %s', dumps(information),
                                        data, e)
        except Exception as e:
            StatisticsService.count_connector_message(self._log.name, 'convertersMsgDropped')
            self._log.error('Error in converter, for config: \n%s\n and message: \n%s\n %s', dumps(self.__config), data,
                            e)

        StatisticsService.count_connector_message(self._log.name, 'convertersAttrProduced',
                                                  count=converted_data.attributes_datapoints_count)
        StatisticsService.count_connector_message(self._log.name, 'convertersTsProduced',
                                                  count=converted_data.telemetry_datapoints_count)
        return converted_data

    def _retrieve_ts_for_sliced_or_table(self, config, data, headers=[]):
        for config_object in config:
            if config_object.get('key') == 'ts' or config_object.get('key') == 'timestamp':
                value = config_object['value']
                if '${' in config_object.get('value') and '}' in config_object.get('value') and headers:
                    value = headers.index(re.sub(r'[^\w]', '', config_object['value']))
                    return int(data[value])
                return self._get_key_or_value(value, data)
        return None

    def _get_device_name(self, data):
        device_name = ''

        try:
            if self.__config.get("devicePatternName") is not None:
                device_name_tags = TBUtility.get_values(self.__config.get("devicePatternName"), data,
                                                        get_tag=True)
                device_name_values = TBUtility.get_values(self.__config.get("devicePatternName"), data,
                                                          expression_instead_none=True)

                device_name = self.__config.get("devicePatternName")
                for (device_name_tag, device_name_value) in zip(device_name_tags, device_name_values):
                    is_valid_key = "${" in self.__config.get("devicePatternName") and "}" in \
                                   self.__config.get("devicePatternName")
                    device_name = device_name.replace('${' + str(device_name_tag) + '}',
                                                                                  str(device_name_value)) \
                        if is_valid_key else device_name_tag
        except Exception as e:
            StatisticsService.count_connector_message(self._log.name, 'convertersMsgDropped')
            self._log.error('Error in converter, for config: \n%s\n and message: \n%s\n %s', dumps(self.__config), data,
                            e)

        return device_name

    def _get_device_type(self, data):
        device_type = ''

        try:
            if self.__config.get("devicePatternType") is not None:
                device_type_tags = TBUtility.get_values(self.__config.get("devicePatternType"), data,
                                                        get_tag=True)
                device_type_values = TBUtility.get_values(self.__config.get("devicePatternType"), data,
                                                          expression_instead_none=True)
                device_type = self.__config.get("devicePatternType")

                for (device_type_tag, device_type_value) in zip(device_type_tags, device_type_values):
                    is_valid_key = "${" in self.__config.get("devicePatternType") and "}" in \
                                   self.__config.get("devicePatternType")
                    device_type = device_type.replace('${' + str(device_type_tag) + '}',
                                                                                  str(device_type_value)) \
                        if is_valid_key else device_type_tag
        except Exception as e:
            StatisticsService.count_connector_message(self._log.name, 'convertersMsgDropped')
            self._log.error('Error in converter, for config: \n%s\n and message: \n%s\n %s', dumps(self.__config), data,
                            e)

        return device_type

    def _convert_json_file(self, data):
        device_name = self._get_device_name(data)
        if not device_name:
            self._log.error("The expression for looking \"deviceName\" not found in config %s",
                            dumps(self.__config))

        device_type = self._get_device_type(data)
        if not device_type:
            self._log.error("The expression for looking \"deviceType\" not found in config %s",
                            dumps(self.__config))

        device_report_strategy = self._get_device_report_strategy(device_name)

        converted_data = ConvertedData(device_name=device_name,
                                       device_type=device_type)

        try:
            for datatype in self.__data_types:
                for datatype_config in self.__config.get(datatype, []):
                    try:
                        values = TBUtility.get_values(datatype_config["value"], data, datatype_config["type"],
                                                      expression_instead_none=True)
                        values_tags = TBUtility.get_values(datatype_config["value"], data, datatype_config["type"],
                                                           get_tag=True)

                        keys = TBUtility.get_values(datatype_config["key"], data, datatype_config["type"],
                                                    expression_instead_none=True)
                        keys_tags = TBUtility.get_values(datatype_config["key"], data, get_tag=True)

                        full_key = datatype_config["key"]
                        for (key, key_tag) in zip(keys, keys_tags):
                            is_valid_key = "${" in datatype_config["key"] and "}" in datatype_config["key"]
                            full_key = full_key.replace('${' + str(key_tag) + '}',
                                                        str(key)) if is_valid_key else key_tag
                        if full_key == 'ts' and datatype == 'timeseries':
                            continue

                        full_value = datatype_config["value"]
                        for (value, value_tag) in zip(values, values_tags):
                            is_valid_value = "${" in datatype_config["value"] and "}" in datatype_config["value"]

                            full_value = full_value.replace('${' + str(value_tag) + '}',
                                                            str(value)) if is_valid_value else str(value)
                        datapoint_key = TBUtility.convert_key_to_datapoint_key(full_key, device_report_strategy,
                                                                               datatype_config, self._log)
                        if datatype == 'timeseries':
                            ts = None
                            if data.get("ts") is not None or data.get("timestamp") is not None:
                                ts = data.get('ts', data.get('timestamp'))
                            telemetry_entry = TelemetryEntry({datapoint_key: full_value}, ts)
                            converted_data.add_to_telemetry(telemetry_entry)
                        else:
                            converted_data.add_to_attributes(datapoint_key, full_value)
                    except Exception as e:
                        StatisticsService.count_connector_message(self._log.name, 'convertersMsgDropped')
                        self._log.error('Error in converter, for config: \n%s\n and message: \n%s\n %s',
                                        dumps(datatype_config), data, e)
        except Exception as e:
            StatisticsService.count_connector_message(self._log.name, 'convertersMsgDropped')
            self._log.error('Error in converter, for config: \n%s\n and message: \n%s\n %s', dumps(self.__config),
                            str(data), e)

        StatisticsService.count_connector_message(self._log.name, 'convertersAttrProduced',
                                                  count=converted_data.attributes_datapoints_count)
        StatisticsService.count_connector_message(self._log.name, 'convertersTsProduced',
                                                  count=converted_data.telemetry_datapoints_count)
        return converted_data

    @CollectStatistics(start_stat_type='receivedBytesFromDevices',
                       end_stat_type='convertedBytesFromDevice')
    def convert(self, config, data):
        if data:
            if config['file_ext'] == 'csv' or (
                    config['file_ext'] == 'txt' and self.__config['txt_file_data_view'] == 'TABLE'):
                return self._convert_table_view_data(config, data)
            elif config['file_ext'] == 'txt' and self.__config['txt_file_data_view'] == 'SLICED':
                return self._convert_slices_view_data(data)
            elif config['file_ext'] == 'json':
                return self._convert_json_file(data)
            else:
                StatisticsService.count_connector_message(self._log.name, 'convertersMsgDropped')
                raise Exception('Incorrect file extension or file data view mode')
