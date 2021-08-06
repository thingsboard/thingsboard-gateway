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

from thingsboard_gateway.connectors.ftp.ftp_converter import FTPConverter
from thingsboard_gateway.connectors.converter import log


class FTPUplinkConverter(FTPConverter):
    def __init__(self, config):
        self.__config = config

    def _convert_table_view_data(self, config, data):
        get_device_name_from_data = False
        get_device_type_from_data = False
        data_types = {'attributes': 'attributes', 'timeseries': 'telemetry'}
        result = {
            'deviceName': None,
            'deviceType': None,
            'attributes': [],
            'telemetry': []
        }

        if '${' in self.__config['devicePatternName'] and '}' in self.__config['devicePatternName']:
            get_device_name_from_data = True
        else:
            result['deviceName'] = self.__config['devicePatternName']
        if '${' in self.__config['devicePatternType'] and '}' in self.__config['devicePatternType']:
            get_device_type_from_data = True
        else:
            result['deviceType'] = self.__config['devicePatternType']

        try:
            for data_type in data_types:
                for information in self.__config[data_type]:
                    arr = data.split(self.__config['delimiter'])

                    key_index = information['key']
                    val_index = information['value']

                    if '${' in information['key'] and '}' in information['key']:
                        key_index = config['headers'].index(re.sub(r'[^\w]', '', information['key']))

                    if '${' in information['value'] and '}' in information['value']:
                        val_index = config['headers'].index(re.sub(r'[^\w]', '', information['value']))

                    result[data_types[data_type]].append({
                        arr[key_index] if isinstance(key_index, int) else key_index:
                            arr[val_index] if isinstance(val_index, int) else val_index
                    })

                    if get_device_name_from_data:
                        index = config['headers'].index(re.sub(r'[^\w]', '', self.__config['devicePatternName']))
                        result['deviceName'] = arr[index]
                    if get_device_type_from_data:
                        index = config['headers'].index(re.sub(r'[^\w]', '', self.__config['devicePatternType']))
                        result['deviceType'] = arr[index]

        except Exception as e:
            log.error('Error in converter, for config: \n%s\n and message: \n%s\n', dumps(self.__config), data)
            log.exception(e)

        return result

    def _convert_slices_view_data(self, config, data):
        return

    def convert(self, config, data):
        if config['file_ext'] == 'csv' or (config['file_ext'] == 'txt' and self.__config['txt_file_data_view'] == 'TABLE'):  # TODO: add branch for TXT (not table view)
            return self._convert_table_view_data(config, data)
        elif config['file_ext'] == 'txt' and self.__config['txt_file_data_view'] == 'SLICED':  # TODO: change from str comparison to regex match
            return self._convert_slices_view_data(config, data)
        else:
            raise Exception('Incorrect txt file data view mode')
