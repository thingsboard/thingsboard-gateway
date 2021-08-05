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

from simplejson import dumps

from thingsboard_gateway.connectors.ftp.ftp_converter import FTPConverter
from thingsboard_gateway.connectors.converter import log
from thingsboard_gateway.tb_utility.tb_utility import TBUtility


class FTPUplinkConverter(FTPConverter):
    def __init__(self, config):
        self.__config = config

    def convert(self, config, data):
        data_types = {'attributes': 'attributes', 'timeseries': 'telemetry'}
        result = {
            'deviceName': None,
            'deviceType': None,
            'attributes': [],
            'telemetry': []
        }

        for data_type in data_types:
            for information in self.__config[data_type]:
                print(data.split(','))
                # f = data.split(self.__config['delimiter'])
                # print(f)
                result[data_types[data_type]].append({'asd': 'asd'})
                # f = []

        return result
