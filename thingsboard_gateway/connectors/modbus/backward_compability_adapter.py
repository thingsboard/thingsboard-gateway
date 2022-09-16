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

from thingsboard_gateway.connectors.connector import log


class BackwardCompatibilityAdapter:
    config_files_count = 1
    CONFIG_PATH = None

    def __init__(self, config, config_dir):
        self.__config = config
        self.__config_dir = config_dir
        BackwardCompatibilityAdapter.CONFIG_PATH = self.__config_dir
        self.__keys = ['host', 'port', 'type', 'method', 'timeout', 'byteOrder', 'wordOrder', 'retries', 'retryOnEmpty',
                       'retryOnInvalid', 'baudrate']

    @staticmethod
    def __save_json_config_file(config):
        with open(
                f'{BackwardCompatibilityAdapter.CONFIG_PATH}modbus_new_{BackwardCompatibilityAdapter.config_files_count}.json',
                'w') as file:
            file.writelines(dumps(config, sort_keys=False, indent='  ', separators=(',', ': ')))
        BackwardCompatibilityAdapter.config_files_count += 1

    @staticmethod
    def __check_slaves_type_connection(config):
        is_tcp_or_udp_connection = False
        is_serial_connection = False

        for slave in config['master']['slaves']:
            if slave['type'] == 'tcp' or slave['type'] == 'udp':
                is_tcp_or_udp_connection = True
            elif slave['type'] == 'serial':
                is_serial_connection = True

        if is_tcp_or_udp_connection and is_serial_connection:
            log.warning('It seems that your slaves using different connection type (tcp/udp and serial). '
                        'It is recommended to separate tcp/udp slaves and serial slaves in different connectors '
                        'to avoid problems with reading data.')

    def convert(self):
        if not self.__config.get('server'):
            # check if slaves are similar type connection
            self.__check_slaves_type_connection(self.__config)
            return self.__config

        log.warning(
            'You are using old configuration structure for Modbus connector. It will be DEPRECATED in the future '
            'version! New config file "modbus_new.json" was generated in %s folder. Please, use it.', self.CONFIG_PATH)
        log.warning('You have to manually connect the new generated config file to tb_gateway.yaml!')

        slaves = []
        for device in self.__config['server'].get('devices', []):
            slave = {**device}

            for key in self.__keys:
                if not device.get(key):
                    slave[key] = self.__config['server'].get(key)

            slave['pollPeriod'] = slave['timeseriesPollPeriod']

            slaves.append(slave)

        result_dict = {'master': {'slaves': slaves}, 'slave': self.__config.get('slave')}

        # check if slaves are similar type connection
        self.__check_slaves_type_connection(result_dict)

        self.__save_json_config_file(result_dict)

        return result_dict
