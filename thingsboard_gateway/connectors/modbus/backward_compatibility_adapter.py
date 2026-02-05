#     Copyright 2026. ThingsBoard
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

from logging import getLogger
from simplejson import dumps

from thingsboard_gateway.connectors.modbus.constants import (
    BAUDRATE_PARAMETER,
    BYTE_ORDER_PARAMETER,
    HOST_PARAMETER,
    METHOD_PARAMETER,
    PORT_PARAMETER,
    RETRIES_PARAMETER,
    TIMEOUT_PARAMETER,
    WORD_ORDER_PARAMETER
)
from thingsboard_gateway.gateway.constants import TYPE_PARAMETER


class BackwardCompatibilityAdapter:
    config_files_count = 1
    CONFIG_PATH = None

    def __init__(self, config, config_dir, logger=None):
        if logger:
            self._log = logger
        else:
            self._log = getLogger('BackwardCompatibilityAdapter')

        self.__config = config
        self.__config_dir = config_dir
        BackwardCompatibilityAdapter.CONFIG_PATH = self.__config_dir
        self.__keys = [HOST_PARAMETER, PORT_PARAMETER, TYPE_PARAMETER, METHOD_PARAMETER, TIMEOUT_PARAMETER,
                       BYTE_ORDER_PARAMETER, WORD_ORDER_PARAMETER, RETRIES_PARAMETER, BAUDRATE_PARAMETER]

    @staticmethod
    def __save_json_config_file(config):
        path_to_file = '%rmodbus_new_%r.json' % (BackwardCompatibilityAdapter.CONFIG_PATH,
                                                 BackwardCompatibilityAdapter.config_files_count)
        with open(path_to_file, 'w') as file:
            file.writelines(dumps(config, sort_keys=False, indent='  ', separators=(',', ': ')))
        BackwardCompatibilityAdapter.config_files_count += 1

    def __check_slaves_type_connection(self, config):
        is_tcp_or_udp_connection = False
        is_serial_connection = False

        for slave in config['master']['slaves']:
            if slave[TYPE_PARAMETER] == 'tcp' or slave[TYPE_PARAMETER] == 'udp':
                is_tcp_or_udp_connection = True
            elif slave[TYPE_PARAMETER] == 'serial':
                is_serial_connection = True

        if is_tcp_or_udp_connection and is_serial_connection:
            self._log.warning('It seems that your slaves using different connection type (tcp/udp and serial). '
                              'It is recommended to separate tcp/udp slaves and serial slaves in different connectors '
                              'to avoid problems with reading data.')

    @staticmethod
    def _convert_slave_configuration(slave_config):
        if not slave_config:
            return {}

        values = slave_config.pop('values', {})
        slave_config['values'] = {}
        if len(values):
            for (key, value) in values.items():
                if isinstance(value, list):
                    value = value[0]
                for section_name, section_value in value.items():
                    if slave_config['values'].get(key) is None:
                        slave_config['values'][key] = {section_name: section_value}
                    else:
                        slave_config['values'][key].update({section_name: section_value})

        return slave_config

    def convert(self):
        if not self.__config.get('server'):
            # check if slaves are similar type connection
            self.__check_slaves_type_connection(self.__config)
            converted_slave_config = self._convert_slave_configuration(self.__config.get('slave'))
            self.__config['slave'] = converted_slave_config
            return self.__config

        self._log.warning(
            'You are using old configuration structure for Modbus connector. It will be DEPRECATED in the future '
            'version! New config file "modbus_new.json" was generated in %s folder. Please, use it.', self.CONFIG_PATH)
        self._log.warning('You have to manually connect the new generated config file to tb_gateway.json!')

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
