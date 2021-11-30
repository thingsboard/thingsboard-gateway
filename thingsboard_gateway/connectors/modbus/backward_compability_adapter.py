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

from thingsboard_gateway.connectors.connector import log


class BackwardCompatibilityAdapter:
    def __init__(self, config):
        self.__config = config
        self.__keys = ['host', 'port', 'type', 'method', 'timeout', 'byteOrder', 'wordOrder', 'retries', 'retryOnEmpty',
                       'retryOnInvalid', 'baudrate']

    @staticmethod
    def __save_json_config_file(config):
        with open('config/modbus_new.json', 'w') as file:
            file.writelines(dumps(config, sort_keys=True, indent='  ', separators=(',', ': ')))

    def convert(self):
        if not self.__config.get('server'):
            return self.__config

        log.warning(
            'You are using old configuration structure for Modbus connector. It will be DEPRECATED in the future '
            'version! New config file "modbus_new.json" was generated in config/ folder. Please, use it.')

        slaves = []
        for device in self.__config['server'].get('devices', []):
            slave = {**device}

            for key in self.__keys:
                if not device.get(key):
                    slave[key] = self.__config['server'].get(key)

            slave['pollPeriod'] = slave['timeseriesPollPeriod']

            slaves.append(slave)

        result_dict = {'master': {'slaves': slaves}, 'slave': self.__config.get('slave')}
        self.__save_json_config_file(result_dict)

        return result_dict
