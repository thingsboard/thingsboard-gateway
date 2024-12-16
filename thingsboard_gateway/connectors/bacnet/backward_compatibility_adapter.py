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

from copy import deepcopy


class BackwardCompatibilityAdapter:
    def __init__(self, config, logger):
        self.__log = logger
        self.__config = deepcopy(config)

    def convert(self):
        try:
            self.__convert_application_section()
            self.__convert_device_section()
        except Exception as e:
            self.__log.error('Error converting old config: %s', e)

        return self.__config

    def __convert_application_section(self):
        general_section = self.__config.pop('general', {})
        self.__convert_address_in_section(general_section)
        self.__config['application'] = general_section

    def __convert_device_section(self):
        for device in self.__config.get('devices', []):
            self.__convert_device_info_section(device)
            self.__convert_address_in_section(device)

            for section in ('attributes', 'timeseries'):
                for item_config in device.get(section, []):
                    self.__convert_object_id(item_config)

            for attr_update_config in device.get('attributeUpdates', []):
                self.__convert_object_id(attr_update_config)

            for rpc_config in device.get('rpc', []):
                self.__convert_object_id(rpc_config)

    @staticmethod
    def __convert_address_in_section(old_config_section):
        address = old_config_section.get('address', '')
        if address:
            return old_config_section
        if 'host' in old_config_section:
            address += old_config_section.pop('host')
            address = address.rstrip('/')
        if 'mask' in old_config_section:
            network_mask = old_config_section.pop('mask', None)
            if network_mask:
                address += '/' + network_mask
        if 'port' in old_config_section:
            address += ':' + str(old_config_section.pop('port', 47808))
        old_config_section['address'] = address

    @staticmethod
    def __convert_device_info_section(old_device_config):
        device_name = old_device_config.pop('deviceName')
        device_type = old_device_config.pop('deviceType', 'default')

        old_device_config['deviceInfo'] = {
            'deviceNameExpressionSource': 'expression',
            'deviceProfileExpressionSource': 'expression',
            'deviceNameExpression': device_name,
            'deviceProfileExpression': device_type
        }

    @staticmethod
    def __convert_object_id(old_item_config):
        old_object_id = old_item_config.pop('objectId')
        (object_type, object_id) = old_object_id.split(':')
        old_item_config['objectType'] = object_type
        old_item_config['objectId'] = object_id

    @staticmethod
    def is_old_config(config):
        return config.get('general') is not None
