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

from thingsboard_gateway.tb_utility.tb_utility import TBUtility


class DeviceInfo:
    def __init__(self, device_info_config, device_details):
        self.device_name = self.__parse_device_name(device_info_config, device_details)
        self.device_type = self.__parse_device_type(device_info_config, device_details)

    def __parse_device_name(self, config, data):
        if config.get('deviceNameExpressionSource', 'expression') == 'expression':
            return self.__parse_device_info(config['deviceNameExpression'], data)

        return config['deviceNameExpression']

    def __parse_device_type(self, config, data):
        if config.get('deviceProfileExpressionSource', 'expression') == 'expression':
            return self.__parse_device_info(config.get('deviceProfileExpression', 'default'), data)

        return config.get('deviceProfileExpression', 'default')

    @staticmethod
    def __parse_device_info(expression, data):
        result_tags = TBUtility.get_values(expression, data.as_dict, get_tag=True)
        result_values = TBUtility.get_values(expression, data.as_dict, expression_instead_none=True)

        result = expression
        for (result_tag, result_value) in zip(result_tags, result_values):
            is_valid_key = "${" in expression and "}" in expression
            result = result.replace('${' + str(result_tag) + '}',
                                    str(result_value)) if is_valid_key else result_tag

        return result
