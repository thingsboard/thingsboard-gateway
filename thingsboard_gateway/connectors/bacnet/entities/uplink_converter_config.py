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

class UplinkConverterConfig:
    def __init__(self, config, device_info, device_details):
        self.__config = config

        self.device_details = device_details
        self.device_name = device_info.device_name
        self.device_type = device_info.device_type
        self.__objects_to_read = self.__get_objects_to_read()
        self.report_strategy = self.__config.get('reportStrategy', {})

    def __get_objects_to_read(self):
        objects_to_read = []

        for section in ('attributes', 'timeseries'):
            section_config = self.__config.get(section, [])

            if section_config != '*':
                for item in section_config:
                    is_local_discovery_config = list(filter(lambda key_value: key_value == '*', item.values()))
                    if len(is_local_discovery_config) == 0:
                        object_to_read = {**item, 'type': section}

                        if item['objectType'] == 'device':
                            object_to_read['objectId'] = self.device_details.object_id

                        objects_to_read.append(object_to_read)

        return objects_to_read

    @property
    def objects_to_read(self):
        return self.__objects_to_read
