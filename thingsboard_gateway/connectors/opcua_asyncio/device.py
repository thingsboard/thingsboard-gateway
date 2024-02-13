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

import re


class Device:
    def __init__(self, path, name, config, converter, converter_for_sub, logger):
        self._log = logger
        self.path = path
        self.name = name
        self.config = config
        self.converter = converter
        self.converter_for_sub = converter_for_sub
        self.values = {
            'timeseries': [],
            'attributes': []
        }

        self.load_values()

    def __repr__(self):
        return f'{self.path}'

    def load_values(self):
        for section in ('attributes', 'timeseries'):
            for node_config in self.config.get(section, []):
                try:
                    if re.search(r"(ns=\d+;[isgb]=[^}]+)", node_config['path']):
                        child = re.search(r"(ns=\d+;[isgb]=[^}]+)", node_config['path'])
                        self.values[section].append({'path': child.groups()[0], 'key': node_config['key']})
                    elif re.search(r"\${([A-Za-z.:\\\d]+)}", node_config['path']):
                        child = re.search(r"\${([A-Za-z.:\\\d]+)", node_config['path'])
                        self.values[section].append(
                            {'path': self.path + child.groups()[0].split('\\.'), 'key': node_config['key']})

                except KeyError as e:
                    self._log.error('Invalid config for %s (key %s not found)', node_config, e)
