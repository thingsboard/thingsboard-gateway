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

from asyncua.common.subscription import Subscription

from thingsboard_gateway.gateway.constants import REPORT_STRATEGY_PARAMETER
from thingsboard_gateway.gateway.entities.report_strategy_config import ReportStrategyConfig


class Device:
    def __init__(self, path, name, config, converter, converter_for_sub, logger):
        self._log = logger
        self.__configured_values_count = 0
        self.path = path
        self.name = name
        self.config = config
        self.converter = converter
        self.converter_for_sub = converter_for_sub
        self.values = {
            'timeseries': [],
            'attributes': []
        }
        self.nodes = []
        self.subscription: Subscription | None = None
        self.nodes_data_change_subscriptions = {}
        self.report_strategy = None
        if self.config.get(REPORT_STRATEGY_PARAMETER):
            try:
                self.report_strategy = ReportStrategyConfig(self.config.get(REPORT_STRATEGY_PARAMETER))
            except ValueError as e:
                self._log.error('Invalid report strategy config for %s: %s, connector report strategy will be used', self.name, e)

        self.load_values()

    def __repr__(self):
        return f'<Device> Path: {self.path}, Name: {self.name}, Configured values: {self.__configured_values_count}'

    def load_values(self):
        self.__configured_values_count = 0
        for section in ('attributes', 'timeseries'):
            for node_config in self.config.get(section, []):
                try:
                    if re.search(r"(ns=\d+;[isgb]=[^}]+)", node_config['value']):
                        child = re.search(r"(ns=\d+;[isgb]=[^}]+)", node_config['value'])
                        self.values[section].append(
                            {'path': child.groups()[0], 'key': node_config['key'],
                             REPORT_STRATEGY_PARAMETER: node_config.get(REPORT_STRATEGY_PARAMETER)})
                    elif re.search(r"\${([A-Za-z.:\\\d]+)}", node_config['value']):
                        child = re.search(r"\${([A-Za-z.:\\\d]+)", node_config['value'])
                        self.values[section].append(
                            {'path': self.path + child.groups()[0].split('\\.'), 'key': node_config['key'],
                             REPORT_STRATEGY_PARAMETER: node_config.get(REPORT_STRATEGY_PARAMETER)})

                except KeyError as e:
                    self._log.error('Invalid config for %s (key %s not found)', node_config, e)
            self.__configured_values_count += len(self.values[section])

        self._log.debug('Loaded %r values for %s', len(self.values), self.name)
