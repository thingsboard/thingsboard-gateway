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
from thingsboard_gateway.connectors.opcua.entities.rpc_request import OpcUaRpcRequest


class Device:
    ABSOLUTE_PATH_PATTERN = re.compile(r"\${(Root\\.[A-Za-z0-9_.:\\\[\]\-()]+)}")
    RELATIVE_PATH_PATTERN = re.compile(r"\${([A-Za-z0-9_.:\\\[\]\-()]+)}")
    NODE_ID_PATTERN = re.compile(r"(ns=\d+;[isgb]=[^}]+)")

    def __init__(self, path, name, device_profile, config, converter, converter_for_sub, device_node, logger):
        self._log = logger
        self.__configured_values_count = 0
        self.path = path
        self.device_node = device_node
        self.name = name
        self.device_profile = device_profile
        self.config = config
        self.converter = converter
        self.converter_for_sub = converter_for_sub
        self.values = {
            'timeseries': [],
            'attributes': []
        }
        self.shared_attributes_keys = self.__get_shared_attributes_keys()
        self.nodes = []
        self.subscription: Subscription | None = None
        self.nodes_data_change_subscriptions = {}
        self.report_strategy = None
        if self.config.get(REPORT_STRATEGY_PARAMETER):
            try:
                self.report_strategy = ReportStrategyConfig(self.config.get(REPORT_STRATEGY_PARAMETER))
            except ValueError as e:
                self._log.error('Invalid report strategy config for %s: %s, connector report strategy will be used',
                                self.name, e)

        self.load_values()

    def __get_shared_attributes_keys(self):
        result = []

        for attr_config in self.config.get('attributes_updates', []):
            result.append(attr_config['key'])

        return result

    def __repr__(self):
        return f'<Device> Path: {self.path}, Name: {self.name}, Configured values: {self.__configured_values_count}'

    def load_values(self):
        self.__configured_values_count = 0

        for section in ('attributes', 'timeseries'):
            for node_config in self.config.get(section, []):
                try:
                    value_str = node_config['value']

                    # Match NodeId value (e.g. ns=2;s=SomeNode)
                    node_id_match = re.search(Device.NODE_ID_PATTERN, value_str)
                    if node_id_match:
                        self.values[section].append({
                            'path': node_id_match.group(1),
                            'key': node_config['key'],
                            'timestampLocation': node_config.get('timestampLocation', 'gateway'),
                            REPORT_STRATEGY_PARAMETER: node_config.get(REPORT_STRATEGY_PARAMETER)
                        })
                        continue

                    # Match absolute path (e.g. ${Root\.Objects.Device.SomeNode})
                    absolute_path_match = re.search(Device.ABSOLUTE_PATH_PATTERN, value_str)
                    if absolute_path_match:
                        full_path = absolute_path_match.group(1).split('\\.')
                        self.values[section].append({
                            'path': full_path,
                            'key': node_config['key'],
                            'timestampLocation': node_config.get('timestampLocation', 'gateway'),
                            REPORT_STRATEGY_PARAMETER: node_config.get(REPORT_STRATEGY_PARAMETER)
                        })
                        continue

                    # Match relative path (e.g. ${Device.SomeNode})
                    relative_path_match = re.search(Device.RELATIVE_PATH_PATTERN, value_str)
                    if relative_path_match:
                        full_path = self.path + relative_path_match.group(1).split('\\.')
                        self.values[section].append({
                            'path': full_path,
                            'key': node_config['key'],
                            'timestampLocation': node_config.get('timestampLocation', 'gateway'),
                            REPORT_STRATEGY_PARAMETER: node_config.get(REPORT_STRATEGY_PARAMETER)
                        })
                        continue

                except KeyError as e:
                    self._log.error('Invalid config for %s (key %s not found)', node_config, e)

            self.__configured_values_count += len(self.values[section])

        self._log.debug('Loaded %r values for %s', len(self.values), self.name)

    def get_node_by_key(self, key):
        try:
            for node_config in self.nodes:
                if node_config['key'] == key:
                    return node_config['node']
            return None
        except KeyError:
            return None

    @staticmethod
    def get_device_rpc_arguments(device_config: dict, rpc_request: OpcUaRpcRequest, logger) -> list | None:
        try:
            for rpc in device_config['rpc_methods']:
                try:
                    if rpc['method'] == rpc_request.rpc_method:
                        arguments_from_config = rpc.get('arguments', [])

                        arguments = rpc_request.params if rpc_request.params is not None else [argument['value'] for
                                                                                               argument in
                                                                                               arguments_from_config if
                                                                                               argument.get('value')]
                        return arguments
                except KeyError as e:
                    logger.error("The requested method name %s does not match with config %s",
                                 rpc_request.rpc_method,
                                 str(e))
                    logger.debug("The requested method name %s does not match with config", exc_info=e)

        except KeyError:
            logger.warning('Rpc methods section is not specified')
