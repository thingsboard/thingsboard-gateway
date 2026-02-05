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

import re
from asyncio import sleep, CancelledError
from time import monotonic
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
        self.shared_attributes_keys_value_pairs = self.__match_key_value_for_attribute_updates()
        self.nodes = []
        self.subscription: Subscription | None = None
        self.subscription_has_expired = False
        self.subscription_watchlog_task = None
        self.nodes_data_change_subscriptions = {}
        self.report_strategy = None
        self.__stopped = False
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

    def __match_key_value_for_attribute_updates(self):
        result = {}
        for attr_config in self.config.get('attributes_updates', []):
            result[attr_config['key']] = attr_config['value'] if attr_config['value'] else None
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

    async def subscription_watchlog(self, death_interval: int):
        try:
            while not self.__stopped and self.subscription and not self.subscription_has_expired:
                await sleep(death_interval)
                if self.subscription is None:
                    continue
                last_activity_time = self.last_subscription_activity
                if last_activity_time is None:
                    continue
                silent_interval = monotonic() - last_activity_time
                if silent_interval >= death_interval:
                    self._log.warning(
                        "Subscription for device %s has not received any data change notifications for %s seconds. Marking subscription as expired.",
                        self.name, silent_interval)
                    self.subscription_has_expired = True
                    return

        except CancelledError:
            self._log.debug("Subscription watchlog task for device %s has been cancelled.", self.name)
            return

    async def stop_watchlog_task(self):
        if self.subscription_watchlog_task:
            self.subscription_watchlog_task.cancel()
            try:
                await self.subscription_watchlog_task
                self._log.debug("Subscription watchlog task for device %s has been successfully cancelled.", self.name)
            except CancelledError:
                self._log.debug("Subscription watchlog task for device %s has been successfully cancelled.", self.name)
            except Exception as e:
                self._log.error("Error while cancelling subscription watchlog task for device %s: %s", self.name, e)
        self.subscription_watchlog_task = None

    @staticmethod
    def is_valid_rpc_method_name(rpc_device_section: dict, rpc_request: OpcUaRpcRequest) -> bool:
        for rpc in rpc_device_section:
            if rpc.get('method') == rpc_request.rpc_method:
                return True
        return False

    @staticmethod
    def get_device_rpc_arguments(rpc_device_section: dict, rpc_request: OpcUaRpcRequest) -> list | None | dict:
        arguments = []
        found_method_rpc_section = next(
            filter(lambda rpc: rpc.get('method') == rpc_request.rpc_method, rpc_device_section))
        arguments_from_config = found_method_rpc_section.get('arguments', [])
        empty_arguments_condition = not arguments_from_config

        if empty_arguments_condition:
            return rpc_request.arguments

        if arguments_from_config:

            arguments = [argument['value'] for argument in arguments_from_config if argument.get('value')]

            if rpc_request.arguments and not isinstance(rpc_request.arguments, list):
                error_message = "The arguments must be specified in the square quotes []"
                return {"error": error_message}

            if rpc_request.arguments and len(rpc_request.arguments) == len(arguments_from_config):
                arguments = rpc_request.arguments
                return arguments

            elif arguments and len(arguments) != len(arguments_from_config):
                error_message = "You must either define values for arguments in config or along with rpc request"
                return {"error": error_message}

            elif (rpc_request.arguments and len(rpc_request.arguments) != len(arguments_from_config)) or (
                    not arguments and rpc_request.arguments is None):
                error_message = f"Expected {len(arguments_from_config)} arguments, but got {len(rpc_request.arguments) if rpc_request.arguments is not None else 0}"
                return {"error": error_message}

        return arguments

    @property
    def stopped(self):
        return self.__stopped

    @stopped.setter
    def stopped(self, value: bool):
        self.__stopped = value

    @property
    def last_subscription_activity(self):
        subscription = getattr(self, 'subscription', None)
        if subscription is None:
            return None
        handler = getattr(subscription, '_handler', None)
        if handler is None:
            return None
        return getattr(handler, 'last_subscription_activity', None)
