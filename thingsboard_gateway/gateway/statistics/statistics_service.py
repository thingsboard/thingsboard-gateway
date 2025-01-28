#      Copyright 2025. ThingsBoard
#  #
#      Licensed under the Apache License, Version 2.0 (the "License");
#      you may not use this file except in compliance with the License.
#      You may obtain a copy of the License at
#  #
#          http://www.apache.org/licenses/LICENSE-2.0
#  #
#      Unless required by applicable law or agreed to in writing, software
#      distributed under the License is distributed on an "AS IS" BASIS,
#      WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#      See the License for the specific language governing permissions and
#      limitations under the License.

import datetime
import subprocess
from threading import Thread, RLock, Event
from time import monotonic
from platform import system as platform_system

import simplejson

from thingsboard_gateway.gateway.statistics.configs import ONCE_SEND_STATISTICS_CONFIG, SERVICE_STATS_CONFIG, \
    MACHINE_STATS_CONFIG


class StatisticsService(Thread):
    STATISTICS_STORAGE = {
        'receivedBytesFromDevices': 0,
        'convertedBytesFromDevice': 0,
        'allReceivedBytesFromTB': 0,
        'allBytesSentToTB': 0,
        'allBytesSentToDevices': 0,
        'eventsAdded': 0,
        'eventsProcessed': 0,
        'msgsSentToPlatform': 0,
        'msgsReceivedFromPlatform': 0,
        'storageMsgPulled': 0,
        'platformMsgPushed': 0,
        'platformAttrProduced': 0,
        'platformTsProduced': 0,
    }

    # This is a dictionary that stores the statistics for each connector
    # The key is the connector name and the value is another dictionary
    # The key of the inner dictionary is the statistic name and the value is the statistic value
    # Example:
    # {
    #     "connector_name": {
    #         "statistic_name": statistic_value
    #         ...
    #     }
    # }
    CONNECTOR_STATISTICS_STORAGE = {}
    __LOCK = RLock()

    def __init__(self, stats_send_period_in_seconds, gateway, log, config_path=None,
                 custom_stats_send_period_in_seconds=3600):
        super().__init__()
        self.name = 'Statistics Thread'
        self.daemon = True
        self._stopped = Event()

        self._config_path = config_path
        self._stats_send_period_in_seconds = stats_send_period_in_seconds if stats_send_period_in_seconds >= 10 else 10
        self._custom_stats_send_period_in_seconds = custom_stats_send_period_in_seconds
        self._gateway = gateway
        self._log = log
        self._custom_command_config = self._load_config()
        self.__install_required_tools()
        self._last_service_poll = 0
        self._last_custom_command_poll = 0
        self._last_streams_statistics_clear_time = datetime.datetime.now()

        self.start()

    def stop(self):
        self._stopped.set()

    def _load_config(self):
        config = []

        # load custom statistics config
        if self._config_path:
            with open(self._config_path, 'r') as file:
                config = simplejson.load(file)

        return config

    @classmethod
    def add_bytes(cls, stat_type, bytes_count, stat_parameter_name=None, statistics_type='STATISTICS_STORAGE'):
        cls.add_count(stat_type, bytes_count, stat_parameter_name, statistics_type)

    @classmethod
    def add_count(cls, stat_key, count=1, stat_parameter_name=None, statistics_type='STATISTICS_STORAGE'):
        if statistics_type == 'CONNECTOR_STATISTICS_STORAGE':
            try:
                if cls.CONNECTOR_STATISTICS_STORAGE.get(stat_key) is None:
                    cls.CONNECTOR_STATISTICS_STORAGE[stat_key] = {}

                cls.CONNECTOR_STATISTICS_STORAGE[stat_key][stat_parameter_name] += count
            except KeyError:
                cls.CONNECTOR_STATISTICS_STORAGE[stat_key].update({stat_parameter_name: count})
        else:
            cls.STATISTICS_STORAGE[stat_key] += count

    @classmethod
    def clear_statistics(cls):
        with cls.__LOCK:
            for key in cls.STATISTICS_STORAGE:
                cls.STATISTICS_STORAGE[key] = 0
            cls.CONNECTOR_STATISTICS_STORAGE = {}

    @staticmethod
    def count_connector_message(connector_name, stat_parameter_name, count=1):
        StatisticsService.add_count(connector_name, stat_parameter_name=stat_parameter_name,
                                    statistics_type='CONNECTOR_STATISTICS_STORAGE', count=count)

    @staticmethod
    def count_connector_bytes(connector_name, msg, stat_parameter_name):
        bytes_count = str(msg).__sizeof__()
        StatisticsService.add_bytes(connector_name, bytes_count, stat_parameter_name,
                                    statistics_type='CONNECTOR_STATISTICS_STORAGE')

    def __install_required_tools(self):
        if self._custom_command_config:
            for attribute in self._custom_command_config:
                installation_command = attribute.get('installCmd')
                if installation_command:
                    try:
                        if platform_system() == 'Windows':
                            subprocess.run(installation_command, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                           encoding='utf-8', timeout=attribute['timeout'])
                        else:
                            subprocess.run(['/bin/sh', '-c', installation_command], stdout=subprocess.PIPE,
                                           stderr=subprocess.PIPE, encoding='utf-8', timeout=attribute['timeout'])
                    except Exception as e:
                        self._log.error("Error while executing installation command '%s': %s", installation_command, e)

    def __collect_custom_command_statistics(self):
        message = {}
        for attribute in self._custom_command_config:
            try:
                if attribute.get('command') is not None:
                    if platform_system() == 'Windows':
                        process = subprocess.run(attribute['command'], stdout=subprocess.PIPE,
                                                 stderr=subprocess.PIPE,
                                                 encoding='utf-8', timeout=attribute['timeout'])
                    else:
                        process = subprocess.run(['/bin/sh', '-c', attribute['command']],
                                                 stdout=subprocess.PIPE,
                                                 stderr=subprocess.PIPE,
                                                 encoding='utf-8', timeout=attribute['timeout'])

                    value = process.stdout
                else:
                    value = attribute['function'](self._gateway)
                if not value:
                    continue
                try:
                    value = float(value)
                except ValueError:
                    pass
            except Exception as e:
                self._log.warning("Statistic parameter %s raise the exception: %s",
                                  attribute['attributeOnGateway'], e)
                continue
            message[attribute['attributeOnGateway']] = value

        return message

    def __collect_statistics_from_config(self, statistics_config):
        message = {}

        for attribute in statistics_config:
            try:
                message[attribute['attributeOnGateway']] = attribute['function'](self._gateway)
            except Exception as e:
                self._log.warning("Statistic parameter %s raise the exception: %s",
                                  attribute['attributeOnGateway'], e)
                continue

        return message

    def __collect_service_statistics(self):
        message = {}

        for attribute in SERVICE_STATS_CONFIG:
            try:
                message[attribute['attributeOnGateway']] = attribute['function'](self._gateway)
            except Exception as e:
                self._log.warning("Statistic parameter %s raise the exception: %s",
                                  attribute['attributeOnGateway'], e)
                continue

        return message

    def __send_custom_command_statistics(self):
        custom_command_statistics_message = self.__collect_custom_command_statistics()
        if custom_command_statistics_message:
            self._gateway.send_telemetry(custom_command_statistics_message)

    def __send_statistics(self):
        statistics_message = {'machineStats': self.__collect_statistics_from_config(MACHINE_STATS_CONFIG),
                              'serviceStats': self.__collect_service_statistics(),
                              'connectorsStats': self.CONNECTOR_STATISTICS_STORAGE}
        self._log.info('Collected regular statistics: %s', statistics_message)
        self._gateway.send_telemetry(statistics_message)
        self.clear_statistics()

    def __send_general_machine_state(self):
        message = self.__collect_statistics_from_config(ONCE_SEND_STATISTICS_CONFIG)
        self._gateway.send_attributes(message)

    def __once_send_statistics(self):
        self.__send_general_machine_state()

    def run(self) -> None:
        try:
            self.__once_send_statistics()
        except Exception as e:
            self._log.error("Error while sending first statistics information: %s", e)

        while not self._stopped.is_set():
            try:
                cur_monotonic = int(monotonic())

                next_service_poll = (self._last_service_poll + self._stats_send_period_in_seconds) - cur_monotonic
                next_custom_command_poll = (self._last_custom_command_poll +
                                            self._custom_stats_send_period_in_seconds) - cur_monotonic

                wait_time = max(0, min(next_service_poll, next_custom_command_poll))

                if wait_time > 0:
                    self._stopped.wait(wait_time)

                cur_monotonic = int(monotonic())
                if (cur_monotonic - self._last_service_poll >= self._stats_send_period_in_seconds or
                        self._last_service_poll == 0):
                    self._last_service_poll = cur_monotonic
                    self.__send_statistics()
                    self.clear_statistics()

                if (cur_monotonic - self._last_custom_command_poll >= self._custom_stats_send_period_in_seconds or
                        self._last_custom_command_poll == 0):
                    self._last_custom_command_poll = cur_monotonic
                    self.__send_custom_command_statistics()

            except Exception as e:
                self._log.error("Error in statistics thread: %s", e)
                self._stopped.wait(5)
