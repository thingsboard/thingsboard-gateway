#      Copyright 2024. ThingsBoard
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
from threading import Thread
from time import sleep, monotonic
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

    def __init__(self, stats_send_period_in_seconds, gateway, log, config_path=None,
                 custom_stats_send_period_in_seconds=3600):
        super().__init__()
        self.name = 'Statistics Thread'
        self.daemon = True
        self._stopped = False

        self._config_path = config_path
        self._stats_send_period_in_seconds = stats_send_period_in_seconds if stats_send_period_in_seconds >= 60 else 60
        self._custom_stats_send_period_in_seconds = custom_stats_send_period_in_seconds
        self._gateway = gateway
        self._log = log
        self._custom_command_config = self._load_config()
        self._last_service_poll = 0
        self._last_custom_command_poll = 0
        self._last_streams_statistics_clear_time = datetime.datetime.now()

        self.start()

    def stop(self):
        self._stopped = True

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
    def clear_streams_statistics(cls):
        for key in cls.STATISTICS_STORAGE:
            cls.STATISTICS_STORAGE[key] = 0

    @staticmethod
    def count_connector_message(connector_name, stat_parameter_name, count=1):
        StatisticsService.add_count(connector_name, stat_parameter_name=stat_parameter_name,
                                    statistics_type='CONNECTOR_STATISTICS_STORAGE', count=count)

    @staticmethod
    def count_connector_bytes(connector_name, msg, stat_parameter_name):
        bytes_count = str(msg).__sizeof__()
        StatisticsService.add_bytes(connector_name, bytes_count, stat_parameter_name,
                                    statistics_type='CONNECTOR_STATISTICS_STORAGE')

    def __check_statistics_storage_must_reset(self):
        if datetime.datetime.now() - self._last_streams_statistics_clear_time >= datetime.timedelta(days=1):
            self.clear_streams_statistics()

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
        self._gateway.send_telemetry(custom_command_statistics_message)

    def __send_statistics(self):
        statistics_message = {'machineStats': self.__collect_statistics_from_config(MACHINE_STATS_CONFIG),
                              'serviceStats': self.__collect_service_statistics(),
                              'connectorsStats': self.CONNECTOR_STATISTICS_STORAGE}
        self._log.info('REGULAR STATS: %s', statistics_message)
        self._gateway.send_telemetry(statistics_message)

    def __send_general_machine_state(self):
        message = self.__collect_statistics_from_config(ONCE_SEND_STATISTICS_CONFIG)
        self._gateway.send_attributes(message)

    def __once_send_statistics(self):
        self.__send_general_machine_state()

    def run(self) -> None:
        self.__once_send_statistics()

        while not self._stopped:
            if monotonic() - self._last_service_poll >= self._stats_send_period_in_seconds:
                self.__check_statistics_storage_must_reset()

                self.__send_statistics()

                self._last_service_poll = monotonic()

            if monotonic() - self._last_custom_command_poll >= self._custom_stats_send_period_in_seconds:
                self.__send_custom_command_statistics()

                self._last_custom_command_poll = monotonic()

            sleep(1)
