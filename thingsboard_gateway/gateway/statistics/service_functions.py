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

import os

from psutil import cpu_percent, virtual_memory, disk_usage, Process
from psutil._common import bytes2human

from thingsboard_gateway.gateway.statistics import statistics_service

SELF_PROCESS = Process(os.getpid())


class StatisticsServiceFunctions:
    @staticmethod
    def total_memory(_):
        return bytes2human(virtual_memory().total)

    @staticmethod
    def total_disk_memory(_):
        return bytes2human(disk_usage('/').total)

    @staticmethod
    def cpu_usage(_):
        return cpu_percent(0.2)

    @staticmethod
    def gateway_cpu_usage(_):
        return SELF_PROCESS.cpu_percent(interval=0.2)

    @staticmethod
    def ram_usage(_):
        return virtual_memory().percent

    @staticmethod
    def gateway_ram_usage(_):
        return round(SELF_PROCESS.memory_percent(), 1)

    @staticmethod
    def free_disk_space(_):
        return bytes2human(disk_usage('/').free)

    @staticmethod
    def connected_device(gateway):
        return gateway.connected_devices

    @staticmethod
    def active_connectors(gateway):
        return gateway.active_connectors

    @staticmethod
    def inactive_connectors(gateway):
        return gateway.inactive_connectors

    @staticmethod
    def total_connectors(gateway):
        return gateway.total_connectors

    @staticmethod
    def msgs_sent_to_platform(_):
        return statistics_service.StatisticsService.STATISTICS_STORAGE.get('msgsSentToPlatform')

    @staticmethod
    def msgs_received_from_platform(_):
        return statistics_service.StatisticsService.STATISTICS_STORAGE.get('msgsReceivedFromPlatform')

    @staticmethod
    def storage_msgs_pulled(_):
        return statistics_service.StatisticsService.STATISTICS_STORAGE.get('storageMsgPulled')

    @staticmethod
    def storage_msgs_count(gateway):
        return gateway.get_storage_events_count()

    @staticmethod
    def platform_msgs_pushed(_):
        return statistics_service.StatisticsService.STATISTICS_STORAGE.get('platformMsgPushed')

    @staticmethod
    def platform_attr_produced(_):
        return statistics_service.StatisticsService.STATISTICS_STORAGE.get('platformAttrProduced')

    @staticmethod
    def platform_ts_produced(_):
        return statistics_service.StatisticsService.STATISTICS_STORAGE.get('platformTsProduced')
