import datetime
import os
import subprocess
from threading import Thread
from time import sleep, monotonic
from platform import system as platform_system

import simplejson
from psutil import cpu_percent, virtual_memory, disk_usage, Process
from psutil._common import bytes2human


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
        return StatisticsService.STATISTICS_STORAGE.get('msgsSentToPlatform')

    @staticmethod
    def msgs_received_from_platform(_):
        return StatisticsService.STATISTICS_STORAGE.get('msgsReceivedFromPlatform')

    @staticmethod
    def storage_msgs_pulled(_):
        return StatisticsService.STATISTICS_STORAGE.get('storageMsgPulled')

    @staticmethod
    def storage_msgs_count(gateway):
        return gateway.get_storage_events_count()

    @staticmethod
    def platform_msgs_pushed(_):
        return StatisticsService.STATISTICS_STORAGE.get('platformMsgPushed')

    @staticmethod
    def platform_attr_produced(_):
        return StatisticsService.STATISTICS_STORAGE.get('platformAttrProduced')

    @staticmethod
    def platform_ts_produced(_):
        return StatisticsService.STATISTICS_STORAGE.get('platformTsProduced')


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

    ONCE_SEND_STATISTICS_CONFIG = [
        {
            "function": StatisticsServiceFunctions.total_memory,
            "attributeOnGateway": "totalMemory"
        },
        {
            "function": StatisticsServiceFunctions.total_disk_memory,
            "attributeOnGateway": "totalDisk"
        }
    ]

    SERVICE_STATS_CONFIG = [
        {
            "function": StatisticsServiceFunctions.storage_msgs_pulled,
            "attributeOnGateway": "storageMsgPulled"
        },
        {
            "function": StatisticsServiceFunctions.storage_msgs_count,
            "attributeOnGateway": "storageMsgCount"
        },
        {
            "function": StatisticsServiceFunctions.platform_msgs_pushed,
            "attributeOnGateway": "platformMsgPushed"
        },
        {
            "function": StatisticsServiceFunctions.platform_attr_produced,
            "attributeOnGateway": "platformAttrProduced"
        },
        {
            "function": StatisticsServiceFunctions.platform_ts_produced,
            "attributeOnGateway": "platformTsProduced"
        }
    ]

    MACHINE_STATS_CONFIG = [
        {
            "function": StatisticsServiceFunctions.cpu_usage,
            "attributeOnGateway": "totalCpuUsage"
        },
        {
            "function": StatisticsServiceFunctions.ram_usage,
            "attributeOnGateway": "freeMemory"
        },
        {
            "function": StatisticsServiceFunctions.free_disk_space,
            "attributeOnGateway": "freeDisk"
        },
        {
            "function": StatisticsServiceFunctions.gateway_cpu_usage,
            "attributeOnGateway": "gwProcessCpuUsage"
        },
        {
            "function": StatisticsServiceFunctions.gateway_ram_usage,
            "attributeOnGateway": "gwMemory"
        },
        {
            "function": StatisticsServiceFunctions.msgs_sent_to_platform,
            "attributeOnGateway": "msgsSentToPlatform"
        },
        {
            "function": StatisticsServiceFunctions.msgs_received_from_platform,
            "attributeOnGateway": "msgsReceivedFromPlatform"
        }
    ]

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

        for attribute in self.SERVICE_STATS_CONFIG:
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

    def __send_machine_statistics(self):
        message = self.__collect_statistics_from_config(self.MACHINE_STATS_CONFIG)
        self._gateway.send_telemetry({'machineStats': message})

    def __send_service_statistics(self):
        message = self.__collect_service_statistics()
        self._gateway.send_telemetry({'service': message})

    def __send_connectors_statistics(self):
        self._gateway.send_telemetry({'connectorsStats': self.CONNECTOR_STATISTICS_STORAGE})

    def __send_general_machine_state(self):
        message = self.__collect_statistics_from_config(self.ONCE_SEND_STATISTICS_CONFIG)
        self._gateway.send_attributes(message)

    def __once_send_statistics(self):
        self.__send_general_machine_state()

    def run(self) -> None:
        self.__once_send_statistics()

        while not self._stopped:
            if monotonic() - self._last_service_poll >= self._stats_send_period_in_seconds:
                self.__check_statistics_storage_must_reset()

                self.__send_machine_statistics()
                self.__send_service_statistics()
                self.__send_connectors_statistics()

                self._last_service_poll = monotonic()

            if monotonic() - self._last_custom_command_poll >= self._custom_stats_send_period_in_seconds:
                self.__send_custom_command_statistics()

                self._last_custom_command_poll = monotonic()

            sleep(1)

    class CollectStatistics:
        def __init__(self, start_stat_type, end_stat_type=None):
            self.start_stat_type = start_stat_type
            self.end_stat_type = end_stat_type

        def __call__(self, func):
            def inner(*args, **kwargs):
                try:
                    _, __, data = args
                    self.collect(self.start_stat_type, data)
                except ValueError:
                    pass

                result = func(*args, **kwargs)
                if result and self.end_stat_type:
                    self.collect(self.end_stat_type, result)

                return result

            return inner

        @staticmethod
        def collect(stat_type, data):
            bytes_count = str(data).__sizeof__()
            StatisticsService.add_bytes(stat_type, bytes_count)

    class CollectAllReceivedBytesStatistics(CollectStatistics):
        def __call__(self, func):
            def inner(*args, **kwargs):
                try:
                    _, data = args
                    self.collect(self.start_stat_type, data)
                except ValueError:
                    pass

                result = func(*args, **kwargs)
                return result

            return inner

    class CollectAllSentTBBytesStatistics(CollectAllReceivedBytesStatistics):
        def __call__(self, func):
            return super().__call__(func)

    class CollectRPCReplyStatistics(CollectStatistics):
        def __call__(self, func):
            def inner(*args, **kwargs):
                data = kwargs.get('content')
                if data:
                    self.collect(self.start_stat_type, data)

                func(*args, **kwargs)

            return inner

    class CollectStorageEventsStatistics(CollectStatistics):
        def __call__(self, func):
            def inner(*args, **kwargs):
                try:
                    data = args[3]
                    if data:
                        StatisticsService.add_count(self.start_stat_type)
                except IndexError:
                    pass

                func(*args, **kwargs)

            return inner

    class CountMessage(CollectStatistics):
        def __call__(self, func):
            def inner(*args, **kwargs):
                StatisticsService.add_count(self.start_stat_type)
                result = func(*args, **kwargs)
                return result

            return inner
