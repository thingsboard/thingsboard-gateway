import datetime
import subprocess
from threading import Thread
from time import time, sleep
from platform import system as platform_system

import simplejson
from psutil import cpu_percent, virtual_memory, disk_usage
from psutil._common import bytes2human


class StatisticsServiceFunctions:
    @staticmethod
    def cpu_usage(_):
        return cpu_percent(0.2)

    @staticmethod
    def ram_usage(_):
        return virtual_memory().percent

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


class StatisticsService(Thread):
    DATA_STREAMS_STATISTICS = {
        'receivedBytesFromDevices': 0,
        'convertedBytesFromDevice': 0,
        'allReceivedBytesFromTB': 0,
        'allBytesSentToTB': 0,
        'allBytesSentToDevices': 0,
        'eventsAdded': 0,
        'eventsProcessed': 0,
    }

    DEFAULT_CONFIG = [
        {
            "function": StatisticsServiceFunctions.cpu_usage,
            "attributeOnGateway": "GATEWAY_CPU_LOAD"
        },
        {
            "function": StatisticsServiceFunctions.ram_usage,
            "attributeOnGateway": "GATEWAY_RAM_USAGE"
        },
        {
            "function": StatisticsServiceFunctions.free_disk_space,
            "attributeOnGateway": "GATEWAY_FREE_DISK_SPACE"
        },
        {
            "function": StatisticsServiceFunctions.connected_device,
            "attributeOnGateway": "GATEWAY_CONNECTED_DEVICES"
        },
        {
            "function": StatisticsServiceFunctions.active_connectors,
            "attributeOnGateway": "GATEWAY_ACTIVE_CONNECTORS"
        },
        {
            "function": StatisticsServiceFunctions.inactive_connectors,
            "attributeOnGateway": "GATEWAY_INACTIVE_CONNECTORS"
        },
        {
            "function": StatisticsServiceFunctions.total_connectors,
            "attributeOnGateway": "GATEWAY_TOTAL_CONNECTORS"
        },
    ]

    def __init__(self, stats_send_period_in_seconds, gateway, log, config_path=None):
        super().__init__()
        self.name = 'Statistics Thread'
        self.daemon = True
        self._stopped = False

        self._config_path = config_path
        self._stats_send_period_in_seconds = stats_send_period_in_seconds if stats_send_period_in_seconds >= 60 else 60
        self._gateway = gateway
        self._log = log
        self._config = self._load_config()
        self._last_poll = 0
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

        # load default statistics config
        if self.DEFAULT_CONFIG:
            config.extend(self.DEFAULT_CONFIG)

        return config

    @classmethod
    def add_bytes(cls, stat_type, bytes_count):
        cls.add_count(stat_type, bytes_count)

    @classmethod
    def add_count(cls, stat_type, count=1):
        cls.DATA_STREAMS_STATISTICS[stat_type] += count

    @classmethod
    def clear_streams_statistics(cls):
        cls.DATA_STREAMS_STATISTICS = {
            'receivedBytesFromDevices': 0,
            'convertedBytesFromDevice': 0,
            'allReceivedBytesFromTB': 0,
            'allBytesSentToTB': 0,
            'allBytesSentToDevices': 0,
            'eventsAdded': 0,
            'eventsProcessed': 0,
        }

    def run(self) -> None:
        while not self._stopped:
            if time() - self._last_poll >= self._stats_send_period_in_seconds:
                data_to_send = {}
                for attribute in self._config:
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

                    data_to_send[attribute['attributeOnGateway']] = value

                self._gateway.tb_client.client.send_telemetry(data_to_send)

                if datetime.datetime.now() - self._last_streams_statistics_clear_time >= datetime.timedelta(days=1):
                    self.clear_streams_statistics()

                self._gateway.tb_client.client.send_telemetry(StatisticsService.DATA_STREAMS_STATISTICS)

                self._last_poll = time()

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
