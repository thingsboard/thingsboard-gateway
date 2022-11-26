import datetime
import subprocess
from threading import Thread
from time import time, sleep

import simplejson


class StatisticsService(Thread):
    DATA_STREAMS_STATISTICS = {
        'receivedBytesFromDevices': 0,
        'convertedBytesFromDevice': 0,
        'allReceivedBytesFromTB': 0,
        'allBytesSentToTB': 0,
        'allBytesSentToDevices': 0,
    }

    def __init__(self, stats_send_period_in_seconds, gateway, log, config_path=None):
        super().__init__()
        self.name = 'Statistics Thread'
        self.daemon = True
        self._stopped = False

        self._config_path = config_path
        self._stats_send_period_in_seconds = stats_send_period_in_seconds / 1000
        self._gateway = gateway
        self._log = log
        self._config = self._load_config()
        self._last_poll = 0
        self._last_streams_statistics_clear_time = datetime.datetime.now()

        self.start()

    def stop(self):
        self._stopped = True

    def _load_config(self):
        if self._config_path:
            with open(self._config_path, 'r') as file:
                return simplejson.load(file)

        return []

    @classmethod
    def add_bytes(cls, stat_type, bytes_count):
        cls.DATA_STREAMS_STATISTICS[stat_type] += bytes_count

    @classmethod
    def clear_streams_statistics(cls):
        cls.DATA_STREAMS_STATISTICS = {
            'receivedBytesFromDevices': 0,
            'convertedBytesFromDevice': 0,
            'allReceivedBytesFromTB': 0,
            'allBytesSentToTB': 0,
            'allBytesSentToDevices': 0,
        }

    def run(self) -> None:
        while not self._stopped:
            if time() - self._last_poll >= self._stats_send_period_in_seconds:
                data_to_send = {}
                for attribute in self._config:
                    try:
                        process = subprocess.run(attribute['command'], stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                                 encoding='utf-8', timeout=attribute['timeout'])
                    except Exception as e:
                        self._log.warning("Statistic parameter %s raise the exception: %s",
                                          attribute['attributeOnGateway'], e)
                        continue

                    value = process.stdout

                    data_to_send[attribute['attributeOnGateway']] = value

                self._gateway.tb_client.client.send_attributes(data_to_send)

                if datetime.datetime.now() - self._last_streams_statistics_clear_time >= datetime.timedelta(days=1):
                    self.clear_streams_statistics()

                self._gateway.tb_client.client.send_attributes(StatisticsService.DATA_STREAMS_STATISTICS)

                self._last_poll = time()

            sleep(.2)

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

                func(*args, **kwargs)

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
