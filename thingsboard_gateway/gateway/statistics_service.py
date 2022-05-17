import subprocess
from threading import Thread
from time import time, sleep

import simplejson


class StatisticsService(Thread):
    def __init__(self, config_path, stats_send_period_in_seconds, gateway, log):
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

        self.start()

    def stop(self):
        self._stopped = True

    def _load_config(self):
        with open(self._config_path, 'r') as file:
            return simplejson.load(file)

    def run(self) -> None:
        while not self._stopped:
            if time() - self._last_poll >= self._stats_send_period_in_seconds:
                data_to_send = {}
                for attribute in self._config:
                    process = subprocess.run(attribute['command'], stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                             encoding='utf-8', timeout=attribute['timeout'])

                    if process.returncode != 0:
                        self._log.error("Statistic parameter raise the exception: %s", process.stderr)
                        continue

                    value = process.stdout

                    data_to_send[attribute['attributeOnGateway']] = value

                self._gateway.tb_client.client.send_attributes(data_to_send)

                self._last_poll = time()

            sleep(.2)
