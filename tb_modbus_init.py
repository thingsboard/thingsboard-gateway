from json import load
from apscheduler.executors.pool import ThreadPoolExecutor, ProcessPoolExecutor
from apscheduler.schedulers.background import BackgroundScheduler
from tb_modbus_server import TBModbusServer


class TBModbusInitializer:
    _scheduler = None
    _servers = set()

    def __init__(self, config_file="modbus-config.json", number_of_workers=20, number_of_processes=1):
        executors = {'default': ThreadPoolExecutor(number_of_workers)}
        if number_of_processes > 1:
            executors.update({'processpool': ProcessPoolExecutor(number_of_processes)})
        self._scheduler = BackgroundScheduler(executors=executors)
        with open(config_file, "r") as config:
            for server in load(config)["servers"]:
                self._servers.add(TBModbusServer(server, self._scheduler))

    def start(self):
        self._scheduler.start()

    def stop_server(self, server):
        # todo add
        result = True
        return result
