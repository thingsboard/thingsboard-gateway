from json import load
from apscheduler.executors.pool import ThreadPoolExecutor, ProcessPoolExecutor
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.events import EVENT_JOB_ERROR
from tb_modbus_server import TBModbusServer
import logging
log = logging.getLogger(__name__)


class TBModbusInitializer:
    _scheduler = None
    _servers = set()

    def __init__(self, config_file="modbus-config.json", number_of_workers=20, number_of_processes=1):
        executors = {'default': ThreadPoolExecutor(number_of_workers)}
        if number_of_processes > 1:
            executors.update({'processpool': ProcessPoolExecutor(number_of_processes)})
        self._scheduler = BackgroundScheduler(executors=executors)
        self._scheduler.add_listener(TBModbusInitializer.listener, EVENT_JOB_ERROR)
        with open(config_file, "r") as config:
            for server in load(config)["servers"]:
                self._servers.add(TBModbusServer(server, self._scheduler))

    def start(self):
        self._scheduler.start()

    @staticmethod
    def listener(event):
        log.exception(event.exception)
