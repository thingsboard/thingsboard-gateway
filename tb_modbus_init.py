from json import load
from apscheduler.executors.pool import ThreadPoolExecutor, ProcessPoolExecutor
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.events import EVENT_JOB_ERROR
from tb_modbus_server import TBModbusServer
import logging
log = logging.getLogger(__name__)


class TBModbusInitializer:
    _scheduler = None
    _dict_devices_servers = {}

    def __init__(self,
                 config_file="modbus-config.json",
                 number_of_workers=20,
                 number_of_processes=1,
                 start_immediately=True):
        executors = {'default': ThreadPoolExecutor(number_of_workers)}
        if number_of_processes > 1:
            executors.update({'processpool': ProcessPoolExecutor(number_of_processes)})
        self._scheduler = BackgroundScheduler(executors=executors)
        self._scheduler.add_listener(TBModbusInitializer.listener, EVENT_JOB_ERROR)
        with open(config_file, "r") as config:
            for server in load(config)["servers"]:
                server = TBModbusServer(server, self._scheduler)
                # todo should we make it a future?
                self._dict_devices_servers.update({device_name: server for device_name in server.devices_names})
        if start_immediately:
            self.start()

    def write_to_device(self, config):
        log.debug("config")
        try:
            result = self._dict_devices_servers[config["deviceName"]].write_to_device(config)
        except KeyError:
            log.debug("There is not device with name {name}".format(name=config["deviceName"]))
        # if there is not such device log error, look at java code
        # todo repeate java code logic
        # if there is such device, call corresponding server
        return result

    def start(self):
        self._scheduler.start()

    @staticmethod
    def listener(event):
        log.exception(event.exception)
