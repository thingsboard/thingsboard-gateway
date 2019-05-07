from json import load
from apscheduler.executors.pool import ThreadPoolExecutor, ProcessPoolExecutor
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.events import EVENT_JOB_ERROR
from tb_modbus_server import TBModbusServer
import logging
log = logging.getLogger(__name__)


class TBModbusInitializer:
    _scheduler = None
    dict_devices_servers = {}

    def __init__(self,
                 gateway,
                 # todo make use of extension id later
                 extension_id,
                 config_file="modbus-config.json",
                 number_of_workers=20,
                 number_of_processes=1,
                 start_immediately=True):
        self.ext_id = extension_id
        executors = {'default': ThreadPoolExecutor(number_of_workers)}
        if number_of_processes > 1:
            executors.update({'processpool': ProcessPoolExecutor(number_of_processes)})
        self._scheduler = BackgroundScheduler(executors=executors)
        self._scheduler.add_listener(TBModbusInitializer.listener, EVENT_JOB_ERROR)
        with open(config_file, "r") as config:
            for server_config in load(config)["servers"]:
                server = TBModbusServer(server_config, self._scheduler, gateway, self.ext_id)
                # todo should we make it a future?
                self.dict_devices_servers.update({device_name: server for device_name in server.devices_names})
        if start_immediately:
            self.start()

    def write_to_device(self, config):
        result = None
        log.debug("config")
        try:
            # todo check if this works irl
            result = self.dict_devices_servers[config["deviceName"]].write_to_device(config)
        except KeyError:
            # todo is it enough to log device name error?
            log.error("There is not device with name {name}, extension {ext_id}".format(name=config["deviceName"],
                                                                                        ext_id=self.ext_id))
        # todo repeate java code logic
        # todo should we return Exception?
        return result

    def start(self):
        self._scheduler.start()

    @staticmethod
    def listener(event):
        log.exception(event.exception)
