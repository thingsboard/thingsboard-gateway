from json import load

from tb_modbus_server import TBModbusServer
import logging
log = logging.getLogger(__name__)


class TBModbusInitializer:
    _scheduler = None
    dict_devices_servers = {}

    def __init__(self,
                 gateway,
                 extension_id,
                 scheduler,
                 config_file="modbus-config.json",
                 start_immediately=True):
        self.ext_id = extension_id
        self._scheduler = scheduler
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
            result = self.dict_devices_servers[config["deviceName"]].write_to_device(config)
        except KeyError:
            log.error("There is not device with name {name}, extension {ext_id}".format(name=config["deviceName"],
                                                                                        ext_id=self.ext_id))
        # todo should we return Exception?
        return result

    def start(self):
        self._scheduler.start()
