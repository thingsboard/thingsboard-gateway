import logging
from json import load

from tb_extension_modbus import TBModbusServer

log = logging.getLogger(__name__)


class TBModbusInitializer:
    _scheduler = None
    dict_devices_servers = {}

    def __init__(self,
                 gateway,
                 extension_id,
                 scheduler,
                 config_file="modbus-config.json"):
        self.ext_id = extension_id
        self._scheduler = scheduler
        with open(config_file, "r") as config:
            for server_config in load(config)["servers"]:
                TBModbusServer(server_config, self._scheduler, gateway, self.ext_id)
