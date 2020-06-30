#      Copyright 2020. ThingsBoard
#  #
#      Licensed under the Apache License, Version 2.0 (the "License");
#      you may not use this file except in compliance with the License.
#      You may obtain a copy of the License at
#  #
#          http://www.apache.org/licenses/LICENSE-2.0
#  #
#      Unless required by applicable law or agreed to in writing, software
#      distributed under the License is distributed on an "AS IS" BASIS,
#      WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#      See the License for the specific language governing permissions and
#      limitations under the License.

from threading import Thread
from time import sleep, time
from random import choice
from string import ascii_lowercase

from thingsboard_gateway.connectors.connector import Connector, log
from thingsboard_gateway.tb_utility.tb_utility import TBUtility

try:
    import puresnmp
except ImportError:
    TBUtility.install_package("puresnmp")
    import puresnmp


class SNMPConnector(Connector, Thread):
    def __init__(self, gateway, config, connector_type):
        super().__init__()
        self.daemon = True
        self.__gateway = gateway
        self._connected = False
        self.__stopped = False
        self._connector_type = connector_type
        self.__config = config
        self.__devices = self.__config["devices"]
        self.setName(config.get("name", 'SNMP Connector ' + ''.join(choice(ascii_lowercase) for _ in range(5))))
        self.statistics = {'MessagesReceived': 0,
                           'MessagesSent': 0}
        self._default_converters = {
            "uplink": "SNMPUplinkConverter",
            "downlink": "SNMPDownlinkConverter"
        }
        self.__methods = {
            "get": puresnmp.get,
            "set": puresnmp.set,
            "walk": puresnmp.walk,
            "table": puresnmp.table,
            "bulkGet": puresnmp.bulkget,
            "bulkWalk": puresnmp.bulkwalk,
            "bulkTable": puresnmp.bulktable,
        }

    def open(self):
        self.__stopped = False
        self.start()

    def run(self):
        self._connected = True
        try:
            while not self.__stopped:
                current_time = time()*1000
                for device in self.__devices:
                    if device.get("previous_poll_time", 0) + device.get("pollPeriod", 10000) < current_time:

                        device["previous_poll_time"] = current_time
                if self.__stopped:
                    break
                else:
                    sleep(.01)
        except Exception as e:
            log.exception(e)

    def close(self):
        self.__stopped = True
        self._connected = False

    def get_name(self):
        return self.name

    def is_connected(self):
        return self._connected

    def on_attributes_update(self, content):
        log.debug(content)

    def server_side_rpc_handler(self, content):
        log.debug(content)

    def collect_statistic_and_send(self, connector_name, data):
        self.statistics["MessagesReceived"] = self.statistics["MessagesReceived"] + 1
        self.__gateway.send_to_storage(connector_name, data)
        self.statistics["MessagesSent"] = self.statistics["MessagesSent"] + 1
