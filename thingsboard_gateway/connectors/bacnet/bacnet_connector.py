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

from random import choice
from string import ascii_lowercase
from thingsboard_gateway.connectors.connector import Connector, log
from thingsboard_gateway.connectors.bacnet.bacnet_converter import BACnetUplinkConverter
from thingsboard_gateway.connectors.bacnet.bacnet_utilities.tb_gateway_bacnet_application import TBBACnetApplication
from thingsboard_gateway.tb_utility.tb_utility import TBUtility
from threading import Thread
from bacpypes.core import run, stop
from time import time, sleep


class BACnetConnector(Thread, Connector):
    def __init__(self, gateway, config, connector_type):
        self.__connector_type = connector_type
        self.statistics = {'MessagesReceived': 0,
                           'MessagesSent': 0}
        super().__init__()
        self.__config = config
        self.setName(config.get('name', 'BACnet ' + ''.join(choice(ascii_lowercase) for _ in range(5))))
        self.__gateway = gateway
        self._application = TBBACnetApplication(self.__config)
        self.__bacnet_core_thread = Thread(target=run, name="BACnet core thread")
        self.__bacnet_core_thread.start()
        self.__stopped = False
        self.__poll_period = self.__config["general"].get("pollPeriod", 5000)
        self.__devices = self.__config["devices"]
        self.__connected = False
        self.daemon = True

    def open(self):
        self.__stopped = False
        self.start()

    def run(self):
        self.__connected = True
        while not self.__stopped:
            cur_time = time()*1000
            for device in self.__devices:
                if cur_time - self.__devices[device["name"]] >= self.__poll_period:
                    device_address = device["address"]
                    device_object_identifier = device["objectIdentifier"]
                    device_name = TBUtility.get_value(device["name"], expression_instead_none=True)
                    for mapping_object in device["mapping"]:
                        converter = TBUtility.check_and_import(self.__connector_type, )
                        data_to_thingsboard = self._application.do_read("192.168.0.4", "analogValue:1", "presentValue")
                        self.__devices["device"]["name"] = cur_time
                else:
                    sleep(.01)

    def close(self):
        self.__stopped = True
        self.__connected = False
        stop()

    def get_name(self):
        return self.name

    def is_connected(self):
        return self.__connected

    def on_attributes_update(self, content):
        pass

    def server_side_rpc_handler(self, content):
        pass
