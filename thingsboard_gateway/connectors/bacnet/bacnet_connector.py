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
from threading import Thread
from time import time, sleep
from string import ascii_lowercase
from bacpypes.core import run, stop
from thingsboard_gateway.connectors.connector import Connector, log
from thingsboard_gateway.connectors.bacnet.bacnet_uplink_converter import BACnetUplinkConverter
from thingsboard_gateway.connectors.bacnet.bacnet_utilities.tb_gateway_bacnet_application import TBBACnetApplication
from thingsboard_gateway.tb_utility.tb_utility import TBUtility


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
        self.__send_whois_broadcast = self.__config["general"].get("sendWhoIsBroadcast", False)
        self.__send_whois_broadcast_period = self.__config.get("sendWhoIsBroadcastPeriod", 5000)
        self.__send_whois_broadcast_previous_time = 0
        self.__connected = False
        self.daemon = True

    def open(self):
        self.__stopped = False
        for device in self.__devices:
            try:
                converter_object = TBUtility.check_and_import(self.__connector_type, device.get("class", "BACnetUplinkConverter"))
                device["converter"] = converter_object(device)
            except Exception as e:
                log.exception(e)
        self.start()

    def run(self):
        self.__connected = True
        while not self.__stopped:
            cur_time = time()*1000
            if self.__send_whois_broadcast and cur_time - self.__send_whois_broadcast_previous_time >= self.__send_whois_broadcast_period:
                self.__send_whois_broadcast_previous_time = cur_time
                self._application.do_whois()
                log.debug("WhoIsRequest has been sent.")
            for device in self.__devices:
                try:
                    if device.get("previous_check") is None or cur_time - device["previous_check"] >= self.__poll_period:
                        if self._application.check_or_add(device):
                            for mapping_type in ["attributes", "timeseries"]:
                                for mapping_object in device[mapping_type]:
                                    data_to_application = {
                                        "device": device,
                                        "mapping_type": mapping_type,
                                        "mapping_object": mapping_object,
                                        "callback": self.__bacnet_device_mapping_response_cb
                                    }
                                    self._application.do_read(**data_to_application)
                        device["previous_check"] = cur_time
                    else:
                        sleep(.1)
                except Exception as e:
                    log.exception(e)

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

    def __bacnet_device_mapping_response_cb(self, converter, mapping_type, config, value):
        log.debug(value)
        log.debug(config)
        log.debug(converter)
        converted_data = {}
        try:
            converted_data = converter.convert((mapping_type, config), value)
        except Exception as e:
            log.exception(e)
        self.__gateway.send_to_storage(self.name, converted_data)
