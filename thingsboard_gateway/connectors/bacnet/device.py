#     Copyright 2024. ThingsBoard
#
#     Licensed under the Apache License, Version 2.0 (the "License");
#     you may not use this file except in compliance with the License.
#     You may obtain a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#     Unless required by applicable law or agreed to in writing, software
#     distributed under the License is distributed on an "AS IS" BASIS,
#     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#     See the License for the specific language governing permissions and
#     limitations under the License.

import time
from threading import Thread

from bacpypes3.primitivedata import ObjectIdentifier

from thingsboard_gateway.connectors.bacnet.bacnet_uplink_converter import AsyncBACnetUplinkConverter
from thingsboard_gateway.connectors.bacnet.entities.bacnet_device_details import BACnetDeviceDetails
from thingsboard_gateway.connectors.bacnet.entities.device_info import DeviceInfo
from thingsboard_gateway.connectors.bacnet.entities.uplink_converter_config import UplinkConverterConfig
from thingsboard_gateway.gateway.constants import UPLINK_PREFIX, CONVERTER_PARAMETER
from thingsboard_gateway.tb_utility.tb_loader import TBModuleLoader


class Device(Thread):
    def __init__(self, connector_type, config, i_am_request, callback, logger):
        super().__init__()

        self.__connector_type = connector_type
        self.__config = config

        self.__log = logger

        self.__stopped = False
        self.active = True
        self.callback = callback
        self.daemon = True

        self.details = BACnetDeviceDetails(i_am_request)
        self.device_info = DeviceInfo(self.__config.get('deviceInfo', {}), self.details)
        self.uplink_converter_config = UplinkConverterConfig(self.__config, self.device_info, self.details)

        self.name = self.device_info.device_name

        self.__poll_period = self.__config.get('pollPeriod', 10000) / 1000
        self.attributes_updates = self.__config.get('attributeUpdates', [])
        self.server_side_rpc = self.__config.get('serverSideRpc', [])

        self.uplink_converter = self.__load_uplink_converter()

        self.__last_poll_time = 0

        self.start()

    def __str__(self):
        return f"Device(name={self.name}, address={self.details.address})"

    def __load_uplink_converter(self):
        try:
            if self.__config.get(UPLINK_PREFIX + CONVERTER_PARAMETER) is not None:
                converter_class = TBModuleLoader.import_module(self.__connector_type,
                                                               self.__config[UPLINK_PREFIX + CONVERTER_PARAMETER])
                converter = converter_class(self.uplink_converter_config, self.__log)
            else:
                converter = AsyncBACnetUplinkConverter(self.uplink_converter_config, self.__log)

            return converter
        except Exception as e:
            self.__log.exception('Failed to load uplink converter for % slave: %s', self.name, e)

    def stop(self):
        self.active = False
        self.__stopped = True

    def run(self):
        while not self.__stopped:
            if time.monotonic() - self.__last_poll_time >= self.__poll_period and self.active:
                self.__send_callback()
                self.__last_poll_time = time.monotonic()

            time.sleep(.01)

    def __send_callback(self):
        try:
            self.callback(self)
        except Exception as e:
            self.__log.error('Error sending callback from device %s: %s', self, e)

    @staticmethod
    def find_self_in_config(devices_config, apdu):
        device_config = list(filter(lambda x: x['address'] == apdu.pduSource.exploded, devices_config))
        if len(device_config):
            return device_config[0]

    @staticmethod
    def get_object_id(config):
        return ObjectIdentifier("%s:%s" % (config['objectType'], config['objectId']))
