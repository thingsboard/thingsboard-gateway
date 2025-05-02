#     Copyright 2025. ThingsBoard
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

from asyncio import sleep
from re import escape, match, fullmatch
from time import monotonic

from bacpypes3.primitivedata import ObjectIdentifier

from thingsboard_gateway.connectors.bacnet.bacnet_uplink_converter import AsyncBACnetUplinkConverter
from thingsboard_gateway.connectors.bacnet.entities.bacnet_device_details import BACnetDeviceDetails
from thingsboard_gateway.connectors.bacnet.entities.device_info import DeviceInfo
from thingsboard_gateway.connectors.bacnet.entities.device_object_config import DeviceObjectConfig
from thingsboard_gateway.connectors.bacnet.entities.uplink_converter_config import UplinkConverterConfig
from thingsboard_gateway.gateway.constants import UPLINK_PREFIX, CONVERTER_PARAMETER
from thingsboard_gateway.tb_utility.tb_loader import TBModuleLoader
from thingsboard_gateway.tb_utility.tb_logger import TbLogger


class Device:
    def __init__(self, connector_type, config, i_am_request, callback, logger: TbLogger):
        self.__connector_type = connector_type
        self.__config = config
        DeviceObjectConfig.update_address_in_config_util(self.__config)
        self.alternative_responses_addresses = self.__config.get('altResponsesAddresses', [])

        self.__log = logger

        self.__stopped = False
        self.active = True
        self.callback = callback

        if not hasattr(i_am_request, 'deviceName'):
            self.__log.warning('Device name is not provided in IAmRequest. Device Id will be used as "objectName')
            i_am_request.deviceName = str(i_am_request.iAmDeviceIdentifier[1])
        self.details = BACnetDeviceDetails(i_am_request)
        self.device_info = DeviceInfo(self.__config.get('deviceInfo', {}), self.details)
        self.uplink_converter_config = UplinkConverterConfig(self.__config, self.device_info, self.details)

        self.name = self.device_info.device_name

        self.__poll_period = self.__config.get('pollPeriod', 10000) / 1000
        self.attributes_updates = self.__config.get('attributeUpdates', [])
        self.server_side_rpc = self.__config.get('serverSideRpc', [])

        self.uplink_converter = self.__load_uplink_converter()

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
            self.__log.exception('Failed to load uplink converter for % device: %s', self.name, e)

    def stop(self):
        self.active = False
        self.__stopped = True

    async def run(self):
        self.__send_callback()
        next_poll_time = monotonic() + self.__poll_period

        while not self.__stopped:
            current_time = monotonic()
            if current_time >= next_poll_time:
                self.__send_callback()
                next_poll_time = current_time + self.__poll_period

            sleep_time = max(0.0, next_poll_time - monotonic())

            await sleep(sleep_time)

    def __send_callback(self):
        try:
            self.callback(self)
        except Exception as e:
            self.__log.error('Error sending callback from device %s: %s', self, e)

    @staticmethod
    def find_self_in_config(devices_config, apdu):
        apdu_address = apdu.pduSource.__str__()

        for device_config in devices_config:
            if Device.is_address_match(apdu_address, device_config.get('address')):
                return device_config
            elif apdu_address in device_config.get('altResponsesAddresses', []):
                return device_config

    @staticmethod
    def is_address_match(address, pattern):
        regex = Device.get_address_regex(pattern)
        return match(regex, address) is not None

    @staticmethod
    def get_address_regex(initial_config_address):
        regex = ''
        i = 0
        pattern = initial_config_address.split('@')[0]
        if pattern == '*:*:*':
            return r'.*'
        elif pattern == '*:*':
            return r'^([^:,@]+)(:[^:,@]+)?(:(47808))?$'
        elif pattern == '*':
            return r'^(0:[^:,@]+|[^\d:,@][^:,@]*:47808|[^:,@]+)$'
        while i < len(pattern):
            if pattern[i] == 'X':
                while i < len(pattern) and pattern[i] == 'X':
                    i += 1
                regex += r'\d+'
            else:
                regex += escape(pattern[i])
                i += 1

        return f"^{regex}$"

    @staticmethod
    def get_who_is_address(address):
        if 'X' in address or '*' in address:
            return Device.__get_broadcast_address(address)
        else:
            return address

    @staticmethod
    def __get_broadcast_address(address):
        parts = address.split(":")

        if len(parts) == 3:
            network = parts[0]
            return Device.__match_network(network)

        elif len(parts) == 2:
            if fullmatch(r"[0-9X]+", parts[0]):
                network = parts[0]
                return Device.__match_network(network)
            else:
                return "*:*"

        elif len(parts) == 1:
            return "*"

        return None

    @staticmethod
    def __match_network(network: str):
        if fullmatch(r"X+", network):
            return "*:*"
        else:
            return f"{network}:*"

    @staticmethod
    def is_pattern_address(address):
        return "X" in address

    @staticmethod
    def get_object_id(config):
        return ObjectIdentifier("%s:%s" % (config['objectType'], config['objectId']))

    @staticmethod
    def is_discovery_config(config):
        fill_for = []

        if config.get('attributes', '') == '*':
            fill_for.append('attributes')

        if config.get('timeseries', '') == '*':
            fill_for.append('timeseries')

        return fill_for
