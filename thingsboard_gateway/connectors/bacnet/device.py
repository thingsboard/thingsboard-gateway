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

from asyncio import Lock, sleep
from re import escape, match, fullmatch, compile
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
    ALLOWED_WITH_ROUTERS_ADDRESSES_PATTERN = compile(r'^([^:,@]+)(:[^:,@]+)?(:(47808))?$')
    ALLOWED_LOCAL_ADDRESSES_PATTERN_WITH_DEFAULT_PORT = compile(r'^(0:[^:,@]+|[^\d:,@][^:,@]*:47808|[^:,@]+)$')
    ALLOWED_LOCAL_ADDRESSES_PATTERN_WITH_CUSTOM_PORT = (
        r'^((:(0:)?)[^:,@]+|[^\d:,@][^:,@]*|[^:,@]+)'
        r'(?:\:{expectedPort})?'
        r'(@[^:,@]+(:\d+)?)?$'  # noqa
    )
    ANY_LOCAL_ADDRESS_WITH_PORT_PATTERN = compile(r"\*:\d+")

    def __init__(self, connector_type, config, i_am_request, queue, logger: TbLogger):
        self.__connector_type = connector_type
        self.__config = config
        DeviceObjectConfig.update_address_in_config_util(self.__config)
        self.alternative_responses_addresses = self.__config.get('altResponsesAddresses', [])

        self.__log = logger

        self.__stopped = False
        self.active = True
        self.__request_process_queue = queue

        if Device.need_to_retrieve_device_name(self.__config) and i_am_request.deviceName is None:
            self.__log.warning('Device name is not provided in IAmRequest. Device Id will be used as "objectName')
            i_am_request.deviceName = str(i_am_request.iAmDeviceIdentifier[1])

        self.details = BACnetDeviceDetails(i_am_request)
        self.device_info = DeviceInfo(self.__config.get('deviceInfo', {}), self.details)
        self.uplink_converter_config = UplinkConverterConfig(self.__config, self.device_info, self.details)

        self.name = self.device_info.device_name

        self.__config_poll_period = self.__config.get('pollPeriod', 10000) / 1000
        self.__poll_period = self.__config_poll_period
        self.attributes_updates = self.__config.get('attributeUpdates', [])
        self.server_side_rpc = self.__config.get('serverSideRpc', [])

        self.uplink_converter = self.__load_uplink_converter()

    def __str__(self):
        return f"Device(name={self.name}, address={self.details.address})"

    @property
    def config(self):
        return self.__config

    @property
    def stopped(self):
        return self.__stopped

    @property
    def original_poll_period(self):
        return self.__config_poll_period

    @property
    def poll_period(self):
        return self.__poll_period

    @poll_period.setter
    def poll_period(self, new_poll_period):
        self.__poll_period = new_poll_period

    @config.setter
    def config(self, new_config):
        self.__config = new_config
        self.uplink_converter_config = UplinkConverterConfig(new_config, self.device_info, self.details)
        self.uplink_converter = self.__load_uplink_converter()

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
        self.__request_process_queue.put_nowait(self)
        next_poll_time = monotonic() + self.__poll_period

        while not self.__stopped:
            current_time = monotonic()
            if current_time >= next_poll_time:
                self.__request_process_queue.put_nowait(self)
                next_poll_time = current_time + self.__poll_period

            sleep_time = max(0.0, next_poll_time - current_time)

            await sleep(sleep_time)

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
            return Device.ALLOWED_WITH_ROUTERS_ADDRESSES_PATTERN
        elif pattern == '*':
            return Device.ALLOWED_LOCAL_ADDRESSES_PATTERN_WITH_DEFAULT_PORT
        elif match(Device.ANY_LOCAL_ADDRESS_WITH_PORT_PATTERN, pattern):
            return Device.ALLOWED_WITH_ROUTERS_ADDRESSES_PATTERN
        elif 'X' in pattern:
            port_present = False
            if pattern.count(':') == 1:
                port_present = True
                pattern_for_check = pattern.split(':')[0]
            else:
                pattern_for_check = pattern
            while i < len(pattern_for_check):
                if pattern_for_check[i] == 'X':
                    while i < len(pattern_for_check) and pattern_for_check[i] == 'X':
                        i += 1
                    regex += r'\d+'
                else:
                    regex += escape(pattern_for_check[i])
                    i += 1
            if port_present:
                if pattern.split(':')[1] == '*':
                    regex += r'(:\d+)?'
                else:
                    regex += pattern.split(':')[1]
            return f"^{regex}$"
        elif ':' in pattern:
            if pattern.count(':') == 2:
                return Device.ALLOWED_WITH_ROUTERS_ADDRESSES_PATTERN
            elif pattern.count(':') == 1:
                if pattern.split(':')[1] == '47808':
                    return Device.ALLOWED_LOCAL_ADDRESSES_PATTERN_WITH_DEFAULT_PORT
                else:
                    expected_port = r'\d+' if pattern.split(':')[1] == '*' else pattern.split(':')[1]
                    return Device.ALLOWED_LOCAL_ADDRESSES_PATTERN_WITH_CUSTOM_PORT.replace('{expectedPort}', expected_port)  # noqa

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

    @staticmethod
    def need_to_retrieve_device_name(config):
        if '${objectName}' in config.get('deviceInfo', {}).get('deviceNameExpression', ''):
            return True
        if '${objectName}' in config.get('deviceInfo', {}).get('deviceProfileExpression', ''):
            return True

        return False


class Devices:
    def __init__(self):
        self.__devices = {}
        self.__devices_by_name = {}
        self.__lock = Lock()

    async def add(self, device):
        if not isinstance(device, Device):
            raise TypeError("Expected a Device instance")

        await self.__lock.acquire()
        try:
            self.__devices[device.details.object_id] = device
            self.__devices_by_name[device.name] = device
        finally:
            self.__lock.release()

    async def remove(self, device):
        await self.__lock.acquire()
        try:
            self.__devices.pop(device.details.object_id, None)
            self.__devices.pop(device.name, None)
        finally:
            self.__lock.release()

    async def get_device_by_id(self, id):
        item = None

        await self.__lock.acquire()
        try:
            item = self.__devices.get(id)
        finally:
            self.__lock.release()

        return item

    async def get_device_by_name(self, name):
        item = None

        await self.__lock.acquire()
        try:
            item = self.__devices_by_name.get(name)
        finally:
            self.__lock.release()

        return item

    def stop_all(self):
        for device in self.__devices.values():
            device.stop()
