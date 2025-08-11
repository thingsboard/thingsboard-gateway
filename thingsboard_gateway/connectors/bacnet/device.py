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
from thingsboard_gateway.tb_utility.tb_utility import TBUtility


class Device:
    ALLOWED_WITH_ROUTERS_ADDRESSES_PATTERN = compile(r'^([^:,@]+)(:[^:,@]+)?(:(:47808))?$')
    ALLOWED_LOCAL_ADDRESSES_PATTERN_WITH_DEFAULT_PORT = compile(r'^(0:[^:,@]+|[^\d:,@][^:,@]*:47808|[^:,@]+)$')
    ALLOWED_LOCAL_ADDRESSES_PATTERN_WITH_CUSTOM_PORT = (
        r'^((:(0:)?)[^:,@]+|[^\d:,@][^:,@]*|[^:,@]+)'
        r'\:{expectedPort}'
        r'(@[^:,@]+(:\d+)?)?$'
    )
    ANY_LOCAL_ADDRESS_WITH_PORT_PATTERN = compile(r"\*:\d+")
    ROUTER_INFO_PARAMS = [
        '${routerName}',
        '${routerId}',
        '${routerVendorId}',
        '${routerAddress}'
    ]

    def __init__(self, connector_type, config, i_am_request, reading_queue, rescan_queue, logger, converter_logger):
        self.__connector_type = connector_type
        self.__config = config
        DeviceObjectConfig.update_address_in_config_util(self.__config)
        self.alternative_responses_addresses = self.__config.get('altResponsesAddresses', [])

        self.__log = logger
        self.__converter_log = converter_logger

        self.__stopped = False
        self.active = True

        self.__request_process_queue = reading_queue
        self.__rescan_process_queue = rescan_queue

        if Device.need_to_retrieve_device_name(self.__config) and i_am_request.deviceName is None:
            self.__log.warning('Device name is not provided in IAmRequest. Device Id will be used as "objectName')
            i_am_request.deviceName = str(i_am_request.iAmDeviceIdentifier[1])

        self.details = BACnetDeviceDetails(i_am_request)
        self.device_info = DeviceInfo(self.__config.get('deviceInfo', {}), self.details)
        self.uplink_converter_config = UplinkConverterConfig(self.__config, self.device_info, self.details)

        self.name = self.device_info.device_name

        self.__objects_rescan_period = self.__config.get('devicesRescanObjectsPeriodSeconds', 60)
        self.rescan_objects_config = []

        self.__config_poll_period = self.__config.get('pollPeriod', 10000) / 1000
        self.__poll_period = self.__config_poll_period
        self.attributes_updates = self.__config.get('attributeUpdates', [])
        self.shared_attributes_keys = self.__get_shared_attributes_keys()
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

    def __get_shared_attributes_keys(self):
        result = []

        for attr_config in self.attributes_updates:
            result.append(attr_config['key'])

        return result

    def __load_uplink_converter(self):
        try:
            if self.__config.get(UPLINK_PREFIX + CONVERTER_PARAMETER) is not None:
                converter_class = TBModuleLoader.import_module(self.__connector_type,
                                                               self.__config[UPLINK_PREFIX + CONVERTER_PARAMETER])
                converter = converter_class(self.uplink_converter_config, self.__converter_log)
            else:
                converter = AsyncBACnetUplinkConverter(self.uplink_converter_config, self.__converter_log)

            return converter
        except Exception as e:
            self.__log.exception('Failed to load uplink converter for % device: %s', self.name, e)

    def stop(self):
        self.active = False
        self.__stopped = True

    async def run(self):
        if len(self.uplink_converter_config.objects_to_read) > 0:
            self.__request_process_queue.put_nowait(self)

        next_poll_time = monotonic() + self.__poll_period

        while not self.__stopped:
            current_time = monotonic()
            if current_time >= next_poll_time:
                if len(self.uplink_converter_config.objects_to_read) > 0:
                    self.__request_process_queue.put_nowait(self)

                next_poll_time = current_time + self.__poll_period

            sleep_time = max(0.0, next_poll_time - current_time)

            await sleep(sleep_time)

    async def rescan(self):
        next_rescan_time = monotonic() + self.__objects_rescan_period

        while not self.__stopped:
            current_time = monotonic()
            if current_time >= next_rescan_time:
                if self.need_to_rescan():
                    self.__rescan_process_queue.put_nowait(self)

                next_rescan_time = current_time + self.__objects_rescan_period

            sleep_time = max(0.0, next_rescan_time - current_time)
            await sleep(sleep_time)

    @staticmethod
    def find_self_in_config(devices_config, apdu):
        apdu_address = apdu.pduSource.__str__()
        device_identifier = apdu.iAmDeviceIdentifier[-1]

        for device_config in devices_config:
            if device_config.get('deviceId') is not None:
                if Device.is_device_identifier_match(device_identifier, device_config.get('deviceId')):
                    return device_config
                else:
                    continue

            if Device.is_address_match(apdu_address, device_config.get('address')):
                return device_config
            elif apdu_address in device_config.get('altResponsesAddresses', []):
                return device_config

    @staticmethod
    def is_device_identifier_match(device_identifier, pattern):
        """
        Check if the device identifier matches the given pattern.
        :param device_identifier: The device identifier to check.
        :param pattern ("deviceId" in configuration file in device section):
        The pattern to match against. Can be a string, list, int or "*":
        - Asterisk (`*`) to match any device identifier.
        - A list of device identifiers to match against.
        - A string pattern that can include wildcards or regex patterns.
        - An integer to match a specific device identifier.
        """

        if pattern is not None:
            if pattern == '*':
                return True

            if isinstance(pattern, str):
                start, end = Device.__parse_range(pattern)
                return start <= device_identifier < end

            if isinstance(pattern, list):
                for rng in pattern:
                    start, end = Device.__parse_range(str(rng))
                    if start <= device_identifier < end:
                        return True

            return match(str(pattern), str(device_identifier)) is not None

        return False

    @staticmethod
    def is_address_match(address, pattern):
        regex = Device.get_address_regex(pattern)
        return match(regex, address) is not None

    @staticmethod
    def get_address_regex(initial_config_address):
        i = 0
        pattern = initial_config_address.split('@')[0]
        if pattern == '*:*:*':
            return r'.*'
        elif pattern == '*:*':
            return Device.ALLOWED_WITH_ROUTERS_ADDRESSES_PATTERN
        elif pattern == '*':
            return Device.ALLOWED_LOCAL_ADDRESSES_PATTERN_WITH_DEFAULT_PORT
        elif 'X' in pattern:
            port_present = False
            if pattern.count(':') == 1:
                port_present = True
                pattern_for_check = pattern.split(':')[0]
            else:
                pattern_for_check = pattern
            regex = ''
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
                    if pattern.split(':')[1] == '47808':
                        regex += r'(?::47808)?'
                    else:
                        regex += r':'
                        regex += pattern.split(':')[1]
            return f"^{regex}$"
        elif ':' in pattern and '*' in pattern:
            if pattern.count(':') == 2:
                return Device.ALLOWED_WITH_ROUTERS_ADDRESSES_PATTERN
            elif pattern.count(':') == 1:
                if pattern.split(':')[1] == '47808':
                    return Device.ALLOWED_LOCAL_ADDRESSES_PATTERN_WITH_DEFAULT_PORT
                else:
                    expected_port = r'\d+' if pattern.split(':')[1] == '*' else pattern.split(':')[1]
                    return Device.ALLOWED_LOCAL_ADDRESSES_PATTERN_WITH_CUSTOM_PORT.replace('{expectedPort}', expected_port)  # noqa
        elif ':' in pattern:
            splitted = pattern.split(':')
            if len(splitted) > 1 and splitted[1] == "47808":
                return f"^{splitted[0]}(?::47808)?$"

        return f"^{pattern}$"

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
    def is_global_discovery_config(config):
        fill_for = []

        if config.get('attributes', '') == '*':
            fill_for.append('attributes')

        if config.get('timeseries', '') == '*':
            fill_for.append('timeseries')

        return fill_for

    @staticmethod
    def is_local_discovery_config(config):
        fill_for = []

        for section in ('attributes', 'timeseries'):
            for item_config in config.get(section, []):
                if isinstance(item_config, dict) and item_config['objectId'] == '*':
                    item_config['type'] = section
                    fill_for.append(item_config)

        return fill_for

    @staticmethod
    def need_to_retrieve_device_name(config):
        if '${objectName}' in config.get('deviceInfo', {}).get('deviceNameExpression', ''):
            return True
        if '${objectName}' in config.get('deviceInfo', {}).get('deviceProfileExpression', ''):
            return True

        return False

    @staticmethod
    def need_to_retrieve_router_info(config):
        for param in Device.ROUTER_INFO_PARAMS:
            if param in config.get('deviceInfo', {}).get('deviceNameExpression', ''):
                return True
            if param in config.get('deviceInfo', {}).get('deviceProfileExpression', ''):
                return True

        return False

    @staticmethod
    def parse_config_key(config):
        if '${' in config['key']:
            result_tags = set()
            result_tags.update(TBUtility.get_values(config['key'], {}, get_tag=True))
            if len(result_tags) > 0:
                if config['propertyId'] != '*':
                    if isinstance(config['propertyId'], list):
                        result_tags.update(config['propertyId'])
                        config['propertyId'] = result_tags
                    elif isinstance(config['propertyId'], str):
                        result_tags.add(config['propertyId'])
                        config['propertyId'] = result_tags

    @staticmethod
    def parse_ranges(object_id_config):
        ranges = []

        if isinstance(object_id_config, list):
            for rng in object_id_config:
                start, end = Device.__parse_range(rng)
                ranges.append((start, end))
        elif isinstance(object_id_config, str):
            if object_id_config != '*':
                start, end = Device.__parse_range(object_id_config)
                ranges.append((start, end))

        return ranges

    @staticmethod
    def __parse_range(rng):
        rng = rng.strip()
        if '-' in rng:
            start, end = map(str.strip, rng.split('-'))
            return int(start), int(end) + 1
        else:
            return int(rng), int(rng) + 1

    def need_to_rescan(self):
        return len(self.details.failed_to_read_indexes) > 0 and not self.details.is_segmentation_supported()


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

    async def get_device_by_id(self, _id):
        await self.__lock.acquire()
        try:
            item = self.__devices.get(_id)
        finally:
            self.__lock.release()

        return item

    async def get_device_by_name(self, name):
        await self.__lock.acquire()
        try:
            item = self.__devices_by_name.get(name)
        finally:
            self.__lock.release()

        return item

    def stop_all(self):
        for device in self.__devices.values():
            device.stop()
