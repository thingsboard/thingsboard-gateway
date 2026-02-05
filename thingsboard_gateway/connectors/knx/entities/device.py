#      Copyright 2026. ThingsBoard
#
#      Licensed under the Apache License, Version 2.0 (the "License");
#      you may not use this file except in compliance with the License.
#      You may obtain a copy of the License at
#
#          http://www.apache.org/licenses/LICENSE-2.0
#
#      Unless required by applicable law or agreed to in writing, software
#      distributed under the License is distributed on an "AS IS" BASIS,
#      WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#      See the License for the specific language governing permissions and
#      limitations under the License.

from threading import Thread
from time import monotonic, sleep

from thingsboard_gateway.connectors.knx.knx_uplink_converter import KNXUplinkConverter
from thingsboard_gateway.gateway.constants import CONVERTER_PARAMETER, UPLINK_PREFIX
from thingsboard_gateway.tb_utility.tb_loader import TBModuleLoader
from thingsboard_gateway.tb_utility.tb_utility import TBUtility


class Device(Thread):
    def __init__(self, connector_type, is_client_connected, config, process_request_queue, logger):
        super().__init__()

        self.__log = logger
        self.__connector_type = connector_type
        self.__is_client_connected = is_client_connected
        self.__config = config

        self.group_addresses_to_read = {}
        self.__fill_group_addresses_to_read()

        device_info = self.__config['deviceInfo']
        self.name = device_info.get('deviceNameExpression', 'KNX Device')
        self.daemon = True
        self.__stopped = False

        self.uplink_converter = self.__load_uplink_converter()

        self.__process_request_queue = process_request_queue

        self.__poll_period = self.__config.get('pollPeriod', 10000) / 1000

        self.__last_poll_time = 0

        self.start()

    def __str__(self):
        return f'Device({self.name})'

    def __fill_group_addresses_to_read(self):
        """
        Fill self.group_addresses_to_read with all group addresses
        to read from the device from:
        - attributes
        - temeseries
        - deviceNameExpression
        - deviceTypeExpression

        Data have the following structure:
        {
            "1.0.5": {
                "type": "string",
                "keys": ["vendor", "deviceName"]
            }
        }
        """

        self.__fill_group_addresses_from_device_info()

        for section in ('attributes', 'timeseries'):
            for datapoint_config in self.__config.get(section, []):
                self.add_group_address(datapoint_config['groupAddress'],
                                       datapoint_config.get('type'),
                                       datapoint_config['key'])

    def add_group_address(self, group_address, datatype, key):
        if not self.group_addresses_to_read.get(group_address):
            self.group_addresses_to_read[group_address] = {
                'type': datatype,
                'keys': []
            }

        self.group_addresses_to_read[group_address]['keys'].append(key)

    def __fill_group_addresses_from_device_info(self):
        device_info = self.__config['deviceInfo']

        device_name_group_address = self.__get_group_address_from_device_name(device_info)
        if device_name_group_address:
            self.add_group_address(device_name_group_address,
                                   device_info.get('deviceNameDataType'),
                                   'deviceName')

        device_type_group_address = self.__get_group_address_from_device_profile(device_info)
        if device_type_group_address:
            self.add_group_address(device_type_group_address,
                                   device_info.get('deviceProfileDataType'),
                                   'deviceType')

    @staticmethod
    def __get_group_address_from_device_name(device_info):
        if device_info.get('deviceNameExpressionSource') == 'expression':
            if '${' in device_info['deviceNameExpression']:
                expression = Device.__get_group_address_from_expression(device_info['deviceNameExpression'])

                return expression

    @staticmethod
    def __get_group_address_from_device_profile(device_info):
        if device_info.get('deviceProfileExpressionSource') == 'expression':
            if '${' in device_info['deviceProfileExpression']:
                expression = Device.__get_group_address_from_expression(device_info['deviceProfileExpression'])

                return expression

    @staticmethod
    def __get_group_address_from_expression(expression):
        return TBUtility.get_value(expression, get_tag=True)

    def __load_uplink_converter(self):
        try:
            if self.__config.get(UPLINK_PREFIX + CONVERTER_PARAMETER) is not None:
                converter_class = TBModuleLoader.import_module(self.__connector_type,
                                                               self.__config[UPLINK_PREFIX + CONVERTER_PARAMETER])
                converter = converter_class(self.uplink_converter_config, self.__log)
            else:
                converter = KNXUplinkConverter(self.__config, self.__log)

            return converter
        except Exception as e:
            self.__log.error('Failed to load uplink converter for % device: %s', self.name, e)

    def run(self):
        while not self.__stopped:
            if monotonic() - self.__last_poll_time > self.__poll_period and self.__is_client_connected.is_set():
                self.__put_to_queue()
                self.__last_poll_time = monotonic()
            else:
                sleep(.01)

    def stop(self):
        self.__stopped = True

    def __put_to_queue(self):
        try:
            self.__process_request_queue.put(self)
        except Exception as e:
            self.__log.exception(e)
