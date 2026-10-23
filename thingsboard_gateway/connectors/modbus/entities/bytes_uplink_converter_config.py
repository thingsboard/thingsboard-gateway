#     Copyright 2026. ThingsBoard
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

from re import fullmatch

from pymodbus.constants import Endian

from thingsboard_gateway.connectors.modbus.utils import Utils


class BytesUplinkConverterConfig:
    def __init__(self, logger, **kwargs):
        self.__log = logger
        self.report_strategy = kwargs.get('reportStrategy')
        self.device_name = kwargs['deviceName']
        self.device_type = kwargs.get('deviceType', 'default')
        self.byte_order = Endian.BIG if kwargs.get('byteOrder', 'LITTLE').upper() == "BIG" else Endian.LITTLE
        self.word_order = Endian.BIG if kwargs.get('wordOrder', 'LITTLE').upper() == "BIG" else Endian.LITTLE
        self.telemetry = kwargs.get('timeseries', [])
        self.attributes = kwargs.get('attributes', [])
        self.unit_id = kwargs['unitId']

        self._validate_config()

    def is_readable(self):
        return len(self.telemetry) > 0 or len(self.attributes) > 0

    def _validate_config(self):
        self._validate_telemetry_and_attrs_config()

    def _validate_telemetry_and_attrs_config(self):
        for conf_section in (self.telemetry, self.attributes):
            for datapoint in conf_section:
                self._validate_datapoint_config(datapoint)

    def _validate_datapoint_config(self, datapoint):
        max_datapoint_per_request = datapoint.get('maxRegistersPerRequest', 16)

        if not isinstance(max_datapoint_per_request, int) or max_datapoint_per_request <= 0:
            self.__log.warning(f"Invalid maxRegistersPerRequest value for datapoint {datapoint.get('tag')}. "
                               f"Setting to default value of 16.")
            datapoint['maxRegistersPerRequest'] = 16

        self._validate_tag_overrides(datapoint)

    def _validate_tag_overrides(self, datapoint):
        if 'tagOverrides' not in datapoint:
            return

        tag_overrides = datapoint['tagOverrides']
        if not isinstance(tag_overrides, dict):
            self.__log.warning("Invalid tagOverrides value for datapoint %s. Expected an object; using tag fallback.",
                               datapoint.get('tag'))
            datapoint['tagOverrides'] = {}
            return

        address_pattern = datapoint.get('address')
        if not Utils.is_wide_range_request(address_pattern):
            self.__log.warning("tagOverrides is supported only for wide-range datapoints. "
                               "Ignoring it for datapoint %s.", datapoint.get('tag'))
            datapoint['tagOverrides'] = {}
            return

        try:
            start_address = Utils.get_start_address(address_pattern)
            end_address = int(address_pattern.split('-', 1)[1])
            objects_count = datapoint.get('objectsCount', 1)
            if not isinstance(objects_count, int) or objects_count <= 0:
                raise ValueError('objectsCount must be a positive integer')
        except (TypeError, ValueError) as error:
            self.__log.warning("Cannot validate tagOverrides for datapoint %s: %s. Using tag fallback.",
                               datapoint.get('tag'), error)
            datapoint['tagOverrides'] = {}
            return

        valid_overrides = {}
        for address, name in tag_overrides.items():
            if not isinstance(address, str) or fullmatch(r'(0|[1-9][0-9]*)', address) is None:
                self.__log.warning("Invalid tagOverrides address %r for datapoint %s. Using tag fallback.",
                                   address, datapoint.get('tag'))
                continue

            numeric_address = int(address)
            if (numeric_address < start_address or numeric_address > end_address or
                    (numeric_address - start_address) % objects_count != 0):
                self.__log.warning("tagOverrides address %s is outside or misaligned with datapoint %s. "
                                   "Using tag fallback.", address, datapoint.get('tag'))
                continue

            if not isinstance(name, str) or not name.strip():
                self.__log.warning("Invalid tagOverrides name at address %s for datapoint %s. "
                                   "Using tag fallback.", address, datapoint.get('tag'))
                continue

            valid_overrides[address] = name

        datapoint['tagOverrides'] = valid_overrides
