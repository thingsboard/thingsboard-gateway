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


from math import ceil
from struct import pack, unpack

from thingsboard_gateway.connectors.converter import Converter


class SerialDownlinkConverter(Converter):
    """
    Downlink converter is used to convert data that can be sent to device.
    Such as we create downlink converter for each configured device,
    this converter is used to convert data that can be sent to only one device.
    Because config, that we passed to init method, is device specific.
    If your connector can handle multiple devices, you can create one converter for all devices.
    """

    def __init__(self, config, logger):
        self._log = logger
        self.__config = config

    def convert(self, config, data) -> bytes:
        """Method to convert data that can be send to serial port."""
        self._log.debug("Data to convert: %s", data)
        byteorder = self.__config.get('byteorder', 'big').lower()
        if data is None:
            return None
        type_ = config.get("type")
        if type_ == "int":
            length = ceil(data.bit_length() / 8)
            return data.to_bytes(length, byteorder=byteorder)
        elif type_ == "float" or type_ == "double":
            fmt_single_precision = ('>' if byteorder == 'big' else '<') + 'f'
            single_precision_bytes = pack(fmt_single_precision, data)
            if unpack(fmt_single_precision, single_precision_bytes)[0] == data:
                return single_precision_bytes
            fmt_double_precision = ('>' if byteorder == 'big' else '<') + 'd'
            return pack(fmt_double_precision, data)
        return data.encode("UTF-8")
