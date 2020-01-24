#     Copyright 2020. ThingsBoard
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

from pymodbus.payload import BinaryPayloadBuilder
from re import findall
from pymodbus.constants import Endian
from thingsboard_gateway.connectors.modbus.modbus_converter import ModbusConverter, log


class BytesModbusDownlinkConverter(ModbusConverter):

    def __init__(self, config):
        self.__config = config

    def convert(self, config, data):
        byte_order = config["byteOrder"] if config.get("byteOrder") else "LITTLE"
        if byte_order == "LITTLE":
            builder = BinaryPayloadBuilder(byteorder=Endian.Little)
        elif byte_order == "BIG":
            builder = BinaryPayloadBuilder(byteorder=Endian.Big)
        else:
            log.warning("byte order is not BIG or LITTLE")
            return
        reg_count = config.get("registerCount", 1)
        value = config["value"]
        if config.get("tag") is not None:
            tags = (findall('[A-Z][a-z]*', config["tag"]))
            if "Coil" in tags:
                builder.add_bits(value)
            elif "String" in tags:
                builder.add_string(value)
            elif "Double" in tags:
                if reg_count == 4:
                    builder.add_64bit_float(value)
                else:
                    log.warning("unsupported amount of registers with double type for device %s in Downlink converter",
                                self.__config["deviceName"])
                    return
            elif "Float" in tags:
                if reg_count == 2:
                    builder.add_32bit_float(value)
                else:
                    log.warning("unsupported amount of registers with float type for device %s in Downlink converter",
                                self.__config["deviceName"])
                    return
            elif "Integer" in tags or "DWord" in tags or "DWord/Integer" in tags or "Word" in tags:
                if reg_count == 1:
                    builder.add_16bit_int(value)
                elif reg_count == 2:
                    builder.add_32bit_int(value)
                elif reg_count == 4:
                    builder.add_64bit_int(value)
                else:
                    log.warning("unsupported amount of registers with integer/word/dword type for device %s in Downlink converter",
                                self.__config["deviceName"])
                    return
            else:
                log.warning("unsupported hardware data type for device %s in Downlink converter",
                            self.__config["deviceName"])

        if config.get("bit") is not None:
            bits = [0 for _ in range(8)]
            bits[config["bit"]-1] = int(value)
            log.debug(bits)
            builder.add_bits(bits)
            return builder.to_string()

        if config["functionCode"] in [5, 15]:
            return builder.to_coils()
        elif config["functionCode"] in [6, 16]:
            return builder.to_registers()
        else:
            log.warning("Unsupported function code,  for device %s in Downlink converter",
                        self.__config["deviceName"])
        return
