#     Copyright 2022. ThingsBoard
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

from pymodbus.constants import Endian
from pymodbus.payload import BinaryPayloadBuilder

from thingsboard_gateway.connectors.modbus.modbus_converter import ModbusConverter, log
from thingsboard_gateway.gateway.statistics_service import StatisticsService


class BytesModbusDownlinkConverter(ModbusConverter):

    def __init__(self, config):
        self.__config = config

    @StatisticsService.CollectStatistics(start_stat_type='allReceivedBytesFromTB',
                                         end_stat_type='allBytesSentToDevices')
    def convert(self, config, data):
        byte_order_str = config.get("byteOrder", "LITTLE")
        word_order_str = config.get("wordOrder", "LITTLE")
        byte_order = Endian.Big if byte_order_str.upper() == "BIG" else Endian.Little
        word_order = Endian.Big if word_order_str.upper() == "BIG" else Endian.Little
        repack = config.get("repack", False)
        builder = BinaryPayloadBuilder(byteorder=byte_order, wordorder=word_order, repack=repack)
        builder_functions = {"string": builder.add_string,
                             "bits": builder.add_bits,
                             "8int": builder.add_8bit_int,
                             "16int": builder.add_16bit_int,
                             "32int": builder.add_32bit_int,
                             "64int": builder.add_64bit_int,
                             "8uint": builder.add_8bit_uint,
                             "16uint": builder.add_16bit_uint,
                             "32uint": builder.add_32bit_uint,
                             "64uint": builder.add_64bit_uint,
                             "16float": builder.add_16bit_float,
                             "32float": builder.add_32bit_float,
                             "64float": builder.add_64bit_float}
        value = None
        if data.get("data") and data["data"].get("params") is not None:
            value = data["data"]["params"]
        else:
            value = config.get("value", 0)

        lower_type = config.get("type", config.get("tag", "error")).lower()

        if lower_type == "error":
            log.error('"type" and "tag" - not found in configuration.')
        variable_size = config.get("objectsCount", config.get("registersCount", config.get("registerCount", 1))) * 16

        if lower_type in ["integer", "dword", "dword/integer", "word", "int"]:
            lower_type = str(variable_size) + "int"
            assert builder_functions.get(lower_type) is not None
            builder_functions[lower_type](int(value))
        elif lower_type in ["uint", "unsigned", "unsigned integer", "unsigned int"]:
            lower_type = str(variable_size) + "uint"
            assert builder_functions.get(lower_type) is not None
            builder_functions[lower_type](int(value))
        elif lower_type in ["float", "double"]:
            lower_type = str(variable_size) + "float"
            assert builder_functions.get(lower_type) is not None
            builder_functions[lower_type](float(value))
        elif lower_type in ["coil", "bits", "coils", "bit"]:
            assert builder_functions.get("bits") is not None
            if variable_size / 8 > 1.0:
                builder_functions["bits"](bytes(value, encoding='UTF-8')) if isinstance(value, str) else \
                    builder_functions["bits"]([int(x) for x in bin(value)[2:]])
            else:
                return bytes(int(value))
        elif lower_type in ["string"]:
            assert builder_functions.get("string") is not None
            builder_functions[lower_type](value)
        elif lower_type in builder_functions and 'int' in lower_type:
            builder_functions[lower_type](int(value))
        elif lower_type in builder_functions and 'float' in lower_type:
            builder_functions[lower_type](float(value))
        elif lower_type in builder_functions:
            builder_functions[lower_type](value)
        else:
            log.error("Unknown variable type")

        builder_converting_functions = {5: builder.to_coils,
                                        15: builder.to_coils,
                                        6: builder.to_registers,
                                        16: builder.to_registers}

        function_code = config["functionCode"]

        if function_code in builder_converting_functions:
            builder = builder_converting_functions[function_code]()
            log.debug(builder)
            if "Exception" in str(builder):
                log.exception(builder)
                builder = str(builder)
            # if function_code is 5 , is useing first coils value 
            if function_code == 5:
                if isinstance(builder, list):
                    builder = builder[0]
            else:
                if variable_size <= 16:
                    if isinstance(builder, list) and len(builder) not in (8, 16, 32, 64):
                        builder = builder[0]
                else:
                    if isinstance(builder, list) and len(builder) not in (2, 4):
                        log.warning("There is a problem with the value builder. Only the first register is written.")
                        builder = builder[0]
            return builder
        log.warning("Unsupported function code, for the device %s in the Modbus Downlink converter", config["device"])
        return None
