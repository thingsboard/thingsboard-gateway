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

from pymodbus.payload import BinaryPayloadBuilder

from thingsboard_gateway.connectors.modbus.modbus_converter import ModbusConverter
from thingsboard_gateway.connectors.modbus.entities.bytes_downlink_converter_config import \
    BytesDownlinkConverterConfig
from thingsboard_gateway.gateway.statistics.decorators import CollectStatistics


class BytesModbusDownlinkConverter(ModbusConverter):

    def __init__(self, _, logger):
        self._log = logger

    @CollectStatistics(start_stat_type='allReceivedBytesFromTB',
                       end_stat_type='allBytesSentToDevices')
    def convert(self, config: BytesDownlinkConverterConfig, data):
        builder = BinaryPayloadBuilder(byteorder=config.byte_order, wordorder=config.word_order, repack=config.repack)
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

        value = data["data"]["params"]

        if config.lower_type == "error":
            self._log.error('"type" and "tag" - not found in configuration.')

        lower_type = config.lower_type
        variable_size = config.objects_count * 16 if lower_type not in ["coils", "bits", "coil",
                                                                        "bit"] else config.objects_count

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
            if variable_size > 1:
                if isinstance(value, str):
                    builder_functions["bits"](bytes(value, encoding='UTF-8'))
                elif isinstance(value, list):
                    builder_functions["bits"]([int(x) for x in value])
                else:
                    builder_functions["bits"]([int(x) for x in bin(value)[2:]])
            else:
                return int(value).to_bytes(1, byteorder='big')
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
            self._log.error("Unknown variable type")
            return None

        builder_converting_functions = {5: builder.to_coils,
                                        15: builder.to_coils,
                                        6: builder.to_registers,
                                        16: builder.to_registers}

        function_code = config.function_code

        if function_code in builder_converting_functions:
            builder = builder_converting_functions[function_code]()
            self._log.debug("Created builder %r.", builder)
            if "Exception" in str(builder):
                self._log.exception(builder)
                builder = str(builder)
            # if function_code is 5 , is using first coils value
            if function_code == 5:
                if isinstance(builder, list):
                    builder = builder[0]
            else:
                if variable_size <= 16:
                    if isinstance(builder, list) and len(builder) not in (8, 16, 32, 64):
                        builder = builder[0]
                else:
                    if isinstance(builder, list) and len(builder) not in (2, 4):
                        self._log.warning("There is a problem with the value builder. "
                                          "Only the first register is written.")
                        builder = builder[0]
            return builder
        self._log.warning("Unsupported function code, for the device %s in the Modbus Downlink converter",
                          config.device_name)
        return None
