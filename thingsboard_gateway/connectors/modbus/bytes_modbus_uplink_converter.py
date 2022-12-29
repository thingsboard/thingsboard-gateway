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
from pymodbus.exceptions import ModbusIOException
from pymodbus.payload import BinaryPayloadDecoder
from pymodbus.pdu import ExceptionResponse

from thingsboard_gateway.connectors.modbus.modbus_converter import ModbusConverter, log
from thingsboard_gateway.gateway.statistics_service import StatisticsService


class BytesModbusUplinkConverter(ModbusConverter):
    def __init__(self, config):
        self.__datatypes = {
            "timeseries": "telemetry",
            "attributes": "attributes"
            }
        self.__result = {"deviceName": config.get("deviceName", "ModbusDevice %s" % (str(config["unitId"]))),
                         "deviceType": config.get("deviceType", "default")}

    @StatisticsService.CollectStatistics(start_stat_type='receivedBytesFromDevices',
                                         end_stat_type='convertedBytesFromDevice')
    def convert(self, config, data):
        self.__result["telemetry"] = []
        self.__result["attributes"] = []
        for config_data in data:
            for tag in data[config_data]:
                try:
                    configuration = data[config_data][tag]["data_sent"]
                    response = data[config_data][tag]["input_data"]
                    if configuration.get("byteOrder"):
                        byte_order = configuration["byteOrder"]
                    elif config.get("byteOrder"):
                        byte_order = config["byteOrder"]
                    else:
                        byte_order = "LITTLE"
                    if configuration.get("wordOrder"):
                        word_order = configuration["wordOrder"]
                    elif config.get("wordOrder"):
                        word_order = config.get("wordOrder", "LITTLE")
                    else:
                        word_order = "LITTLE"
                    endian_order = Endian.Little if byte_order.upper() == "LITTLE" else Endian.Big
                    word_endian_order = Endian.Little if word_order.upper() == "LITTLE" else Endian.Big
                    decoded_data = None
                    if not isinstance(response, ModbusIOException) and not isinstance(response, ExceptionResponse):
                        if configuration["functionCode"] in [1, 2]:
                            decoder = None
                            coils = response.bits
                            try:
                                decoder = BinaryPayloadDecoder.fromCoils(coils, byteorder=endian_order, wordorder=word_endian_order)
                            except TypeError:
                                decoder = BinaryPayloadDecoder.fromCoils(coils, wordorder=word_endian_order)
                            assert decoder is not None
                            decoded_data = self.decode_from_registers(decoder, configuration)
                        elif configuration["functionCode"] in [3, 4]:
                            decoder = None
                            registers = response.registers
                            log.debug("Tag: %s Config: %s registers: %s", tag, str(configuration), str(registers))
                            try:
                                decoder = BinaryPayloadDecoder.fromRegisters(registers, byteorder=endian_order, wordorder=word_endian_order)
                            except TypeError:
                                # pylint: disable=E1123
                                decoder = BinaryPayloadDecoder.fromRegisters(registers, endian=endian_order, wordorder=word_endian_order)
                            assert decoder is not None
                            decoded_data = self.decode_from_registers(decoder, configuration)
                            if configuration.get("divider"):
                                decoded_data = float(decoded_data) / float(configuration["divider"])
                            if configuration.get("multiplier"):
                                decoded_data = decoded_data * configuration["multiplier"]
                    else:
                        log.exception(response)
                        decoded_data = None
                    if config_data == "rpc":
                        return decoded_data
                    log.debug("datatype: %s \t key: %s \t value: %s", self.__datatypes[config_data], tag, str(decoded_data))
                    if decoded_data is not None:
                        self.__result[self.__datatypes[config_data]].append({tag: decoded_data})
                except Exception as e:
                    log.exception(e)
        log.debug(self.__result)
        return self.__result

    @staticmethod
    def decode_from_registers(decoder, configuration):
        type_ = configuration["type"]
        objects_count = configuration.get("objectsCount", configuration.get("registersCount", configuration.get("registerCount", 1)))
        lower_type = type_.lower()

        decoder_functions = {
            'string': decoder.decode_string,
            'bytes': decoder.decode_string,
            'bit': decoder.decode_bits,
            'bits': decoder.decode_bits,
            '8int': decoder.decode_8bit_int,
            '8uint': decoder.decode_8bit_uint,
            '16int': decoder.decode_16bit_int,
            '16uint': decoder.decode_16bit_uint,
            '16float': decoder.decode_16bit_float,
            '32int': decoder.decode_32bit_int,
            '32uint': decoder.decode_32bit_uint,
            '32float': decoder.decode_32bit_float,
            '64int': decoder.decode_64bit_int,
            '64uint': decoder.decode_64bit_uint,
            '64float': decoder.decode_64bit_float,
            }

        decoded = None

        if lower_type in ['bit', 'bits']:
            decoded_lastbyte = decoder_functions[type_]()
            decoded = decoder_functions[type_]()
            decoded += decoded_lastbyte

        elif lower_type == "string":
            decoded = decoder_functions[type_](objects_count * 2)

        elif lower_type == "bytes":
            decoded = decoder_functions[type_](size=objects_count * 2)

        elif decoder_functions.get(lower_type) is not None:
            decoded = decoder_functions[lower_type]()

        elif lower_type in ['int', 'long', 'integer']:
            type_ = str(objects_count * 16) + "int"
            assert decoder_functions.get(type_) is not None
            decoded = decoder_functions[type_]()

        elif lower_type in ["double", "float"]:
            type_ = str(objects_count * 16) + "float"
            assert decoder_functions.get(type_) is not None
            decoded = decoder_functions[type_]()

        elif lower_type == 'uint':
            type_ = str(objects_count * 16) + "uint"
            assert decoder_functions.get(type_) is not None
            decoded = decoder_functions[type_]()

        else:
            log.error("Unknown type: %s", type_)

        if isinstance(decoded, int):
            result_data = decoded
        elif isinstance(decoded, bytes) and lower_type == "string":
            result_data = decoded.decode('UTF-8')
        elif isinstance(decoded, bytes) and lower_type == "bytes":
            result_data = decoded.hex()
        elif isinstance(decoded, list):
            if configuration.get('bit') is not None:
                result_data = int(decoded[configuration['bit']])
            else:
                if objects_count == 1 and configuration.get('bitTargetType', 'bool') == 'bool':
                    result_data = bool(decoded[-1])
                else:
                    result_data = [int(bit) for bit in decoded]
        elif isinstance(decoded, float):
            result_data = decoded
        elif decoded is not None:
            result_data = int(decoded, 16)
        else:
            result_data = decoded

        return result_data
