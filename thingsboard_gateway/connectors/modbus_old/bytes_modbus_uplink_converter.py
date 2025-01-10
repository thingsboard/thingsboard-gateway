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
from time import time

from pymodbus.constants import Endian
from pymodbus.exceptions import ModbusIOException
from pymodbus.payload import BinaryPayloadDecoder
from pymodbus.pdu import ExceptionResponse

from thingsboard_gateway.connectors.modbus_old.modbus_converter import ModbusConverter
from thingsboard_gateway.gateway.constants import REPORT_STRATEGY_PARAMETER, ATTRIBUTES_PARAMETER
from thingsboard_gateway.gateway.entities.converted_data import ConvertedData
from thingsboard_gateway.gateway.entities.report_strategy_config import ReportStrategyConfig
from thingsboard_gateway.gateway.entities.telemetry_entry import TelemetryEntry
from thingsboard_gateway.gateway.statistics.decorators import CollectStatistics
from thingsboard_gateway.gateway.statistics.statistics_service import StatisticsService
from thingsboard_gateway.tb_utility.tb_utility import TBUtility


class BytesModbusUplinkConverter(ModbusConverter):
    def __init__(self, config, logger):
        self._log = logger
        self.__datatypes = {
            "timeseries": "telemetry",
            "attributes": "attributes"
            }
        self.__device_name = config.get("deviceName", "ModbusDevice %s" % (str(config["unitId"])))
        self.__device_type = config.get("deviceType", "default")
        self.__device_report_strategy = None
        try:
            self.__device_report_strategy = ReportStrategyConfig(config.get(REPORT_STRATEGY_PARAMETER))
        except ValueError as e:
            self._log.trace("Report strategy config is not specified for device %s", self.__device_name)

    @staticmethod
    def from_coils(coils, endian_order=Endian.Little, word_endian_order=Endian.Big):
        _is_wordorder = '_wordorder' in BinaryPayloadDecoder.fromCoils.__code__.co_varnames
        if _is_wordorder:
            try:
                decoder = BinaryPayloadDecoder.fromCoils(coils, byteorder=endian_order,
                                                         wordorder=word_endian_order)
            except TypeError:
                decoder = BinaryPayloadDecoder.fromCoils(coils, wordorder=word_endian_order)
        else:
            try:
                decoder = BinaryPayloadDecoder.fromCoils(coils, byteorder=endian_order,
                                                         wordorder=word_endian_order)
            except TypeError:
                decoder = BinaryPayloadDecoder.fromCoils(coils, wordorder=word_endian_order)

        return decoder

    @CollectStatistics(start_stat_type='receivedBytesFromDevices',
                       end_stat_type='convertedBytesFromDevice')
    def convert(self, config, data):
        StatisticsService.count_connector_message(self._log.name, 'convertersMsgProcessed')

        converted_data = ConvertedData(self.__device_name, self.__device_type)

        timestamp = data.get("ts", time() * 1000)

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
                        word_order = config.get("wordOrder", "BIG")
                    else:
                        word_order = "BIG"
                    endian_order = Endian.Little if byte_order.upper() == "LITTLE" else Endian.Big
                    word_endian_order = Endian.Little if word_order.upper() == "LITTLE" else Endian.Big
                    decoded_data = None
                    if not isinstance(response, ModbusIOException) and not isinstance(response, ExceptionResponse):
                        if configuration["functionCode"] in [1, 2]:
                            decoder = None
                            coils = response.bits

                            try:
                                decoder = self.from_coils(coils, endian_order=endian_order,
                                                          word_endian_order=word_endian_order)
                            except TypeError:
                                decoder = self.from_coils(coils, word_endian_order=word_endian_order)

                            assert decoder is not None
                            decoded_data = self.decode_from_registers(decoder, configuration)
                        elif configuration["functionCode"] in [3, 4]:
                            decoder = None
                            registers = response.registers
                            self._log.debug("Tag: %s Config: %s registers: %s", tag, str(configuration), str(registers))
                            try:
                                decoder = BinaryPayloadDecoder.fromRegisters(registers, byteorder=endian_order,
                                                                             wordorder=word_endian_order)
                            except TypeError:
                                # pylint: disable=E1123
                                decoder = BinaryPayloadDecoder.fromRegisters(registers, byteorder=endian_order,
                                                                             wordorder=word_endian_order)
                            assert decoder is not None
                            decoded_data = self.decode_from_registers(decoder, configuration)
                            if configuration.get("divider"):
                                decoded_data = float(decoded_data) / float(configuration["divider"])
                            elif configuration.get("multiplier"):
                                decoded_data = decoded_data * configuration["multiplier"]
                    else:
                        self._log.exception(response)
                        decoded_data = None
                    if config_data == "rpc":
                        return decoded_data
                    self._log.debug("datatype: %s \t key: %s \t value: %s", self.__datatypes[config_data], tag, str(decoded_data))
                    if decoded_data is not None:
                        converted_key = TBUtility.convert_key_to_datapoint_key(tag, self.__device_report_strategy, configuration, self._log)
                        if self.__datatypes[config_data] == ATTRIBUTES_PARAMETER:
                            converted_data.add_to_attributes(converted_key, decoded_data)
                        else:
                            telemetry_entry = TelemetryEntry({converted_key: decoded_data}, timestamp)
                            converted_data.add_to_telemetry(telemetry_entry)
                except Exception as e:
                    self._log.exception(e)
                    StatisticsService.count_connector_message(self._log.name, 'convertersMsgDropped')

        self._log.debug("Converted data: %s", converted_data)
        StatisticsService.count_connector_message(self._log.name, 'convertersAttrProduced',
                                                  count=converted_data.attributes_datapoints_count)
        StatisticsService.count_connector_message(self._log.name, 'convertersTsProduced',
                                                  count=converted_data.telemetry_datapoints_count)

        return converted_data

    def decode_from_registers(self, decoder, configuration):
        type_ = configuration["type"]
        objects_count = configuration.get("objectsCount",
                                          configuration.get("registersCount", configuration.get("registerCount", 1)))
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
            decoded = decoder_functions[type_]()
            decoded_lastbyte = decoder_functions[type_]()
            decoded += decoded_lastbyte
            decoded = decoded[len(decoded)-objects_count:]

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
            self._log.error("Unknown type: %s", type_)

        if isinstance(decoded, int):
            result_data = decoded
        elif isinstance(decoded, bytes) and lower_type == "string":
            try:
                result_data = decoded.decode('UTF-8')
            except UnicodeDecodeError as e:
                self._log.error("Error decoding string from bytes, will be saved as hex: %s", decoded, exc_info=e)
                result_data = decoded.hex()
        elif isinstance(decoded, bytes) and lower_type == "bytes":
            result_data = decoded.hex()
        elif isinstance(decoded, list):
            if configuration.get('bit') is not None:
                result_data = int(decoded[configuration['bit'] if
                configuration['bit'] < len(decoded) else len(decoded) - 1])
            else:
                bitAsBoolean = configuration.get('bitTargetType', 'bool') == 'bool'
                if objects_count == 1:
                    result_data = bool(decoded[-1]) if bitAsBoolean else int(decoded[-1])
                else:
                    result_data = [bool(bit) if bitAsBoolean else int(bit) for bit in decoded]
        elif isinstance(decoded, float):
            result_data = decoded
        elif decoded is not None:
            result_data = int(decoded, 16)
        else:
            result_data = decoded

        return result_data
