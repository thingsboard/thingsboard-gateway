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

from typing import List, Union

from pymodbus.constants import Endian
from pymodbus.exceptions import ModbusIOException
from pymodbus.payload import BinaryPayloadDecoder
from pymodbus.pdu import ExceptionResponse

from thingsboard_gateway.connectors.modbus.entities.bytes_uplink_converter_config import BytesUplinkConverterConfig
from thingsboard_gateway.connectors.modbus.modbus_converter import ModbusConverter
from thingsboard_gateway.gateway.entities.converted_data import ConvertedData
from thingsboard_gateway.gateway.entities.report_strategy_config import ReportStrategyConfig
from thingsboard_gateway.gateway.statistics.decorators import CollectStatistics
from thingsboard_gateway.gateway.statistics.statistics_service import StatisticsService
from thingsboard_gateway.tb_utility.tb_utility import TBUtility


class BytesModbusUplinkConverter(ModbusConverter):
    def __init__(self, config: BytesUplinkConverterConfig, logger):
        self._log = logger
        self.__config = config

    @CollectStatistics(start_stat_type='receivedBytesFromDevices',
                       end_stat_type='convertedBytesFromDevice')
    def convert(self, _, data: List[dict]) -> Union[ConvertedData, None]:
        result = ConvertedData(self.__config.device_name, self.__config.device_type)
        device_report_strategy = self._get_device_report_strategy(self.__config.report_strategy,
                                                                  self.__config.device_name)

        converted_data_append_methods = {
            'attributes': result.add_to_attributes,
            'telemetry': result.add_to_telemetry
        }
        for device_data in data:
            StatisticsService.count_connector_message(self._log.name, 'convertersMsgProcessed')

            for config_section in converted_data_append_methods:
                for config in getattr(self.__config, config_section):
                    encoded_data = device_data[config_section].get(config['tag'])

                    if encoded_data:
                        decoded_data = self.decode_data(encoded_data, config,
                                                        self.__config.byte_order,
                                                        self.__config.word_order)

                        if decoded_data is not None:
                            datapoint_key = TBUtility.convert_key_to_datapoint_key(config['tag'], device_report_strategy,
                                                                                   config, self._log)
                            converted_data_append_methods[config_section]({datapoint_key: decoded_data})

            self._log.trace("Decoded data: %s", result)
            StatisticsService.count_connector_message(self._log.name, 'convertersAttrProduced',
                                                      count=result.attributes_datapoints_count)
            StatisticsService.count_connector_message(self._log.name, 'convertersTsProduced',
                                                      count=result.telemetry_datapoints_count)

            return result

    def decode_data(self, encoded_data, config, endian_order, word_endian_order):
        decoded_data = None

        if not isinstance(encoded_data, ModbusIOException) and not isinstance(encoded_data, ExceptionResponse):
            if config['functionCode'] in (1, 2):
                try:
                    decoder = self.from_coils(encoded_data.bits, endian_order=endian_order,
                                              word_endian_order=word_endian_order)
                except TypeError:
                    decoder = self.from_coils(encoded_data.bits, word_endian_order=word_endian_order)

                decoded_data = self.decode_from_registers(decoder, config)
            elif config['functionCode'] in (3, 4):
                decoder = BinaryPayloadDecoder.fromRegisters(encoded_data.registers, byteorder=endian_order,
                                                             wordorder=word_endian_order)
                decoded_data = self.decode_from_registers(decoder, config)

                if config.get('divider'):
                    decoded_data = float(decoded_data) / float(config['divider'])
                elif config.get('multiplier'):
                    decoded_data = decoded_data * config['multiplier']
        else:
            self._log.exception("Error while decoding data: %s, with config: %s", encoded_data, config)
            decoded_data = None

        return decoded_data

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

    def decode_from_registers(self, decoder, configuration):
        objects_count = configuration.get("objectsCount",
                                          configuration.get("registersCount", configuration.get("registerCount", 1)))
        lower_type = configuration["type"].lower()

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
            decoded = decoder_functions[lower_type]()
            decoded_lastbyte = decoder_functions[lower_type]()
            decoded += decoded_lastbyte
            decoded = decoded[len(decoded)-objects_count:]

        elif lower_type == "string":
            decoded = decoder_functions[lower_type](objects_count * 2)

        elif lower_type == "bytes":
            decoded = decoder_functions[lower_type](size=objects_count * 2)

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
            self._log.error("Unknown type: %s", lower_type)

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

    def _get_device_report_strategy(self, report_strategy, device_name):
        try:
            return ReportStrategyConfig(report_strategy)
        except ValueError as e:
            self._log.trace("Report strategy config is not specified for device %s: %s", device_name, e)
