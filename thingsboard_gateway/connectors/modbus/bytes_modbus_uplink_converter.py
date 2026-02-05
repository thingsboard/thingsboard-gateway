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

from typing import List, Union

from pymodbus.constants import Endian
from pymodbus.payload import BinaryPayloadDecoder

from thingsboard_gateway.connectors.modbus.entities.bytes_uplink_converter_config import BytesUplinkConverterConfig
from thingsboard_gateway.connectors.modbus.modbus_converter import ModbusConverter
from thingsboard_gateway.connectors.modbus.utils import Utils
from thingsboard_gateway.connectors.modbus.constants import REQUIRED_KEYS_FOR_WIDE_RANGE_TAG_NAME
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

                    try:
                        if Utils.is_wide_range_request(config['address']):
                            datapoints = self.__process_wide_range_response(config, encoded_data)
                        else:
                            datapoints = self.__process_single_address_response(config, encoded_data)
                    except (ValueError, IndexError, TypeError) as e:
                        self._log.error("Encoded data is invalid: %s, with config: %s, error: %s",
                                        encoded_data, config, e)
                        continue

                    for datapoint in datapoints:
                        for key_name, decoded_data in datapoint.items():
                            datapoint_key = TBUtility.convert_key_to_datapoint_key(key_name,
                                                                                   device_report_strategy,
                                                                                   config,
                                                                                   self._log)
                            converted_data_append_methods[config_section]({datapoint_key: decoded_data})

        self._log.trace("Decoded data: %s", result)
        StatisticsService.count_connector_message(self._log.name, 'convertersAttrProduced',
                                                  count=result.attributes_datapoints_count)
        StatisticsService.count_connector_message(self._log.name, 'convertersTsProduced',
                                                  count=result.telemetry_datapoints_count)

        return result

    def __process_wide_range_response(self, config, encoded_data):
        encoded_data = self.__validate_wide_range_encoded_data(encoded_data)
        registers_data = self.__get_registers_from_wide_range_encoded_data(encoded_data,
                                                                           config['functionCode'])
        datapoints = self.__process_wide_range_response_encoded_data(config, registers_data)
        return datapoints

    def __validate_wide_range_encoded_data(self, encoded_data):
        invalid_chunks = []

        for chunk in encoded_data:
            if not Utils.is_encoded_data_valid(chunk):
                invalid_chunks.append(chunk)
                self._log.error("Encoded data chunk is invalid: %s. Skipping", chunk)

        if len(invalid_chunks) > 0:
            encoded_data = [chunk for chunk in encoded_data if chunk not in invalid_chunks]

        return encoded_data

    def __get_registers_from_wide_range_encoded_data(self, encoded_data, function_code):
        registers_data = []

        for chunk in encoded_data:
            registers_chunk = Utils.get_registers_from_encoded_data(chunk, function_code)
            registers_data.extend(registers_chunk)

        return registers_data

    def __process_single_address_response(self, config, encoded_data):
        encoded_data = encoded_data[0]

        if not Utils.is_encoded_data_valid(encoded_data):
            raise ValueError('Encoded data is invalid')

        registers_data = Utils.get_registers_from_encoded_data(encoded_data,
                                                               config['functionCode'])

        datapoints = self.__process_single_address_response_encoded_data(config, registers_data)

        return datapoints

    def __process_wide_range_response_encoded_data(self, config, encoded_data):
        result = []

        try:
            current_address = Utils.get_start_address(config['address'])
        except Exception as e:
            self._log.error("Error getting start address from config: %s, with config: %s",
                            e, config, exc_info=e)
            return []

        for i in range(0, len(encoded_data), config.get('objectsCount', 1)):
            chunk = encoded_data[i:i + config.get('objectsCount', 1)]
            decoded_data = self.decode_data(chunk, config,
                                            self.__config.byte_order,
                                            self.__config.word_order)

            if decoded_data is None:
                self._log.warning("Decoded data is empty, with config: %s", config)
                continue

            key_name = self.__get_key_name(config, current_address)
            result.append({key_name: decoded_data})

            current_address += config.get('objectsCount', 1)

        return result

    def __process_single_address_response_encoded_data(self, config, encoded_data):
        decoded_data = self.decode_data(encoded_data, config,
                                        self.__config.byte_order,
                                        self.__config.word_order)

        if decoded_data is None:
            self._log.warning("Decoded data is empty, with config: %s", config)
            return []

        key_name = self.__get_key_name(config)

        return [{key_name: decoded_data}]

    def __get_key_name(self, config, current_address=None):
        if Utils.is_wide_range_request(config['address']) and current_address is not None:
            key_name = self.__get_wide_range_key_name(config, current_address)
        else:
            key_name = config['tag']

        return key_name

    def __get_wide_range_key_name(self, config, current_address):
        key_name_info = self.__get_info_for_key_name(config)
        key_name_info['address'] = current_address
        config['tag'] = self.__validate_key_name_expression(config['tag'])
        result_tags = TBUtility.get_values(config['tag'], key_name_info, get_tag=True)
        result_values = TBUtility.get_values(config['tag'], key_name_info, expression_instead_none=True)

        result = config['tag']
        for (result_tag, result_value) in zip(result_tags, result_values):
            is_valid_key = "${" in config['tag'] and "}" in config['tag']
            result = result.replace('${' + str(result_tag) + '}',
                                    str(result_value)) if is_valid_key else result_tag

        return result

    def __get_info_for_key_name(self, config):
        return {
            'unitId': self.__config.unit_id,
            'address': config['address'],
            'functionCode': config['functionCode'],
            'type': config['type'],
            'objectsCount': config.get('objectsCount', 1),
        }

    def __validate_key_name_expression(self, key_name):
        for required_key in REQUIRED_KEYS_FOR_WIDE_RANGE_TAG_NAME:
            if required_key not in key_name:
                self._log.warning("Tag name '%s' does not contain required key '%s'. "
                                  "Appending it to the key name.", key_name, required_key)
                key_name += f"_${{{required_key}}}"

        return key_name

    def decode_data(self, encoded_data, config, endian_order, word_endian_order):
        decoded_data = None

        if config['functionCode'] in (1, 2):
            try:
                decoder = self.from_coils(encoded_data, endian_order=endian_order,
                                          word_endian_order=word_endian_order)
            except TypeError:
                decoder = self.from_coils(encoded_data, word_endian_order=word_endian_order)

            decoded_data = self.decode_from_registers(decoder, config)
        elif config['functionCode'] in (3, 4):
            decoder = BinaryPayloadDecoder.fromRegisters(encoded_data, byteorder=endian_order,
                                                         wordorder=word_endian_order)
            decoded_data = self.decode_from_registers(decoder, config)

            if config.get('divider'):
                decoded_data = float(decoded_data) / float(config['divider'])
            elif config.get('multiplier'):
                decoded_data = decoded_data * config['multiplier']

        if self._is_enum_value(config):
            decoded_data = self._process_enum_value(config, decoded_data)

        return decoded_data

    @staticmethod
    def from_coils(coils, endian_order=Endian.LITTLE, word_endian_order=Endian.BIG):
        _is_wordorder = '_wordorder' in BinaryPayloadDecoder.fromCoils.__code__.co_varnames
        if _is_wordorder:
            try:
                decoder = BinaryPayloadDecoder.fromCoils(coils, byteorder=endian_order,
                                                         _wordorder=word_endian_order)
            except TypeError:
                decoder = BinaryPayloadDecoder.fromCoils(coils, _wordorder=word_endian_order)
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
            result_data = float(round(decoded, configuration.get('round', 6)))
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

    @staticmethod
    def _is_enum_value(config):
        return 'variants' in config

    def _process_enum_value(self, config, decoded_data):
        try:
            enum_key = str(decoded_data)

            return config['variants'].get(enum_key, decoded_data)
        except Exception as e:
            self._log.exception(e)
            return decoded_data
