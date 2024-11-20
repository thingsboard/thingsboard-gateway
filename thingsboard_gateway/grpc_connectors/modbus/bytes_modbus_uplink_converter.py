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

from pymodbus.constants import Endian
from pymodbus.exceptions import ModbusIOException
from pymodbus.payload import BinaryPayloadDecoder
from pymodbus.pdu import ExceptionResponse

from thingsboard_gateway.connectors.modbus_old.bytes_modbus_uplink_converter import BytesModbusUplinkConverter
from thingsboard_gateway.connectors.modbus_old.modbus_converter import ModbusConverter


class GrpcBytesModbusUplinkConverter(ModbusConverter):
    def __init__(self, config, logger):
        self._log = logger
        self.__datatypes = {
            "timeseries": "telemetry",
            "attributes": "attributes"
        }
        self.__result = {"deviceName": config.get("deviceName", "ModbusDevice %s" % (str(config["unitId"]))),
                         "deviceType": config.get("deviceType", "default")}

    def convert(self, config, data):
        self.__result["telemetry"] = {}
        self.__result["attributes"] = {}
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
                        if configuration["functionCode"] == 1:
                            decoder = None
                            coils = response.bits
                            try:
                                decoder = BinaryPayloadDecoder.fromCoils(coils, byteorder=endian_order,
                                                                         wordorder=word_endian_order)
                            except TypeError:
                                decoder = BinaryPayloadDecoder.fromCoils(coils, wordorder=word_endian_order)
                            assert decoder is not None

                            decoded_data = decoder.decode_bits()[0]
                        elif configuration["functionCode"] in [2, 3, 4]:
                            decoder = None
                            registers = response.registers
                            self._log.debug("Tag: %s Config: %s registers: %s", tag, str(configuration), str(registers))
                            try:
                                decoder = BinaryPayloadDecoder.fromRegisters(registers, byteorder=endian_order,
                                                                             wordorder=word_endian_order)
                            except TypeError:
                                # pylint: disable=E1123
                                decoder = BinaryPayloadDecoder.fromRegisters(registers, endian=endian_order,
                                                                             wordorder=word_endian_order)
                            assert decoder is not None
                            decoded_data = BytesModbusUplinkConverter.decode_from_registers(decoder, configuration)
                            if configuration.get("divider"):
                                decoded_data = float(decoded_data) / float(configuration["divider"])
                            if configuration.get("multiplier"):
                                decoded_data = decoded_data * configuration["multiplier"]
                    else:
                        self._log.exception(response)
                        decoded_data = None
                    if config_data == "rpc":
                        return decoded_data
                    self._log.debug("datatype: %s \t key: %s \t value: %s", self.__datatypes[config_data], tag,
                              str(decoded_data))
                    if decoded_data is not None:
                        self.__result[self.__datatypes[config_data]][tag] = decoded_data
                except Exception as e:
                    self._log.exception(e)
        self._log.debug(self.__result)
        return self.__result
