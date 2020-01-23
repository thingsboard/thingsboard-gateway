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

from pymodbus.constants import Endian
from pymodbus.payload import BinaryPayloadDecoder

from thingsboard_gateway.connectors.modbus.modbus_converter import ModbusConverter, log


class BytesModbusUplinkConverter(ModbusConverter):
    def __init__(self, config):
        self.__result = {"deviceName": config.get("deviceName", "ModbusDevice %s" % (config["unitId"])),
                         "deviceType": config.get("deviceType", "ModbusDevice")}

    def convert(self, config, data):
        self.__result["telemetry"] = []
        self.__result["attributes"] = []
        for config_data in data:
            if self.__result.get(config_data) is None:
                self.__result[config_data] = []
            for tag in data[config_data]:
                log.debug(tag)
                data_sent = data[config_data][tag]["data_sent"]
                input_data = data[config_data][tag]["input_data"]
                log.debug("Called convert function from %s with args", self.__class__.__name__)
                log.debug(data_sent)
                log.debug(input_data)
                result = None
                if data_sent.get("functionCode") == 1 or data_sent.get("functionCode") == 2:
                    result = input_data.bits
                    log.debug(result)
                    if "registerCount" in data_sent:
                        result = result[:data_sent["registerCount"]]
                    else:
                        result = result[0]
                elif data_sent.get("functionCode") == 3 or data_sent.get("functionCode") == 4:
                    result = input_data.registers
                    byte_order = data_sent.get("byteOrder", "LITTLE")
                    reg_count = data_sent.get("registerCount", 1)
                    type_of_data = data_sent["type"]
                    try:
                        if byte_order == "LITTLE":
                            decoder = BinaryPayloadDecoder.fromRegisters(result, byteorder=Endian.Little)
                        elif byte_order == "BIG":
                            decoder = BinaryPayloadDecoder.fromRegisters(result, byteorder=Endian.Big)
                        else:
                            log.warning("byte order is not BIG or LITTLE")
                            # continue
                    except Exception as e:
                        log.error(e)
                    if type_of_data == "string":
                        result = decoder.decode_string(2 * reg_count)
                    elif type_of_data == "long":
                        try:
                            if reg_count == 1:
                                # r = decoder.decode_8bit_int()
                                result = decoder.decode_16bit_int()
                            elif reg_count == 2:
                                result = decoder.decode_32bit_int()
                            elif reg_count == 4:
                                result = decoder.decode_64bit_int()
                            else:
                                log.warning("unsupported register count for long data type in response for tag %s",
                                            data_sent["tag"])
                        except Exception as e:
                            log.exception(e)
                    elif type_of_data == "double":
                        if reg_count == 2:
                            result = decoder.decode_32bit_float()
                        elif reg_count == 4:
                            result = decoder.decode_64bit_float()
                        else:
                            log.warning("unsupported register count for double data type in response for tag %s",
                                        data_sent["tag"])
                            # continue
                    elif type_of_data == "bit":
                        if "bit" in data_sent:
                            if type(result) == list:
                                if len(result) > 1:
                                    log.warning("with bit parameter only one register is expected, got more then one in response for tag %s",
                                                data_sent["tag"])
                                    continue
                                result = result[0]
                            position = 15 - data_sent["bit"]  # reverse order
                            # transform result to string representation of a bit sequence, add "0" to make it longer >16
                            result = "0000000000000000" + str(bin(result)[2:])
                            # get length of 16, then get bit, then cast it to int(0||1 from "0"||"1", then cast to boolean)
                            result = bool(int((result[len(result) - 16:])[15 - position]))
                        else:
                            log.error("Bit address not found in config for modbus connector for tag: %s", data_sent["tag"])

                    else:
                        log.warning("unknown data type, not string, long or double in response for tag %s",
                                    data_sent["tag"])
                        continue
                try:
                    if result == 0:
                        self.__result[config_data].append({tag: result})
                    elif int(result):
                        self.__result[config_data].append({tag: result})
                except ValueError:
                    self.__result[config_data].append({tag: int(result, 16)})
        self.__result["telemetry"] = self.__result.pop("timeseries")
        log.debug(self.__result)
        return self.__result
