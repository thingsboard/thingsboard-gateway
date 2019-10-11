import logging
from pymodbus.payload import BinaryPayloadDecoder
from pymodbus.constants import Endian
from connectors.modbus.modbus_converter import ModbusConverter

log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)


class BytesModbusUplinkConverter(ModbusConverter):
    def __init__(self, config):
        self.__result = {"deviceName": config.get("deviceName", "ModbusDevice %s" % (config["unitId"])),
                         "deviceType": config.get("deviceType", "ModbusDevice")}

    def convert(self, data, config):
        self.__result["telemetry"] = []
        self.__result["attributes"] = []
        for config_data in data:
            if self.__result.get(config_data) is None:
                self.__result[config_data] = []
            for tag in data[config_data]:
                sended_data = data[config_data][tag]["sended_data"]
                input_data = data[config_data][tag]["input_data"]
                log.debug("Called convert function from %s with args", self.__class__.__name__)
                log.debug(sended_data)
                log.debug(input_data)
                result = None
                if sended_data.get("functionCode") == 1 or sended_data.get("functionCode") == 2:
                    result = input_data.bits
                    log.debug(result)
                    if "registerCount" in sended_data:
                        result = result[:sended_data["registerCount"]]
                    else:
                        result = result[0]
                elif sended_data.get("functionCode") == 3 or sended_data.get("functionCode") == 4:
                    result = input_data.registers
                    byte_order = sended_data.get("byteOrder", "LITTLE")
                    reg_count = sended_data.get("registerCount", 1)
                    type_of_data = sended_data["type"]
                    if byte_order == "LITTLE":
                        decoder = BinaryPayloadDecoder.fromRegisters(result, byteorder=Endian.Little)
                    elif byte_order == "BIG":
                        decoder = BinaryPayloadDecoder.fromRegisters(result, byteorder=Endian.Big)
                    else:
                        log.warning("byte order is not BIG or LITTLE")
                        continue
                    if type_of_data == "string":
                        result = decoder.decode_string(2 * reg_count)
                    elif type_of_data == "long":
                        if reg_count == 1:
                            result = decoder.decode_16bit_int()
                        elif reg_count == 2:
                            result = decoder.decode_32bit_int()
                        elif reg_count == 4:
                            result = decoder.decode_64bit_int()
                        else:
                            log.warning("unsupported register count for long data type in response for tag %s",
                                        sended_data["tag"])
                            continue
                    elif type_of_data == "double":
                        if reg_count == 2:
                            result = decoder.decode_32bit_float()
                        elif reg_count == 4:
                            result = decoder.decode_64bit_float()
                        else:
                            log.warning("unsupported register count for double data type in response for tag %s",
                                        sended_data["tag"])
                            continue
                    else:
                        log.warning("unknown data type, not string, long or double in response for tag %s",
                                    sended_data["tag"])
                        continue
                    if "bit" in sended_data:
                        if len(result) > 1:
                            log.warning("with bit parameter only one register is expected, got more then one in response for tag %s",
                                        sended_data["tag"])
                            continue
                        result = result[0]
                        position = 15 - sended_data["bit"]  # reverse order
                        # transform result to string representation of a bit sequence, add "0" to make it longer >16
                        result = "0000000000000000" + str(bin(result)[2:])
                        # get length of 16, then get bit, then cast it to int(0||1 from "0"||"1", then cast to boolean)
                        result = bool(int((result[len(result) - 16:])[15 - position]))
                self.__result[config_data].append({tag: int(result)})
        self.__result["telemetry"] = self.__result.pop("timeseries")
        log.debug(self.__result)
        return self.__result
