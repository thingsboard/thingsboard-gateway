from pymodbus.constants import Endian


class BytesDownlinkConverterConfig:
    def __init__(self, device_name, byte_order, word_order, repack, objects_count, function_code, lower_type, address):
        self.device_name = device_name
        self.byte_order = Endian.Big if byte_order.upper() == "BIG" else Endian.Little
        self.word_order = Endian.Big if word_order.upper() == "BIG" else Endian.Little
        self.repack = repack
        self.objects_count = objects_count
        self.function_code = function_code
        self.lower_type = lower_type.lower()
        self.address = address
