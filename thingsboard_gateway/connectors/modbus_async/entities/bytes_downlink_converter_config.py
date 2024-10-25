class BytesDownlinkConverterConfig:
    def __init__(self, device_name, byte_order, word_order, repack, objects_count, function_code, lower_type, address):
        self.device_name = device_name
        self.byte_order = byte_order
        self.word_order = word_order
        self.repack = repack
        self.objects_count = objects_count
        self.function_code = function_code
        self.lower_type = lower_type.lower()
        self.address = address
