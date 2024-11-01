from pymodbus.constants import Endian


class BytesUplinkConverterConfig:
    def __init__(self, **kwargs):
        self.report_strategy = kwargs.get('reportStrategy')
        self.device_name = kwargs['deviceName']
        self.device_type = kwargs.get('deviceType', 'default')
        self.byte_order = Endian.Big if kwargs.get('byteOrder', 'LITTLE').upper() == "BIG" else Endian.Little
        self.word_order = Endian.Big if kwargs.get('wordOrder', 'LITTLE').upper() == "BIG" else Endian.Little
        self.telemetry = kwargs.get('timeseries', [])
        self.attributes = kwargs.get('attributes', [])
