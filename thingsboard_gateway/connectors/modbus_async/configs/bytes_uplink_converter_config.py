class BytesUplinkConverterConfig:
    def __init__(self, **kwargs):
        self.device_name = kwargs['deviceName']
        self.device_type = kwargs.get('deviceType', 'default')
        self.byte_order = kwargs.get('byteOrder', 'LITTLE')
        self.word_order = kwargs.get('wordOrder', 'LITTLE')
        self.telemetry = kwargs.get('timeseries', [])
        self.attributes = kwargs.get('attributes', [])
