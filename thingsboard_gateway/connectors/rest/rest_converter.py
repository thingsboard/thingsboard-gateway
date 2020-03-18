from thingsboard_gateway.connectors.converter import Converter, log


class RestConverter(Converter):
    def __init__(self, config):
        super().__init__()
        self.__log = log
        self.__config = config

    def convert(self, config, data):
        # Example
        # dict_result = {"deviceName": None, "deviceType": None, "attributes": [], "telemetry": []}
        result = {"deviceName": config['devices']['test']['name'],
                  "deviceType": config['devices']['test']['type'],
                  # "attributes": config['devices']['test']['attributes'],
                  "attributes":[],
                  "telemetry": [data], }
        return result





