from thingsboard_gateway.connectors.ftp.ftp_converter import FTPConverter


class FTPDownlinkConverter(FTPConverter):
    def __init__(self, config):
        self.__config = config

    def convert(self, config, data):
        pass
