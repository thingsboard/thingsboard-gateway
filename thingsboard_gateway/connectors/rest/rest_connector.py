from thingsboard_gateway.connectors.connector import Connector, log
from thingsboard_gateway.connectors.mqtt.json_mqtt_uplink_converter import JsonMqttUplinkConverter
from threading import Thread
from thingsboard_gateway.tb_utility.tb_utility import TBUtility


class HttpConnector(Connector, Thread):
    def __init__(self, gateway, config, connector_type):
        super().__init__()
        self.__log = log
        self.config = config
        self.__connector_type = connector_type
        self.statistics = {'MessagesReceived': 0,
                           'MessagesSent': 0}
        self.__gateway = gateway

    def open(self):
        pass

    def close(self):
        pass

    def get_name(self):
        pass

    def is_connected(self):
        pass

    def on_attributes_update(self, content):
        pass

    def server_side_rpc_handler(self, content):
        pass
