from thingsboard_gateway.connectors.connector import Connector, log
from thingsboard_gateway.connectors.mqtt.json_mqtt_uplink_converter import JsonMqttUplinkConverter
from threading import Thread
from thingsboard_gateway.tb_utility.tb_utility import TBUtility


'''

url: http://127.0.0.1/test_device

method: POST

HEADER: 

Authorization: Basic dXNlcjpwYXNzd2Q=

BODY:

body = {
            "name": "number 2",
            "sensorModel": "0AF0CE",
            "temp": 25.8,
        }

mapping_dict = {
                "/test_device":
                    {
                        "converter":JsonRestUplinkConverter(config["converter"]),
                        "rpc":"...",
                        "attributeUpdates":"..."
                    }
                }

mapping_dict["/test_device"]["converter"].convert("/test_device", body)

#TODO: 
1 Create endpoints (with/without authorization and methods from the config)
1.1 Initialize converters for endpoints
1.2 Create a dictionary for data processing 
2 Run application on open() function on host and port from the config.
3 On receiving message: convert data and send_to_storage 


'''



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