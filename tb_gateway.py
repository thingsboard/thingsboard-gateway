from tb_device_mqtt import TBDeviceMqttClient
from tb_gateway_mqtt import TBGatewayMqttClient

class TBGateway:
    def __init__(self, config_file):
        with open(config_file) as config:
            pass
    def start(self):
        #order matters?
        #init storage?
        #init gateway client
        #init extensions if enabled
        pass
