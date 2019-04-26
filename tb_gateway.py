from tb_gateway_mqtt import TBGatewayMqttClient
from json import load

class TBGateway:
    def __init__(self, config_file):
        with open(config_file) as config:
            config = load(config)
            # initialize client
            host = config["host"]
            token = config["token"]
            mqtt = TBGatewayMqttClient(host, token)
            mqtt.connect()
            # init storage

            # init extensions


def main(config_file):
    if __name__ == "main":
        TBGateway(config_file)
