import logging
import time
from json import load,dumps
from gateway.tb_gateway_mqtt import TBGatewayMqttClient
import connectors
log = logging.getLogger(__name__)

test_data = {"test_attribute":"test_value"}



class GatewayClient(TBGatewayMqttClient):

    def __init__(self):

        self.__read_config('TB_gateway.json')
        self.port = self._val_or_def(self.config.get('port'),1883)
        self.keepalive = self._val_or_def(self.config.get('keepalive'),60)
        super().__init__(self.config.get('host'),self.config.get('token'))

    def __read_config(self,config_file):
        with open('config/'+config_file,'r') as config_file:
            self.config = load(config_file)

    def _val_or_def(self,value,default):
        return value if value is not None else default
