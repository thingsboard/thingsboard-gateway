import logging
import time
import json
from gateway.tb_gateway_mqtt import TBGatewayMqttClient
import connectors
log = logging.getLogger(__name__)


class GatewayClient(TBGatewayMqttClient):

    def __init__(self):
        self.__read_config('tb_gateway.json')
        self.port = self._val_or_def(self.gateway_config.get('port'), 1883)
        self.keepalive = self._val_or_def(self.gateway_config.get('keepalive'), 60)
        super().__init__(self.gateway_config.get('host'), self.gateway_config.get('token'))
        self._client.on_message = self._on_message


    def _on_message(self, client, userdata, message):
        content = self._decode(message)
        super()._on_decoded_message(content, message)
        self._on_decoded_message(content, message)


    def _on_decoded_message(self, content, message):
        super()._on_decoded_message(content,message)
        if content.get('configuration'):
            configuration_items = json.loads(content.get('configuration'))
            self.devices_configs = {config['type'].lower(): {} for config in configuration_items if config['type'].lower() not in configuration_items}
            for config in configuration_items:
                self.devices_configs[config['type'].lower()][config['id']] = config['configuration']
                log.debug('Received config for device_id: '+config.get('id'))
            for protocol in self.devices_configs:
                log.debug(protocol)
                log.debug(self.devices_configs[protocol])
                config_json = ""
                for device in (self.devices_configs[protocol]):
                    config_json = config_json + json.dumps(self.devices_configs[protocol][device])
                    conf_filename = 'extensions/' + protocol + '/' + device + '.json'
                    log.debug('Configuration updated for ' + device + ' to file ' + conf_filename)
                    with open(conf_filename, 'w+') as config_file:
                        config_file.write(config_json)



    def __read_config(self,config_file):
        with open('config/'+config_file, 'r') as config_file:
            self.gateway_config = json.load(config_file)

    def _val_or_def(self,value,default):
        return value if value is not None else default

    def get_devices(self):
        return self._TBGatewayMqttClient__connected_devices
