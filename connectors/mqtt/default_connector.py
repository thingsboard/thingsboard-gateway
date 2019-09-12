import json
import logging
import threading
import time

try:
    from tb_client.tb_device_mqtt import TBDeviceMqttClient
except ModuleNotFoundError:
    import sys
    sys.path.append('/home/zenx/Documents/ThingsBoard/main_TB_gateway/ThingsBoard-Gateway')
    from tb_client.tb_device_mqtt import TBDeviceMqttClient


log = logging.getLogger(__name__)


class TBNoConfigException(Exception):
    pass


class TBMqttDefaultConnector():
    def __init__(self, extension):
        self.devices = {}
        if extension.get('config'):
            self.get_config(extension)
        else:
            raise TBNoConfigException

        for device in self.devices_config.get('brokers'):
            device_name = device.get('name')
            host = device.get('host')

            if device.get('credentials').get('type') == "anonymous":
                username = ''
                password = ''
            elif device.get('credentials').get('type') == "basic":
                username = device.get('credentials').get('username')
                password = device.get('credentials').get('password')
            elif device.get('credentials').get('type') == 'cert.PEM':
                # TODO ssl
                pass

            port = device.get('port') if device.get('port') else 1883
            client = TBDeviceMqttClient(host)

            self.devices[device_name] = {
                "username": username,
                "password": password,
                "host": host,
                "port": port,
                "client": client
            }
            self.devices[device_name]["client"]._client.username_pw_set(username,password)

        for dev in self.devices:
            current_device = self.devices[dev]
            current_device["thread"] = threading.Thread(name=dev + " thread",
                                                        target=self.device_loop,
                                                        args=[current_device],
                                                        daemon=False)
            current_device["thread"].start()

    def device_loop(self, device):
        log.warning(device["thread"].getName() + ' Started')

        def subscribe_callback(result):
            print(result)

        device["client"].connect(port=device["port"])
        while not device["client"]._TBDeviceMqttClient__is_connected:
            time.sleep(.1)

        device["client"].subscribe_to_all_attributes(subscribe_callback)

        while True:
            time.sleep(1)

    def get_config(self, extension):
        log.info('Loading config for {} extension'.format(extension.get('name')))
        try:
            with open('extensions/mqtt/'+extension.get('config'), 'r') as config_file:
                self.devices_config = json.load(config_file)
                log.debug('Config for {} loaded'.format(extension.get('name')))

        except Exception:
            self.device_config = {}
            log.exception('Exception occured')


if __name__ == '__main__':
    test_extension = {
          "type": "mqtt",
          "config": "default_mqtt.json",
          "enabled": True
        }
    connector = TBMqttDefaultConnector(test_extension)
