import re

import simplejson


class DeviceFilter:
    def __init__(self, config_path):
        self._config_path = config_path
        self._config = self._load_config()

    def _load_config(self):
        if self._config_path:
            with open(self._config_path, 'r') as file:
                return simplejson.load(file)

        return {'deny': {}, 'allow': {}}

    def validate_device(self, connector_name, data):
        for con_name, device_list in self._config['deny'].items():
            if con_name == connector_name:
                for device in device_list:
                    if re.fullmatch(device, data['deviceName']):
                        return False

        for con_name, device_list in self._config['allow'].items():
            if con_name == connector_name:
                for device in device_list:
                    if re.fullmatch(device, data['deviceName']):
                        return True

        return True
