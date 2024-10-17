from copy import deepcopy


class BackwardCompatibilityAdapter:
    def __init__(self, config):
        self._config = deepcopy(config)

    @staticmethod
    def is_old_config(config):
        return config.get('socket') is None

    def convert(self):
        socket_type = self._config.pop('type', 'TCP')
        address = self._config.pop('address', '127.0.0.1')
        port = self._config.pop('port', 50000)
        buffer_size = self._config.pop('bufferSize', 1024)

        self._config['socket'] = {
            'type': socket_type,
            'address': address,
            'port': port,
            'bufferSize': buffer_size
        }

        for device in self._config.get('devices', []):
            if device.get('addressFilter'):
                address_filter = device.pop('addressFilter')
                device['address'] = address_filter

            for attribute_requests in device.get('attributeRequests', []):
                attribute_requests['requestExpressionSource'] = 'expression'
                attribute_requests['attributeNameExpressionSource'] = 'expression'

        return self._config
