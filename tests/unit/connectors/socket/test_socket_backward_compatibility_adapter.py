import unittest
from os import path
from tests.unit.BaseUnitTest import BaseUnitTest
from simplejson import load
from thingsboard_gateway.connectors.socket.backward_compatibility_adapter import BackwardCompatibilityAdapter


class SocketBackwardCompatibilityAdapterTests(BaseUnitTest):
    CONFIG_PATH = path.join(path.dirname(path.dirname(path.dirname(path.abspath(__file__)))),
                            "connectors" + path.sep + "socket" + path.sep + "data" + path.sep)

    @staticmethod
    def convert_json(config_path):
        with open(config_path, 'r') as file:
            config = load(file)

        return config

    def setUp(self):
        self.maxDiff = 8000
        self.adapter = BackwardCompatibilityAdapter(config={})

    def test_is_old_config(self):
        is_old_config = self.convert_json(self.CONFIG_PATH + 'old_config.json')
        self.assertTrue(is_old_config, True)

    def test_convert_with_empty_config(self):
        result = self.adapter.convert()
        expected_result = {'socket':
                               {'address': '127.0.0.1',
                                'bufferSize': 1024,
                                'port': 50000,
                                'type': 'TCP'}
                           }
        self.assertEqual(expected_result, result)

    def test_convert_from_old_to_new_config(self):
        self.adapter._config = self.convert_json(self.CONFIG_PATH + 'old_config.json')
        result = self.adapter.convert()
        expected_result = self.convert_json(self.CONFIG_PATH + 'new_config.json')
        self.assertEqual(expected_result, result)


if __name__ == '__main__':
    unittest.main()
