import unittest
from os import path
from tests.unit.BaseUnitTest import BaseUnitTest
from simplejson import load
from thingsboard_gateway.connectors.ftp.backward_compatibility_adapter import FTPBackwardCompatibilityAdapter


class FtpBackwardCompatibilityAdapterTests(BaseUnitTest):
    CONFIG_PATH = path.join(path.dirname(path.dirname(path.dirname(path.abspath(__file__)))),
                            "connectors" + path.sep + "ftp" + path.sep + "data" + path.sep)

    @staticmethod
    def convert_json(config_path):
        with open(config_path, 'r') as file:
            config = load(file)

        return config

    def setUp(self):
        self.maxDiff = 8000
        self.adapter = FTPBackwardCompatibilityAdapter(config={})

    def test_convert_with_anonym_type_in_config(self):
        self.adapter._config = self.convert_json(self.CONFIG_PATH + 'anonym_type/old_config.json')
        result = self.adapter.convert()
        expected_result = self.convert_json(self.CONFIG_PATH + 'anonym_type/new_config.json')
        self.assertEqual(result, expected_result)

    def test_convert_with_basic_type_in_config(self):
        self.adapter._config = self.convert_json(self.CONFIG_PATH + 'basic_type/old_config.json')
        result = self.adapter.convert()
        expected_result = self.convert_json(self.CONFIG_PATH + 'basic_type/new_config.json')
        self.assertEqual(result, expected_result)

    def test_convert_with_disconnectRequests_in_config(self):
        self.adapter._config = self.convert_json(self.CONFIG_PATH + 'attribute_updates/old_config.json')
        result = self.adapter.convert()
        expected_result = self.convert_json(self.CONFIG_PATH + 'attribute_updates/new_config.json')
        self.assertEqual(result, expected_result)


if __name__ == '__main__':
    unittest.main()
