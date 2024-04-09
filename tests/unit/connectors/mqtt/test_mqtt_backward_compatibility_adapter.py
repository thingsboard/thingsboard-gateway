import unittest
from os import path
from tests.unit.BaseUnitTest import BaseUnitTest
from simplejson import load
from thingsboard_gateway.connectors.mqtt.backward_compatibility_adapter import BackwardCompatibilityAdapter


class MqttBackwardCompatibilityAdapterTests(BaseUnitTest):
    CONFIG_PATH = path.join(path.dirname(path.dirname(path.dirname(path.abspath(__file__)))),
                            "connectors" + path.sep + "mqtt" + path.sep + "data" + path.sep)

    @staticmethod
    def convert_json(config_path):
        with open(config_path, 'r') as file:
            config = load(file)

        return config

    def setUp(self):
        self.maxDiff = 8000
        self.adapter = BackwardCompatibilityAdapter(config={})

    def test_convert_with_empty_config(self):
        result = self.adapter.convert()
        expected_result = {
            'dataMapping': {},
            'requestsMapping': {
                'attributeRequests': {},
                'attributeUpdates': {},
                'connectRequests': {},
                'disconnectRequests': {},
                'serverSideRpc': {}
            }
        }
        self.assertEqual(result, expected_result)

    def test_convert_with_mapping_in_config(self):
        self.adapter._config = self.convert_json(self.CONFIG_PATH + 'mapping/old_config.json')
        result = self.adapter.convert()
        expected_result = self.convert_json(self.CONFIG_PATH + 'mapping/new_config.json')
        self.assertEqual(result, expected_result)

    def test_convert_with_connectRequests_in_config(self):
        self.adapter._config = self.convert_json(self.CONFIG_PATH + 'connect_requests/old_config.json')
        result = self.adapter.convert()
        expected_result = self.convert_json(self.CONFIG_PATH + 'connect_requests/new_config.json')
        self.assertEqual(result, expected_result)

    def test_convert_with_disconnectRequests_in_config(self):
        self.adapter._config = self.convert_json(self.CONFIG_PATH + 'disconnect_requests/old_config.json')
        result = self.adapter.convert()
        expected_result = self.convert_json(self.CONFIG_PATH + 'disconnect_requests/new_config.json')
        self.assertEqual(result, expected_result)

    def test_convert_with_attributeRequests_in_config(self):
        self.adapter._config = self.convert_json(self.CONFIG_PATH + 'attribute_requests/old_config.json')
        result = self.adapter.convert()
        expected_result = self.convert_json(self.CONFIG_PATH + 'attribute_requests/new_config.json')
        self.assertEqual(result, expected_result)

    def test_convert_with_attributeUpdates_in_config(self):
        self.adapter._config = self.convert_json(self.CONFIG_PATH + 'attribute_updates/old_config.json')
        result = self.adapter.convert()
        expected_result = self.convert_json(self.CONFIG_PATH + 'attribute_updates/new_config.json')
        self.assertEqual(result, expected_result)

    def test_convert_with_serverSideRpc_in_config(self):
        self.adapter._config = self.convert_json(self.CONFIG_PATH + 'server_side_rpc/old_config.json')
        result = self.adapter.convert()
        expected_result = self.convert_json(self.CONFIG_PATH + 'server_side_rpc/new_config.json')
        self.assertEqual(result, expected_result)

    def test_is_old_config_format_with_mapping(self):
        self.adapter._config = {'mapping': []}
        result = self.adapter.is_old_config_format(self.adapter._config)
        self.assertTrue(result)

    def test_is_old_config_format_without_mapping(self):
        self.adapter._config = {'mappingData': []}
        result = self.adapter.is_old_config_format(self.adapter._config)
        self.assertFalse(result)


if __name__ == '__main__':
    unittest.main()
