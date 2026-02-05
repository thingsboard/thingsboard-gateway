#     Copyright 2026. ThingsBoard
#
#     Licensed under the Apache License, Version 2.0 (the "License");
#     you may not use this file except in compliance with the License.
#     You may obtain a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#     Unless required by applicable law or agreed to in writing, software
#     distributed under the License is distributed on an "AS IS" BASIS,
#     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#     See the License for the specific language governing permissions and
#     limitations under the License.

import unittest
from os import path
from tests.unit.BaseUnitTest import BaseUnitTest
from simplejson import load
from thingsboard_gateway.connectors.ftp.backward_compatibility_adapter import FTPBackwardCompatibilityAdapter


class FtpBackwardCompatibilityAdapterTests(BaseUnitTest):
    CONFIG_PATH = path.join(path.dirname(path.dirname(path.dirname(path.abspath(__file__)))),
                            "connectors" + path.sep + "ftp" + path.sep + "data" + path.sep)

    @staticmethod
    def load_configuration(config_path):
        with open(config_path, 'r') as file:
            config = load(file)

        return config

    def setUp(self):
        self.maxDiff = 8000
        self.adapter = FTPBackwardCompatibilityAdapter(config={})

    def test_convert_with_anonym_type_in_config(self):
        self.adapter._config = self.load_configuration(self.CONFIG_PATH + 'anonym_type/old_config.json')
        result = self.adapter.convert()
        expected_result = self.load_configuration(self.CONFIG_PATH + 'anonym_type/new_config.json')
        self.assertEqual(result, expected_result)

    def test_convert_with_basic_type_in_config(self):
        self.adapter._config = self.load_configuration(self.CONFIG_PATH + 'basic_type/old_config.json')
        result = self.adapter.convert()
        expected_result = self.load_configuration(self.CONFIG_PATH + 'basic_type/new_config.json')
        self.assertEqual(result, expected_result)

    def test_convert_with_disconnectRequests_in_config(self):
        self.adapter._config = self.load_configuration(self.CONFIG_PATH + 'attribute_updates/old_config.json')
        result = self.adapter.convert()
        expected_result = self.load_configuration(self.CONFIG_PATH + 'attribute_updates/new_config.json')
        self.assertEqual(result, expected_result)


if __name__ == '__main__':
    unittest.main()
