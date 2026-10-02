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

from tempfile import TemporaryDirectory
from unittest import TestCase
from unittest.mock import Mock

from thingsboard_gateway.tb_utility.tb_gateway_remote_configurator import RemoteConfigurator

STORAGE = {"type": "sqlite", "data_file_path": "./data/", "messages_ttl_in_days": 7}


class TestRemoteStorageConfiguration(TestCase):
    """An unchanged storage configuration must not re-create the storage (thingsboard-gateway#2124)."""

    def setUp(self):
        self.configurator = RemoteConfigurator.__new__(RemoteConfigurator)
        self.configurator._RemoteConfigurator__log = Mock()
        self.configurator._config = {"storage": {**STORAGE, "ts": 1}}
        self.configurator._get_general_config_in_local_format = Mock(return_value={})
        self.directory = TemporaryDirectory()
        gateway = Mock()
        gateway.get_config_path.return_value = self.directory.name + "/"
        gateway.event_storage_types = {"sqlite": Mock()}
        self.configurator._gateway = gateway
        self.event_storage = gateway._event_storage

    def tearDown(self):
        self.directory.cleanup()

    def test_unchanged_configuration_keeps_storage(self):
        self.configurator._handle_storage_configuration_update({**STORAGE, "ts": 2})
        self.event_storage.stop.assert_not_called()
        self.assertIs(self.configurator._gateway._event_storage, self.event_storage)

    def test_configuration_with_fewer_keys_keeps_storage(self):
        self.configurator._config["storage"]["max_records_count"] = 100000
        self.configurator._handle_storage_configuration_update({**STORAGE, "ts": 2})
        self.event_storage.stop.assert_not_called()

    def test_changed_configuration_recreates_storage(self):
        self.configurator._handle_storage_configuration_update({**STORAGE, "messages_ttl_in_days": 30})
        self.event_storage.stop.assert_called()
        self.assertIsNot(self.configurator._gateway._event_storage, self.event_storage)
