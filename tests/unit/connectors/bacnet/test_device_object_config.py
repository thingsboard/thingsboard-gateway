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

from bacpypes3.pdu import Address

from tests.unit.BaseUnitTest import BaseUnitTest
from thingsboard_gateway.connectors.bacnet.entities.device_object_config import DeviceObjectConfig


class DeviceObjectConfigAddressTests(BaseUnitTest):
    def test_host_and_port(self):
        config = {"host": "192.168.1.10", "port": 47808}
        DeviceObjectConfig.update_address_in_config_util(config)
        self.assertEqual(config["address"], "192.168.1.10:47808")
        self.assertNotIn("host", config)
        self.assertNotIn("port", config)

    def test_host_mask_and_port(self):
        config = {"host": "192.168.1.10", "mask": "24", "port": "47809"}
        DeviceObjectConfig.update_address_in_config_util(config)
        self.assertEqual(config["address"], "192.168.1.10/24:47809")

    def test_existing_address_is_kept(self):
        config = {"address": "10.0.0.1:47808", "host": "192.168.1.10", "port": 47808}
        DeviceObjectConfig.update_address_in_config_util(config)
        self.assertEqual(config["address"], "10.0.0.1:47808")

    def test_empty_port_is_omitted(self):
        for port in ("", "  ", None):
            with self.subTest(port=port):
                config = {"host": "192.168.1.10", "port": port}
                DeviceObjectConfig.update_address_in_config_util(config)
                self.assertEqual(config["address"], "192.168.1.10")
                self.assertNotIn("port", config)

    def test_routed_address_with_empty_port_is_valid(self):
        config = {"host": "6661:3@192.168.168.101", "port": ""}
        DeviceObjectConfig.update_address_in_config_util(config)
        self.assertEqual(config["address"], "6661:3@192.168.168.101")
        Address(config["address"])

    def test_routed_address_with_port(self):
        config = {"host": "6661:3@192.168.168.101", "port": 47808}
        DeviceObjectConfig.update_address_in_config_util(config)
        self.assertEqual(config["address"], "6661:3@192.168.168.101:47808")
        Address(config["address"])
