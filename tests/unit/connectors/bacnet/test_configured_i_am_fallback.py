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

import logging
from types import SimpleNamespace
from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock

from bacpypes3.basetypes import Segmentation
from bacpypes3.pdu import Address

from thingsboard_gateway.connectors.bacnet.bacnet_connector import AsyncBACnetConnector


class TestConfiguredIAmFallback(IsolatedAsyncioTestCase):

    async def asyncSetUp(self):
        self.connector = AsyncBACnetConnector.__new__(AsyncBACnetConnector)
        self.connector._AsyncBACnetConnector__log = logging.getLogger("bacnet configured i-am tests")
        self.connector._AsyncBACnetConnector__application = AsyncMock()
        self.connector._AsyncBACnetConnector__stopped = False

    async def test_build_i_am_from_device_config_without_probe(self):
        device_config = {
            "objectName": "manual-device",
            "vendorIdentifier": "221",
            "maxApduLengthAccepted": "2048",
            "segmentationSupported": "segmentedBoth"
        }

        apdu = await self.connector._AsyncBACnetConnector__build_i_am_like_apdu(
            Address("192.168.1.199:47808"),
            101,
            device_config
        )

        self.assertIsNotNone(apdu)
        self.assertEqual(apdu.iAmDeviceIdentifier, ("device", 101))
        self.assertEqual(apdu.deviceName, "manual-device")
        self.assertEqual(apdu.vendorID, 221)
        self.assertEqual(apdu.maxAPDULengthAccepted, 2048)
        self.assertEqual(apdu.segmentationSupported, Segmentation.segmentedBoth)
        self.connector._AsyncBACnetConnector__application.read_property.assert_not_called()

    async def test_build_i_am_returns_defaults_without_probe(self):
        self.connector._AsyncBACnetConnector__application.read_property = AsyncMock(side_effect=RuntimeError("No response"))

        apdu = await self.connector._AsyncBACnetConnector__build_i_am_like_apdu(
            Address("192.168.1.200:47808"),
            102,
            {}
        )

        self.assertIsNotNone(apdu)
        self.assertEqual(apdu.deviceName, "102")
        self.assertEqual(apdu.vendorID, 0)
        self.assertEqual(apdu.maxAPDULengthAccepted, 50)
        self.assertEqual(apdu.segmentationSupported, Segmentation.noSegmentation)
        self.assertEqual(self.connector._AsyncBACnetConnector__application.read_property.await_count, 4)

    async def test_build_i_am_with_partial_config_and_probe_fill(self):
        self.connector._AsyncBACnetConnector__application.read_property = AsyncMock(side_effect=[
            "probed-device-name",
            1476,
            Segmentation.segmentedTransmit
        ])

        device_config = {
            "vendorIdentifier": 99
        }

        apdu = await self.connector._AsyncBACnetConnector__build_i_am_like_apdu(
            Address("192.168.1.201:47808"),
            103,
            device_config
        )

        self.assertIsNotNone(apdu)
        self.assertEqual(apdu.deviceName, "probed-device-name")
        self.assertEqual(apdu.vendorID, 99)
        self.assertEqual(apdu.maxAPDULengthAccepted, 1476)
        self.assertEqual(apdu.segmentationSupported, Segmentation.segmentedTransmit)
        self.assertEqual(
            [call.args[2] for call in self.connector._AsyncBACnetConnector__application.read_property.await_args_list],
            ["objectName", "maxApduLengthAccepted", "segmentationSupported"]
        )

    async def test_discover_devices_defaults_to_setup_without_discovery(self):
        self.connector._AsyncBACnetConnector__config = {
            "devices": [
                {
                    "address": "192.168.1.202:47808",
                    "deviceId": 104
                }
            ]
        }
        self.connector._AsyncBACnetConnector__add_configured_device_without_iam = AsyncMock()

        await self.connector._AsyncBACnetConnector__discover_devices()

        self.connector._AsyncBACnetConnector__add_configured_device_without_iam.assert_awaited_once()
        self.connector._AsyncBACnetConnector__application.do_who_is.assert_not_awaited()

    async def test_discover_devices_runs_who_is_when_setup_without_discovery_disabled(self):
        self.connector._AsyncBACnetConnector__config = {
            "devices": [
                {
                    "address": "192.168.1.202:47808",
                    "deviceId": 104,
                    "setupWithoutDiscovery": False
                }
            ]
        }
        self.connector._AsyncBACnetConnector__add_configured_device_without_iam = AsyncMock()

        await self.connector._AsyncBACnetConnector__discover_devices()

        self.connector._AsyncBACnetConnector__application.do_who_is.assert_awaited_once_with(
            device_address="192.168.1.202:47808"
        )
        self.connector._AsyncBACnetConnector__add_configured_device_without_iam.assert_not_awaited()

    async def test_discover_devices_uses_who_is_for_pattern_address(self):
        self.connector._AsyncBACnetConnector__config = {
            "devices": [
                {
                    "address": "192.168.1.X:47808",
                    "deviceId": 104
                }
            ]
        }
        self.connector._AsyncBACnetConnector__add_configured_device_without_iam = AsyncMock()

        await self.connector._AsyncBACnetConnector__discover_devices()

        self.connector._AsyncBACnetConnector__application.do_who_is.assert_awaited_once()
        self.connector._AsyncBACnetConnector__add_configured_device_without_iam.assert_not_awaited()

    async def test_set_additional_device_info_keeps_manual_name_when_probe_fails(self):
        self.connector._AsyncBACnetConnector__application.get_device_name = AsyncMock(return_value=None)
        apdu = SimpleNamespace(
            pduSource=Address("192.168.1.203:47808"),
            iAmDeviceIdentifier=("device", 105),
            deviceName="manual-name"
        )
        device_config = {
            "deviceInfo": {
                "deviceNameExpression": "BACnet Device ${objectName}",
                "deviceProfileExpression": "default"
            }
        }

        await self.connector._AsyncBACnetConnector__set_additional_device_info_to_apdu(apdu, device_config)

        self.assertEqual(apdu.deviceName, "manual-name")
