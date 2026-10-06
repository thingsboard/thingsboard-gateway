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

from thingsboard_gateway.connectors.bacnet.application import Application


class TestApplicationReadFallback(IsolatedAsyncioTestCase):

    async def asyncSetUp(self):
        self.app = Application.__new__(Application)
        self.app._Application__log = logging.getLogger("bacnet application fallback tests")

    async def test_read_multiple_objects_falls_back_to_read_property(self):
        device = SimpleNamespace(details=SimpleNamespace(vendor_id=0, address="192.168.1.199:47808", object_id=101))
        object_list = [{
            "objectType": "analogInput",
            "objectId": "5",
            "propertyId": "presentValue"
        }]

        async def fake_send_wrapper(func, err_msg=None, *args, **kwargs):
            if func.__name__ == "request":
                return None
            if func.__name__ == "read_property":
                return 42.5
            return None

        self.app._Application__send_request_wrapper = AsyncMock(side_effect=fake_send_wrapper)

        result = await self.app.read_multiple_objects(device, object_list)

        self.assertEqual(len(result), 1)
        self.assertEqual(str(result[0][0]), "analog-input,5")
        self.assertEqual(str(result[0][1]), "present-value")
        self.assertEqual(result[0][3], 42.5)

    async def test_read_multiple_objects_returns_empty_when_fallback_fails(self):
        device = SimpleNamespace(details=SimpleNamespace(vendor_id=0, address="192.168.1.199:47808", object_id=101))
        object_list = [{
            "objectType": "analogInput",
            "objectId": "5",
            "propertyId": "presentValue"
        }]

        self.app._Application__send_request_wrapper = AsyncMock(return_value=None)

        result = await self.app.read_multiple_objects(device, object_list)

        self.assertEqual(result, [])
