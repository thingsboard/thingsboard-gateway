#     Copyright 2025. ThingsBoard
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

from tests.unit.connectors.opcua.opcua_base_test import OpcUABaseTest


class OpcUAConstantsTest(OpcUABaseTest):

    async def asyncSetUp(self):
        await super().asyncSetUp()
        # Prepare connector fields used by __send_constants_for_device
        self.connector._OpcUaConnector__constant_telemetry_sent_devices = set()
        # Avoid touching Thread.name setter; patch accessors instead
        self.connector.get_name = lambda: "test-connector"
        self.connector.get_id = lambda: "test-id"

        # Build device from constants config
        self.fake_device = self.create_fake_device('constants/opcua_config_constants.json')
        self.connector._OpcUaConnector__device_nodes[:] = [self.fake_device]

    async def asyncTearDown(self):
        await super().asyncTearDown()

    async def test_parse_constants_buckets(self):
        # Device should have parsed constants into dedicated buckets
        self.assertTrue(hasattr(self.fake_device, "constant_attributes"))
        self.assertTrue(hasattr(self.fake_device, "constant_timeseries"))

        self.assertEqual(len(self.fake_device.constant_attributes), 1, "Expected one constant attribute")
        self.assertEqual(self.fake_device.constant_attributes[0]["key"], "Customer")
        self.assertEqual(self.fake_device.constant_attributes[0]["value"], "ACME Corp")

        self.assertEqual(len(self.fake_device.constant_timeseries), 1, "Expected one constant timeseries")
        self.assertEqual(self.fake_device.constant_timeseries[0]["key"], "InitialBatchSize")
        self.assertEqual(self.fake_device.constant_timeseries[0]["value"], "12")

    async def test_send_constants_once_and_idempotent(self):
        # First send: attributes + telemetry (one-time)
        self.connector._OpcUaConnector__send_constants_for_device(self.fake_device)

        calls = self.connector._OpcUaConnector__gateway.send_to_storage.call_args_list
        self.assertEqual(len(calls), 1, "Expected 1 send_to_storage call on first dispatch")

        first_data = calls[0][0][2]  # args: (connector_name, connector_id, converted_data)
        first_payload = first_data.to_dict()

        # Attributes include the constant "Customer"
        self.assertIn("attributes", first_payload)
        self.assertEqual(first_payload["attributes"].get("Customer"), "ACME Corp")

        # Telemetry includes one-time constant "InitialBatchSize" cast to integer
        self.assertIn("telemetry", first_payload)
        self.assertEqual(len(first_payload["telemetry"]), 1, "Expected one telemetry entry")
        first_telemetry_entry = first_payload["telemetry"][0]
        self.assertIn("values", first_telemetry_entry)
        self.assertIsInstance(first_telemetry_entry.get("ts"), int, "Telemetry ts must be an integer timestamp")
        self.assertEqual(first_telemetry_entry["values"].get("InitialBatchSize"), 12, "Expected int-casted value 12")

        # Second send: attributes again, telemetry should NOT be re-sent
        self.connector._OpcUaConnector__send_constants_for_device(self.fake_device)

        calls = self.connector._OpcUaConnector__gateway.send_to_storage.call_args_list
        self.assertEqual(len(calls), 2, "Expected 2 send_to_storage calls after second dispatch")

        second_data = calls[1][0][2]
        second_payload = second_data.to_dict()

        # Attributes should still contain the constant "Customer"
        self.assertEqual(second_payload["attributes"].get("Customer"), "ACME Corp")

        # Telemetry should be empty (one-time semantics)
        self.assertIsInstance(second_payload.get("telemetry"), list)
        self.assertEqual(len(second_payload["telemetry"]), 0, "Constant telemetry must be sent only once")