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

from asyncio import Future
from unittest.mock import patch, call
from thingsboard_gateway.gateway.constants import ON_ATTRIBUTE_UPDATE_DEFAULT_TIMEOUT
from asyncua.ua import NodeId

from tests.unit.connectors.opcua.opcua_base_test import OpcUABaseTest


class OpcUAAttributeUpdatesTest(OpcUABaseTest):

    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.fake_device = self.create_fake_device('attribute_updates/opcua_config_attribute_update_full_path.json')
        self.connector._OpcUaConnector__device_nodes.append(self.fake_device)

    async def asyncTearDown(self):
        await super().asyncTearDown()

    async def test_on_attribute_updates_full_path(self):
        payload = {"device": self.fake_device.name,
                   "data": {"Frequency": 5}}

        raw_path = r"Root\.Objects\.MyObject\.Frequency"
        node_id = NodeId(13, 2)
        expected_pairs = {"Frequency": '${Root\\.Objects\\.MyObject\\.Frequency}'}

        with patch.object(self.connector, "_OpcUaConnector__resolve_node_id", return_value=node_id) as resolve_mock, \
                patch.object(self.connector,
                             "_OpcUaConnector__write_node_value",
                             return_value=True) as write_mock:
            self.connector.on_attributes_update(payload)
        self.assertEqual(self.fake_device.shared_attributes_keys_value_pairs,
                         expected_pairs)
        resolve_mock.assert_called_once_with(raw_path=raw_path,
                                             device=self.fake_device)
        write_mock.assert_called_once_with(node_id, 5,
                                           timeout=ON_ATTRIBUTE_UPDATE_DEFAULT_TIMEOUT)

    async def test_on_attribute_updates_node_identifier(self):
        self.fake_device = self.create_fake_device(
            "attribute_updates/opcua_config_attribute_update_identifier.json")
        self.connector._OpcUaConnector__device_nodes[:] = [self.fake_device]

        payload = {"device": self.fake_device.name,
                   "data": {"Frequency": 5}}

        raw_path = "ns=2;i=13"
        node_id = NodeId(13, 2)
        expected_pairs = {"Frequency": "${ns=2;i=13}"}

        with patch.object(self.connector, "_OpcUaConnector__resolve_node_id",
                          return_value=node_id) as resolve_mock, \
                patch.object(self.connector,
                             "_OpcUaConnector__write_node_value",
                             return_value=True) as write_mock:
            self.connector.on_attributes_update(payload)

        self.assertEqual(self.fake_device.shared_attributes_keys_value_pairs,
                         expected_pairs)

        resolve_mock.assert_called_once_with(raw_path=raw_path,
                                             device=self.fake_device)
        write_mock.assert_called_once_with(node_id, 5,
                                           timeout=ON_ATTRIBUTE_UPDATE_DEFAULT_TIMEOUT)

    async def test_on_attribute_updates_with_multiple_attribute_updates(self):
        self.fake_device = self.create_fake_device(
            'attribute_updates/opcua_config_on_attribute_update_section_multiple_attribute_updates.json'
        )
        self.connector._OpcUaConnector__device_nodes[:] = [self.fake_device]

        payload = {"device": self.fake_device.name,
                   "data": {"Frequency": 5, "Temperature": 7}}

        expected_pairs = {
            "Frequency": r"${Root\.Objects\.MyObject\.Frequency}",
            "Temperature": r"${ns=2;i=15}",
        }

        freq_raw, temp_raw = r"Root\.Objects\.MyObject\.Frequency", r"ns=2;i=15"
        freq_node, temp_node = NodeId(13, 2), NodeId(15, 2)

        def _resolve_side_effect(*, raw_path, device):
            return freq_node if raw_path == freq_raw else temp_node

        with patch.object(self.connector,"_OpcUaConnector__resolve_node_id", side_effect=_resolve_side_effect) as resolve_mock, \
                patch.object(self.connector,
                             "_OpcUaConnector__write_node_value",
                             return_value=True) as write_mock:
            self.connector.on_attributes_update(payload)

        self.assertEqual(self.fake_device.shared_attributes_keys_value_pairs,
                         expected_pairs)

        resolve_mock.assert_has_calls([
            call(raw_path=freq_raw, device=self.fake_device),
            call(raw_path=temp_raw, device=self.fake_device),
        ], any_order=False)

        write_mock.assert_has_calls([
            call(freq_node, 5,
                 timeout=ON_ATTRIBUTE_UPDATE_DEFAULT_TIMEOUT),
            call(temp_node, 7,
                 timeout=ON_ATTRIBUTE_UPDATE_DEFAULT_TIMEOUT),
        ], any_order=False)

    async def test_attribute_update_with_non_listed_attribute(self):
        self.fake_device = self.create_fake_device(
            'attribute_updates/opcua_config_on_attribute_update_section_multiple_attribute_updates.json'
        )
        self.connector._OpcUaConnector__device_nodes[:] = [self.fake_device]

        payload = {"device": self.fake_device.name,
                   "data": {"Frequency": 5, "Temperature": 7, "abba1965": 20}}

        expected_pairs = {
            "Frequency": r"${Root\.Objects\.MyObject\.Frequency}",
            "Temperature": r"${ns=2;i=15}",
        }

        freq_raw, temp_raw = r"Root\.Objects\.MyObject\.Frequency", r"ns=2;i=15"
        freq_node, temp_node = NodeId(13, 2), NodeId(15, 2)

        def _resolve_side_effect(*, raw_path, device):
            return freq_node if raw_path == freq_raw else temp_node

        with patch.object(self.connector,
                          "_OpcUaConnector__resolve_node_id",
                          side_effect=_resolve_side_effect) as resolve_mock, \
                patch.object(self.connector,
                             "_OpcUaConnector__write_node_value",
                             return_value=True) as write_mock:
            self.connector.on_attributes_update(payload)

        self.assertEqual(self.fake_device.shared_attributes_keys_value_pairs,
                         expected_pairs)

        resolve_mock.assert_has_calls([
            call(raw_path=freq_raw, device=self.fake_device),
            call(raw_path=temp_raw, device=self.fake_device),
        ], any_order=False)

        write_mock.assert_has_calls([
            call(freq_node, 5,
                 timeout=ON_ATTRIBUTE_UPDATE_DEFAULT_TIMEOUT),
            call(temp_node, 7,
                 timeout=ON_ATTRIBUTE_UPDATE_DEFAULT_TIMEOUT),
        ], any_order=False)

    async def test_attribute_updates_no_attribute_section(self):
        self.fake_device = self.create_fake_device(
            'attribute_updates/opcua_config_attribute_update_empty_section.json'
        )
        self.connector._OpcUaConnector__device_nodes[:] = [self.fake_device]
        node_id = NodeId(13, 2)

        payload = {"device": self.fake_device.name,
                   "data": {"Frequency": 5}}
        self.assertEqual(self.fake_device.shared_attributes_keys_value_pairs, {})

        with patch.object(self.connector,"_OpcUaConnector__resolve_node_id",
                          return_value=node_id) as resolve_mock, \
                patch.object(self.connector,
                             "_OpcUaConnector__write_node_value",
                             return_value=True) as write_mock:
            self.connector.on_attributes_update(payload)
        resolve_mock.assert_not_called()
        write_mock.assert_not_called()

    async def test_attribute_update_with_partly_invalid_paths(self):
        self.fake_device = self.create_fake_device(
            "attribute_updates/"
            "opcua_config_on_attribute_update_section_multiple_attribute_updates_invalid_path.json")
        self.connector._OpcUaConnector__device_nodes[:] = [self.fake_device]

        payload = {"device": self.fake_device.name,
                   "data": {"Frequency": 5, "Temperature": 7}}
        expected_pairs = {
            "Frequency": r"${Root\.Objects\.MyObject\.Frequencttrtrtggrtgy}",
            "Temperature": r"${ns=2;i=15}",
        }
        bad_raw = r"Root\.Objects\.MyObject\.Frequencttrtrtggrtgy"
        good_raw = r"ns=2;i=15"

        bad_node = None
        good_node = NodeId(15, 2)

        def resolve_side_effect(*, raw_path, device):
            return good_node if raw_path == good_raw else bad_node

        with patch.object(self.connector,
                          "_OpcUaConnector__resolve_node_id",
                          side_effect=resolve_side_effect) as resolve_mock, \
                patch.object(self.connector,
                             "_OpcUaConnector__write_node_value",
                             return_value=True) as write_mock:
            self.connector.on_attributes_update(payload)
        self.assertEqual(self.fake_device.shared_attributes_keys_value_pairs,
                         expected_pairs)
        self.assertCountEqual(
            [call.kwargs["raw_path"] for call in resolve_mock.mock_calls],
            [bad_raw, good_raw])
        write_mock.assert_called_once_with(
            good_node, 7, timeout=ON_ATTRIBUTE_UPDATE_DEFAULT_TIMEOUT)

    async def test_attribute_is_written(self):
        node_id, value = NodeId(13, 2), 10
        done = Future()
        done.set_result({"value": value})

        with patch.object(
                self.connector._OpcUaConnector__loop, "create_task", return_value=done
        ) as ct_mock:
            result = self.connector._OpcUaConnector__write_node_value(node_id, value, timeout=5)

        ct_mock.assert_called_once()
        self.assertTrue(result)
        self.assertEqual(done.result(), {"value": 10})

    async def test_write_returns_error_dict(self):
        node_id, value = None, 10
        done = Future()
        done.set_result({'error': "'NoneType' object has no attribute 'write_value'"})

        with patch.object(
                self.connector._OpcUaConnector__loop, "create_task", return_value=done,
        ):
            result = self.connector._OpcUaConnector__write_node_value(node_id, value, timeout=5)

        self.assertFalse(result)

    async def test_write_fails_when_create_task_raises(self):
        node_id, value = NodeId(99, 2), 7
        done = Future()
        done.set_result({'error': 'Failed to send request to OPC UA server'})

        with patch.object(
                self.connector._OpcUaConnector__loop, "create_task",
        ) as ct_mock, patch("time.sleep", return_value=None):
            result = self.connector._OpcUaConnector__write_node_value(node_id, value, timeout=5)

        ct_mock.assert_called_once()
        self.assertFalse(result)
