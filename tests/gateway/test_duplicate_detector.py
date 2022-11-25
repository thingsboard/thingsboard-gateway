#      Copyright 2022. ThingsBoard
#  #
#      Licensed under the Apache License, Version 2.0 (the "License");
#      you may not use this file except in compliance with the License.
#      You may obtain a copy of the License at
#  #
#          http://www.apache.org/licenses/LICENSE-2.0
#  #
#      Unless required by applicable law or agreed to in writing, software
#      distributed under the License is distributed on an "AS IS" BASIS,
#      WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#      See the License for the specific language governing permissions and
#      limitations under the License.

import unittest
from time import time

from thingsboard_gateway.gateway.constants import *
from thingsboard_gateway.gateway.duplicate_detector import DuplicateDetector


class Connector:
    def __init__(self, enable_filtering):
        self._enable_filtering = enable_filtering

    def is_filtering_enable(self, device_name):
        return self._enable_filtering


class TestDuplicateDetector(unittest.TestCase):

    TEST_DEVICE_NAME = "Test device"
    TEST_DEVICE_TYPE = "TimeMachine"

    @staticmethod
    def _to_dict(dicts_in_array):
        result_dict = {}
        for attribute in dicts_in_array:
            for key, value in attribute.items():
                result_dict[key] = value
        return result_dict

    def _is_dicts_equal(self, actual, expected):
        self.assertEqual(len(actual), len(expected))
        diff = set(actual.items()) ^ set(expected.items())
        self.assertTrue(len(diff) == 0)

    def _is_data_packets_equal(self, actual, expected):
        self.assertIsNotNone(actual)
        self.assertEqual(actual.get(DEVICE_NAME_PARAMETER), expected.get(DEVICE_NAME_PARAMETER))
        self.assertEqual(actual.get(DEVICE_TYPE_PARAMETER), expected.get(DEVICE_TYPE_PARAMETER))

        actual_attributes = TestDuplicateDetector._to_dict(actual.get(ATTRIBUTES_PARAMETER))
        expected_attributes = TestDuplicateDetector._to_dict(expected.get(ATTRIBUTES_PARAMETER))

        self._is_dicts_equal(actual_attributes, expected_attributes)

        actual_telemetry = actual.get(TELEMETRY_PARAMETER)
        expected_telemetry = expected.get(TELEMETRY_PARAMETER)

        self.assertEqual(len(actual_telemetry), len(expected_telemetry))

    @staticmethod
    def _create_data_packet(attributes=None, telemetry=None):
        return {
            DEVICE_NAME_PARAMETER: TestDuplicateDetector.TEST_DEVICE_NAME,
            DEVICE_TYPE_PARAMETER: TestDuplicateDetector.TEST_DEVICE_TYPE,
            ATTRIBUTES_PARAMETER: attributes if attributes else [TestDuplicateDetector._create_attributes()],
            TELEMETRY_PARAMETER: telemetry if telemetry else [TestDuplicateDetector._create_telemetry()]
        }

    @staticmethod
    def _create_attributes():
        return {'testAttribute': 100500}

    @staticmethod
    def _create_telemetry():
        return {'testTelemetry': 12345}

    @staticmethod
    def _create_ts_telemetry():
        return {
            TELEMETRY_TIMESTAMP_PARAMETER: 1668624816000,
            TELEMETRY_VALUES_PARAMETER: TestDuplicateDetector._create_telemetry()
        }

    def setUp(self):
        self.connectors = {}
        self._duplicate_detector = DuplicateDetector(self.connectors)

    def test_in_data_filter_disable(self):
        expected_data = self._create_data_packet()
        expected_data[SEND_ON_CHANGE_PARAMETER] = False
        actual_data1 = self._duplicate_detector.filter_data("some_connector", expected_data)
        self._is_data_packets_equal(actual_data1, expected_data)
        actual_data2 = self._duplicate_detector.filter_data("some_connector", expected_data)
        self._is_data_packets_equal(actual_data2, expected_data)

    def test_in_data_filter_enable(self):
        expected_data = self._create_data_packet()
        expected_data[SEND_ON_CHANGE_PARAMETER] = True
        actual_data1 = self._duplicate_detector.filter_data("some_connector", expected_data)
        self._is_data_packets_equal(actual_data1, expected_data)
        actual_data2 = self._duplicate_detector.filter_data("some_connector", expected_data)
        self.assertIsNone(actual_data2)

    def test_dont_filter_data_from_unknown_connector(self):
        expected_data = self._create_data_packet()
        actual_data1 = self._duplicate_detector.filter_data("unknown_connector", expected_data)
        self._is_data_packets_equal(actual_data1, expected_data)
        actual_data2 = self._duplicate_detector.filter_data("unknown_connector", expected_data)
        self._is_data_packets_equal(actual_data2, expected_data)

    def test_connector_with_disable_filtering(self):
        connector_name = "some_connector"
        self.connectors[connector_name] = Connector(False)

        expected_data = self._create_data_packet()
        actual_data1 = self._duplicate_detector.filter_data(connector_name, expected_data)
        self._is_data_packets_equal(actual_data1, expected_data)
        actual_data2 = self._duplicate_detector.filter_data(connector_name, expected_data)
        self._is_data_packets_equal(actual_data2, expected_data)

    def test_connector_with_enable_filtering(self):
        connector_name = "some_connector"
        self.connectors[connector_name] = Connector(True)

        expected_data = self._create_data_packet()
        actual_data1 = self._duplicate_detector.filter_data(connector_name, expected_data)
        self._is_data_packets_equal(actual_data1, expected_data)
        actual_data2 = self._duplicate_detector.filter_data(connector_name, expected_data)
        self.assertIsNone(actual_data2)

    def test_one_unchanged_and_one_changed_attributes(self):
        unchanged_attribute = {"UnchangedAttr": 1}
        changed_attr_name = "ChangedAttr"
        changed_attribute_value1 = {changed_attr_name: 10}
        changed_attribute_value2 = {changed_attr_name: 20}
        expected_attributes1 = [unchanged_attribute, changed_attribute_value1]
        expected_attributes2 = [unchanged_attribute, changed_attribute_value2]

        expected_data1 = self._create_data_packet(expected_attributes1, [])
        expected_data1[SEND_ON_CHANGE_PARAMETER] = True
        expected_data2 = self._create_data_packet(expected_attributes2, [])
        expected_data2[SEND_ON_CHANGE_PARAMETER] = True

        actual_data1 = self._duplicate_detector.filter_data("some_connector", expected_data1)
        expected_data1.pop(SEND_ON_CHANGE_PARAMETER, None)
        self.assertEqual(actual_data1, expected_data1)

        actual_data2 = self._duplicate_detector.filter_data("some_connector", expected_data2)
        actual_attributes = actual_data2.get(ATTRIBUTES_PARAMETER)
        self.assertNotEqual(len(actual_attributes), len(expected_attributes2))
        self.assertTrue(len(actual_attributes) == 1)
        self._is_dicts_equal(actual_attributes[0], changed_attribute_value2)

    def test_one_unchanged_and_one_changed_telemetry_without_timestamp(self):
        unchanged_telemetry = {"UnchangedTelemetry": 1}
        changed_telemetry_name = "ChangedTelemetry"
        changed_telemetry_value1 = {changed_telemetry_name: 10}
        changed_telemetry_value2 = {changed_telemetry_name: 20}
        expected_telemetry1 = [unchanged_telemetry, changed_telemetry_value1]
        expected_telemetry2 = [unchanged_telemetry, changed_telemetry_value2]

        expected_data1 = self._create_data_packet([], expected_telemetry1)
        expected_data1[SEND_ON_CHANGE_PARAMETER] = True
        expected_data2 = self._create_data_packet([], expected_telemetry2)
        expected_data2[SEND_ON_CHANGE_PARAMETER] = True

        actual_data1 = self._duplicate_detector.filter_data("some_connector", expected_data1)
        expected_data1.pop(SEND_ON_CHANGE_PARAMETER, None)
        self.assertEqual(actual_data1, expected_data1)

        actual_data2 = self._duplicate_detector.filter_data("some_connector", expected_data2)
        actual_telemetry = actual_data2.get(TELEMETRY_PARAMETER)
        self.assertNotEqual(len(actual_telemetry), len(expected_telemetry2))
        self.assertTrue(len(actual_telemetry) == 1)
        self._is_dicts_equal(actual_telemetry[0], changed_telemetry_value2)

    def test_one_unchanged_and_one_changed_telemetry_with_timestamp(self):
        unchanged_telemetry = {
            TELEMETRY_TIMESTAMP_PARAMETER: time(),
            TELEMETRY_VALUES_PARAMETER: {"UnchangedTelemetry": 1}
        }
        changed_telemetry_name = "ChangedTelemetry"
        changed_telemetry_value1 = {
            TELEMETRY_TIMESTAMP_PARAMETER: time(),
            TELEMETRY_VALUES_PARAMETER: {changed_telemetry_name: 10}
        }
        changed_telemetry_value2 = {
            TELEMETRY_TIMESTAMP_PARAMETER: time(),
            TELEMETRY_VALUES_PARAMETER: {changed_telemetry_name: 20}
        }
        expected_telemetry1 = [unchanged_telemetry, changed_telemetry_value1]
        expected_telemetry2 = [unchanged_telemetry, changed_telemetry_value2]

        expected_data1 = self._create_data_packet([], expected_telemetry1)
        expected_data1[SEND_ON_CHANGE_PARAMETER] = True
        expected_data2 = self._create_data_packet([], expected_telemetry2)
        expected_data2[SEND_ON_CHANGE_PARAMETER] = True

        actual_data1 = self._duplicate_detector.filter_data("some_connector", expected_data1)
        expected_data1.pop(SEND_ON_CHANGE_PARAMETER, None)
        self.assertEqual(actual_data1, expected_data1)

        actual_data2 = self._duplicate_detector.filter_data("some_connector", expected_data2)
        actual_telemetry = actual_data2.get(TELEMETRY_PARAMETER)
        self.assertNotEqual(len(actual_telemetry), len(expected_telemetry2))
        self.assertTrue(len(actual_telemetry) == 1)
        self.assertEqual(actual_telemetry[0].get(TELEMETRY_TIMESTAMP_PARAMETER), changed_telemetry_value2.get(TELEMETRY_TIMESTAMP_PARAMETER))
        self._is_dicts_equal(actual_telemetry[0].get(TELEMETRY_VALUES_PARAMETER), changed_telemetry_value2.get(TELEMETRY_VALUES_PARAMETER))

    def test_device_deletion(self):
        connector_name = "some_connector"
        self.connectors[connector_name] = Connector(True)

        expected_data = self._create_data_packet()
        actual_data1 = self._duplicate_detector.filter_data(connector_name, expected_data)
        self._is_data_packets_equal(actual_data1, expected_data)
        actual_data2 = self._duplicate_detector.filter_data(connector_name, expected_data)
        self.assertIsNone(actual_data2)

        self._duplicate_detector.delete_device(TestDuplicateDetector.TEST_DEVICE_NAME)
        actual_data3 = self._duplicate_detector.filter_data(connector_name, expected_data)
        self._is_data_packets_equal(actual_data3, expected_data)

    def test_device_renaming(self):
        connector_name = "some_connector"
        self.connectors[connector_name] = Connector(True)

        expected_data = self._create_data_packet()
        actual_data1 = self._duplicate_detector.filter_data(connector_name, expected_data)
        self._is_data_packets_equal(actual_data1, expected_data)
        actual_data2 = self._duplicate_detector.filter_data(connector_name, expected_data)
        self.assertIsNone(actual_data2)

        new_device_name = "New device name"
        self._duplicate_detector.rename_device(TestDuplicateDetector.TEST_DEVICE_NAME, new_device_name)
        expected_data[DEVICE_NAME_PARAMETER] = new_device_name
        actual_data3 = self._duplicate_detector.filter_data(connector_name, expected_data)
        self.assertIsNone(actual_data3)


if __name__ == '__main__':
    unittest.main()