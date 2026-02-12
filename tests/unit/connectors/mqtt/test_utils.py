from tests.unit.BaseUnitTest import BaseUnitTest
from thingsboard_gateway.connectors.mqtt.utils import Utils


class MqttUtilsTest(BaseUnitTest):

    def test_regex_conversion_single_group_returns_group(self):
        topic = "/devices/AM123/mytype/data"
        expr = r"^/devices/([^/]+)/mytype/data$"

        self.assertEqual(Utils.get_value_from_topic(topic, expr), "AM123")

    def test_multiple_groups_returns_last_group(self):
        topic = "/tenant/t1/site/kyiv/devices/AM123/mytype/data"
        expr = r"^/tenant/([^/]+)/site/([^/]+)/devices/([^/]+)/mytype/data$"

        self.assertEqual(Utils.get_value_from_topic(topic, expr), "AM123")
        topic2 = "/devices/AM123/mytype/data"
        expr2 = r"^/devices/([^/]+)/([^/]+)/data$"

        self.assertEqual(Utils.get_value_from_topic(topic2, expr2), "mytype")

    def test_regex_conversion_full_match_when_no_groups(self):
        topic = "/devices/AM123/mytype/data"
        expr = r"^/devices/AM123/mytype/data$"

        self.assertEqual(Utils.get_value_from_topic(topic, expr), topic)

    def test_same_regex_semantics_capturing_vs_non_capturing(self):
        topic = "/devices/AM123/mytype/data"
        expr_capturing = r"^/devices/([^/]+)/mytype/data$"
        self.assertEqual(Utils.get_value_from_topic(topic, expr_capturing), "AM123")
        expr_non_capturing = r"^/devices/(?:[^/]+)/mytype/data$"
        self.assertEqual(Utils.get_value_from_topic(topic, expr_non_capturing), topic)

    def test_optional_group_last_index_uses_last_matched_group(self):
        topic = "/devices/AM123/data"
        expr = r"^/devices/([^/]+)(?:/profile/([^/]+))?/data$"
        self.assertEqual(Utils.get_value_from_topic(topic, expr), "AM123")
        topic2 = "/devices/AM123/profile/mytype/data"
        self.assertEqual(Utils.get_value_from_topic(topic2, expr), "mytype")

    def test_regex_conversion_no_match_returns_none(self):
        topic = "/devices/AM123/mytype/data"
        expr = r"^/devices/([^/]+)/other/data$"
        self.assertIsNone(Utils.get_value_from_topic(topic, expr))
