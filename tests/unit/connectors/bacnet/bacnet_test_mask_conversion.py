import logging
from unittest import TestCase

from thingsboard_gateway.connectors.bacnet.bacnet_connector import AsyncBACnetConnector


class TestApplicationMaskValidation(TestCase):
    def setUp(self):
        self.bacnet_connector = AsyncBACnetConnector.__new__(AsyncBACnetConnector)
        self.bacnet_connector._AsyncBACnetConnector__log = logging.getLogger("bacnet mask tests")

        self.default_application_config = {
            "objectName": "TB_gateway123",
            "host": "192.168.1.136",
            "port": "47808",
            "mask": "22",
            "objectIdentifier": 594,
            "vendorIdentifier": 11,
            "maxApduLengthAccepted": 1476,
            "segmentationSupported": "noSegmentation",
            "networkNumber": 1,
            "deviceDiscoveryTimeoutInSec": 5,
        }

    def validate_mask(self, mask_to_test: str):
        connector_config = {"application": dict(self.default_application_config)}
        connector_config["application"]["mask"] = mask_to_test
        self.bacnet_connector._AsyncBACnetConnector__config = connector_config

        is_valid = self.bacnet_connector._AsyncBACnetConnector__is_valid_application_device_section()
        normalized_app_config = self.bacnet_connector._AsyncBACnetConnector__config["application"]
        return is_valid, normalized_app_config

    def test_accepts_cidr_prefixes_1_to_32(self):
        for cidr_prefix in range(1, 33):
            with self.subTest(cidr_prefix=cidr_prefix):
                is_valid, app_config = self.validate_mask(str(cidr_prefix))
                self.assertTrue(is_valid)
                self.assertEqual(app_config["mask"], str(cidr_prefix))

    def test_accepts_cidr_prefix_0_by_current_logic(self):
        is_valid, app_config = self.validate_mask("0")
        self.assertTrue(is_valid)
        self.assertEqual(app_config["mask"], "0")

    def test_out_of_range_numeric_masks_fall_back_to_default_24(self):
        for invalid_prefix in ["33", "100", "999999"]:
            with self.subTest(invalid_prefix=invalid_prefix):
                is_valid, app_config = self.validate_mask(invalid_prefix)
                self.assertTrue(is_valid)
                self.assertEqual(app_config["mask"], "24")

    def test_non_numeric_masks_fall_back_to_default_24(self):
        for invalid_mask in ["-1", "abc", "", "  ", "24/"]:
            with self.subTest(invalid_mask=invalid_mask):
                is_valid, app_config = self.validate_mask(invalid_mask)
                self.assertTrue(is_valid)
                self.assertEqual(app_config["mask"], "24")

    def test_accepts_valid_dotted_decimal_netmasks(self):
        valid_netmasks = [
            "255.255.255.0",  # /24
            "255.255.255.128",  # /25
            "255.255.255.192",  # /26
            "255.255.254.0",  # /23
            "255.255.252.0",  # /22
            "255.0.0.0",  # /8
        ]
        for netmask in valid_netmasks:
            with self.subTest(netmask=netmask):
                is_valid, app_config = self.validate_mask(netmask)
                self.assertTrue(is_valid)
                self.assertEqual(app_config["mask"], netmask)

    def test_invalid_dotted_decimal_netmasks_fall_back_to_default_24(self):
        invalid_netmasks = [
            "255.255.255.256",
            "255.255.255",
        ]
        for netmask in invalid_netmasks:
            with self.subTest(netmask=netmask):
                is_valid, app_config = self.validate_mask(netmask)
                self.assertTrue(is_valid)
                self.assertEqual(app_config["mask"], "24")

