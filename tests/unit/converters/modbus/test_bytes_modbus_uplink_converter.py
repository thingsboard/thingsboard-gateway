import unittest

try:
    from pymodbus.constants import Endian
except (ImportError, ModuleNotFoundError):
    from thingsboard_gateway.tb_utility.tb_utility import TBUtility
    TBUtility.install_package("pymodbus", version="3.0.0", force_install=True)
    from pymodbus.constants import Endian

from pymodbus.payload import BinaryPayloadBuilder

from tests.unit.BaseUnitTest import BaseUnitTest
from thingsboard_gateway.connectors.modbus.bytes_modbus_uplink_converter import BytesModbusUplinkConverter
from thingsboard_gateway.connectors.modbus.entities.bytes_uplink_converter_config import BytesUplinkConverterConfig
from thingsboard_gateway.connectors.modbus.modbus_connector import AsyncModbusConnector


class DummyResponse:
    def __init__(self, registers=None, bits=None):
        self.registers = registers
        self.bits = bits


class ModbusConverterTests(BaseUnitTest):
    def _convert(self, datapoint, responses, section="timeseries", *, byte_order="BIG", word_order="BIG"):
        return self._convert_multiple(
            [datapoint],
            {datapoint["tag"]: responses},
            section=section,
            byte_order=byte_order,
            word_order=word_order,
        )

    def _convert_multiple(self, datapoints, responses, section="timeseries", *,
                          byte_order="BIG", word_order="BIG"):
        slave_config = BytesUplinkConverterConfig(
            self.log,
            deviceName="Modbus Test",
            deviceType="default",
            unitId=1,
            byteOrder=byte_order,
            wordOrder=word_order,
            timeseries=datapoints if section == "timeseries" else [],
            attributes=datapoints if section == "attributes" else [],
        )
        converter = BytesModbusUplinkConverter(slave_config, logger=self.log)
        data = [{
            "telemetry": responses if section == "timeseries" else {},
            "attributes": responses if section == "attributes" else {},
        }]
        return converter, converter.convert(None, data).to_dict()

    @staticmethod
    def _telemetry_values(converted_data):
        return converted_data["telemetry"][0]["values"]

    def test_wide_range_uses_independent_tag_overrides(self):
        datapoint = {
            "tag": "a${address}",
            "tagOverrides": {"7012": "STR1", "7013": "DC voltage"},
            "type": "16uint",
            "functionCode": 3,
            "objectsCount": 1,
            "address": "7012-7013",
        }

        _, result = self._convert(datapoint, [DummyResponse([11, 22])])

        self.assertEqual({"STR1": 11, "DC voltage": 22}, self._telemetry_values(result))

    def test_attributes_use_independent_tag_overrides(self):
        datapoint = {
            "tag": "attribute_${address}",
            "tagOverrides": {"10": "Mode", "11": "Alarm state"},
            "type": "16uint",
            "functionCode": 4,
            "objectsCount": 1,
            "address": "10-11",
        }

        _, result = self._convert(datapoint, [DummyResponse([3, 1])], section="attributes")

        self.assertEqual({"Mode": 3, "Alarm state": 1}, result["attributes"])

    def test_partial_overrides_fall_back_to_tag_expression(self):
        datapoint = {
            "tag": "register_${address}",
            "tagOverrides": {"20": "Temperature"},
            "type": "16uint",
            "functionCode": 3,
            "objectsCount": 1,
            "address": "20-22",
        }

        _, result = self._convert(datapoint, [DummyResponse([101, 102, 103])])

        self.assertEqual(
            {"Temperature": 101, "register_21": 102, "register_22": 103},
            self._telemetry_values(result),
        )

    def test_legacy_config_keeps_existing_tag_fallback(self):
        datapoint = {
            "tag": "measurement",
            "type": "16uint",
            "functionCode": 3,
            "objectsCount": 1,
            "address": "0-1",
        }

        _, result = self._convert(datapoint, [DummyResponse([11, 22])])

        self.assertEqual(
            {"measurement_0": 11, "measurement_1": 22},
            self._telemetry_values(result),
        )

    def test_overrides_preserve_chunk_order_across_split_responses(self):
        datapoint = {
            "tag": "register_${address}",
            "tagOverrides": {"0": "First", "1": "Second", "2": "Third", "3": "Fourth"},
            "type": "16uint",
            "functionCode": 3,
            "objectsCount": 1,
            "maxRegistersPerRequest": 2,
            "address": "0-3",
        }

        address_ranges = AsyncModbusConnector._AsyncModbusConnector__get_address_ranges(datapoint)
        _, result = self._convert(datapoint, [DummyResponse([10, 20]), DummyResponse([30, 40])])

        self.assertEqual([(0, 2), (2, 2)], address_ranges)
        self.assertEqual(
            {"First": 10, "Second": 20, "Third": 30, "Fourth": 40},
            self._telemetry_values(result),
        )

    def test_objects_count_uses_decoded_start_addresses(self):
        datapoint = {
            "tag": "register_${address}",
            "tagOverrides": {"100": "Energy A", "102": "Energy B"},
            "type": "32uint",
            "functionCode": 3,
            "objectsCount": 2,
            "address": "100-102",
        }

        _, result = self._convert(
            datapoint,
            [DummyResponse([0x1234, 0x5678, 0x9ABC, 0xDEF0])],
            byte_order="BIG",
            word_order="BIG",
        )

        self.assertEqual(
            {"Energy A": 0x12345678, "Energy B": 0x9ABCDEF0},
            self._telemetry_values(result),
        )

    def test_overrides_do_not_change_byte_and_word_order_conversion(self):
        order_cases = (
            (Endian.BIG, Endian.BIG, "BIG", "BIG"),
            (Endian.BIG, Endian.LITTLE, "BIG", "LITTLE"),
            (Endian.LITTLE, Endian.BIG, "LITTLE", "BIG"),
            (Endian.LITTLE, Endian.LITTLE, "LITTLE", "LITTLE"),
        )
        for builder_byte_order, builder_word_order, config_byte_order, config_word_order in order_cases:
            with self.subTest(byte_order=config_byte_order, word_order=config_word_order):
                builder = BinaryPayloadBuilder(byteorder=builder_byte_order, wordorder=builder_word_order)
                builder.add_32bit_uint(0x12345678)
                datapoint = {
                    "tag": "register_${address}",
                    "tagOverrides": {"70": "Energy"},
                    "type": "32uint",
                    "functionCode": 3,
                    "objectsCount": 2,
                    "address": "70-70",
                }

                _, result = self._convert(
                    datapoint,
                    [DummyResponse(builder.to_registers())],
                    byte_order=config_byte_order,
                    word_order=config_word_order,
                )

                self.assertEqual({"Energy": 0x12345678}, self._telemetry_values(result))

    def test_overrides_support_coil_blocks(self):
        legacy_datapoint = {
            "tag": "coil_${address}",
            "type": "bit",
            "functionCode": 1,
            "objectsCount": 1,
            "address": "0-1",
        }
        datapoint = {
            "tag": "coil_${address}",
            "tagOverrides": {"0": "Running", "1": "Alarm"},
            "type": "bit",
            "functionCode": 1,
            "objectsCount": 1,
            "address": "0-1",
        }

        _, legacy_result = self._convert(legacy_datapoint, [DummyResponse(bits=[True, False])])
        _, result = self._convert(datapoint, [DummyResponse(bits=[True, False])])

        legacy_values = self._telemetry_values(legacy_result)
        self.assertEqual(
            [legacy_values["coil_0"], legacy_values["coil_1"]],
            [self._telemetry_values(result)["Running"], self._telemetry_values(result)["Alarm"]],
        )

    def test_split_requests_preserve_objects_count_addressing(self):
        datapoint = {
            "tag": "register_${address}",
            "tagOverrides": {"100": "First value", "102": "Second value"},
            "type": "32uint",
            "functionCode": 3,
            "objectsCount": 2,
            "maxRegistersPerRequest": 2,
            "address": "100-102",
        }
        responses = [DummyResponse([0x1234, 0x5678]), DummyResponse([0x9ABC, 0xDEF0])]

        address_ranges = AsyncModbusConnector._AsyncModbusConnector__get_address_ranges(datapoint)
        _, result = self._convert(datapoint, responses)

        self.assertEqual([(100, 2), (102, 2)], address_ranges)
        self.assertEqual(
            {"First value": 0x12345678, "Second value": 0x9ABCDEF0},
            self._telemetry_values(result),
        )

    def test_duplicate_override_names_use_legacy_fallback(self):
        datapoint = {
            "tag": "register_${address}",
            "tagOverrides": {"30": "Duplicate", "31": "Duplicate"},
            "type": "16uint",
            "functionCode": 3,
            "objectsCount": 1,
            "address": "30-31",
        }

        with self.assertLogs(self.log, level="WARNING") as captured:
            converter, result = self._convert(datapoint, [DummyResponse([7, 8])])
            converter.convert(None, [{"telemetry": {datapoint["tag"]: [DummyResponse([7, 8])]},
                                      "attributes": {}}])

        self.assertEqual(
            {"register_30": 7, "register_31": 8},
            self._telemetry_values(result),
        )
        collision_warnings = [line for line in captured.output if "conflicts with another key" in line]
        self.assertEqual(2, len(collision_warnings))

    def test_override_colliding_with_fallback_is_ignored(self):
        datapoint = {
            "tag": "register_${address}",
            "tagOverrides": {"40": "register_41"},
            "type": "16uint",
            "functionCode": 3,
            "objectsCount": 1,
            "address": "40-41",
        }

        _, result = self._convert(datapoint, [DummyResponse([5, 6])])

        self.assertEqual(
            {"register_40": 5, "register_41": 6},
            self._telemetry_values(result),
        )

    def test_duplicate_overrides_across_datapoints_use_legacy_fallback(self):
        datapoints = [
            {
                "tag": "first_${address}",
                "tagOverrides": {"10": "Same"},
                "type": "16uint",
                "functionCode": 3,
                "objectsCount": 1,
                "address": "10-10",
            },
            {
                "tag": "second_${address}",
                "tagOverrides": {"20": "Same"},
                "type": "16uint",
                "functionCode": 3,
                "objectsCount": 1,
                "address": "20-20",
            },
        ]

        _, result = self._convert_multiple(
            datapoints,
            {"first_${address}": [DummyResponse([111])], "second_${address}": [DummyResponse([222])]},
        )

        self.assertEqual({"first_10": 111, "second_20": 222}, self._telemetry_values(result))

    def test_duplicate_attribute_overrides_across_datapoints_use_legacy_fallback(self):
        datapoints = [
            {
                "tag": "first_${address}",
                "tagOverrides": {"10": "Same"},
                "type": "16uint",
                "functionCode": 4,
                "objectsCount": 1,
                "address": "10-10",
            },
            {
                "tag": "second_${address}",
                "tagOverrides": {"20": "Same"},
                "type": "16uint",
                "functionCode": 4,
                "objectsCount": 1,
                "address": "20-20",
            },
        ]

        _, result = self._convert_multiple(
            datapoints,
            {"first_${address}": [DummyResponse([111])], "second_${address}": [DummyResponse([222])]},
            section="attributes",
        )

        self.assertEqual({"first_10": 111, "second_20": 222}, result["attributes"])

    def test_override_colliding_with_fallback_in_another_attribute_uses_fallback(self):
        datapoints = [
            {
                "tag": "first_${address}",
                "tagOverrides": {"10": "second_20"},
                "type": "16uint",
                "functionCode": 4,
                "objectsCount": 1,
                "address": "10-10",
            },
            {
                "tag": "second_${address}",
                "type": "16uint",
                "functionCode": 4,
                "objectsCount": 1,
                "address": "20-20",
            },
        ]

        _, result = self._convert_multiple(
            datapoints,
            {"first_${address}": [DummyResponse([111])], "second_${address}": [DummyResponse([222])]},
            section="attributes",
        )

        self.assertEqual({"first_10": 111, "second_20": 222}, result["attributes"])

    def test_override_colliding_with_fallback_in_another_telemetry_uses_fallback(self):
        datapoints = [
            {
                "tag": "first_${address}",
                "tagOverrides": {"10": "second_20"},
                "type": "16uint",
                "functionCode": 3,
                "objectsCount": 1,
                "address": "10-10",
            },
            {
                "tag": "second_${address}",
                "type": "16uint",
                "functionCode": 3,
                "objectsCount": 1,
                "address": "20-20",
            },
        ]

        _, result = self._convert_multiple(
            datapoints,
            {"first_${address}": [DummyResponse([111])], "second_${address}": [DummyResponse([222])]},
        )

        self.assertEqual({"first_10": 111, "second_20": 222}, self._telemetry_values(result))

    def test_legacy_collisions_across_datapoints_keep_existing_last_value_behavior(self):
        datapoints = [
            {
                "tag": "${unitId}_${address}",
                "type": "16uint",
                "functionCode": 3,
                "objectsCount": 1,
                "address": "10-10",
            },
            {
                "tag": "1_${address}",
                "type": "16uint",
                "functionCode": 3,
                "objectsCount": 1,
                "address": "10-10",
            },
        ]

        _, result = self._convert_multiple(
            datapoints,
            {"${unitId}_${address}": [DummyResponse([111])], "1_${address}": [DummyResponse([222])]},
        )

        self.assertEqual({"1_10": 222}, self._telemetry_values(result))

    def test_incomplete_wide_range_response_does_not_shift_addresses(self):
        datapoint = {
            "tag": "register_${address}",
            "tagOverrides": {str(address): f"Address {address}" for address in range(6)},
            "type": "16uint",
            "functionCode": 3,
            "objectsCount": 1,
            "maxRegistersPerRequest": 2,
            "address": "0-5",
        }
        valid_chunks = (DummyResponse([10, 11]), DummyResponse([20, 21]), DummyResponse([30, 31]))

        for invalid_index in range(3):
            with self.subTest(invalid_index=invalid_index):
                responses = list(valid_chunks)
                responses[invalid_index] = None
                _, result = self._convert(datapoint.copy(), responses)
                self.assertEqual([], result["telemetry"])

    def test_legacy_incomplete_wide_range_keeps_existing_compaction_behavior(self):
        datapoint = {
            "tag": "register_${address}",
            "type": "16uint",
            "functionCode": 3,
            "objectsCount": 1,
            "maxRegistersPerRequest": 2,
            "address": "0-3",
        }

        _, result = self._convert(datapoint, [None, DummyResponse([30, 40])])

        self.assertEqual({"register_0": 30, "register_1": 40}, self._telemetry_values(result))

    def test_invalid_overrides_are_removed_during_config_validation(self):
        datapoint = {
            "tag": "register_${address}",
            "tagOverrides": {
                "100": "Valid",
                "101": "Misaligned",
                "104": "Outside",
                "0102": "Non canonical",
                "bad": "Invalid address",
                "102": "   ",
            },
            "type": "32uint",
            "functionCode": 3,
            "objectsCount": 2,
            "address": "100-102",
        }

        with self.assertLogs(self.log, level="WARNING"):
            config = BytesUplinkConverterConfig(
                self.log,
                deviceName="Modbus Test",
                unitId=1,
                timeseries=[datapoint],
                attributes=[],
            )

        self.assertEqual({"100": "Valid"}, config.telemetry[0]["tagOverrides"])

    def test_non_object_overrides_use_tag_fallback(self):
        datapoint = {
            "tag": "register_${address}",
            "tagOverrides": ["First", "Second"],
            "type": "16uint",
            "functionCode": 3,
            "objectsCount": 1,
            "address": "50-51",
        }

        with self.assertLogs(self.log, level="WARNING"):
            _, result = self._convert(datapoint, [DummyResponse([1, 2])])

        self.assertEqual(
            {"register_50": 1, "register_51": 2},
            self._telemetry_values(result),
        )

    def test_invalid_range_metadata_disables_overrides(self):
        invalid_cases = (("bad-range", 1), ("10-11", 0))

        for address, objects_count in invalid_cases:
            with self.subTest(address=address, objects_count=objects_count):
                datapoint = {
                    "tag": "register_${address}",
                    "tagOverrides": {"10": "Name"},
                    "type": "16uint",
                    "functionCode": 3,
                    "objectsCount": objects_count,
                    "address": address,
                }
                with self.assertLogs(self.log, level="WARNING"):
                    config = BytesUplinkConverterConfig(
                        self.log,
                        deviceName="Modbus Test",
                        unitId=1,
                        timeseries=[datapoint],
                        attributes=[],
                    )

                self.assertEqual({}, config.telemetry[0]["tagOverrides"])

    def test_tag_overrides_on_single_address_use_legacy_tag(self):
        datapoint = {
            "tag": "Single register",
            "tagOverrides": {"70": "Ignored"},
            "type": "16uint",
            "functionCode": 3,
            "objectsCount": 1,
            "address": 70,
        }

        with self.assertLogs(self.log, level="WARNING"):
            _, result = self._convert(datapoint, [DummyResponse([123])])

        self.assertEqual({"Single register": 123}, self._telemetry_values(result))

    def test_empty_overrides_use_tag_fallback_without_warning(self):
        datapoint = {
            "tag": "register_${address}",
            "tagOverrides": {},
            "type": "16uint",
            "functionCode": 3,
            "objectsCount": 1,
            "address": "60-61",
        }

        _, result = self._convert(datapoint, [DummyResponse([9, 10])])

        self.assertEqual(
            {"register_60": 9, "register_61": 10},
            self._telemetry_values(result),
        )

    @unittest.skip("Skip tests, because builder contains wrong numbers, should be fixed in future.")
    def test_modbus_getting_values(self):
        self.maxDiff = None
        test_modbus_config = {
            "attributes": [
                {"string": {
                    "byteOrder": "BIG",
                    "wordOrder": "BIG",
                    "tag": "string",
                    "type": "string",
                    "functionCode": 4,
                    "objectsCount": 4
                }},
                {"bits": {
                    "byteOrder": "BIG",
                    "wordOrder": "BIG",
                    "tag": "bits",
                    "type": "bits",
                    "functionCode": 4,
                    "objectsCount": 8
                }},
                {"bits2": {
                    "byteOrder": "BIG",
                    "wordOrder": "BIG",
                    "tag": "bits",
                    "type": "bits",
                    "functionCode": 4,
                    "objectsCount": 2
                }},
                {"8int": {
                    "byteOrder": "BIG",
                    "wordOrder": "BIG",
                    "tag": "8int",
                    "type": "8int",
                    "functionCode": 4,
                    "objectsCount": 1
                }},
                {"16int": {
                    "byteOrder": "BIG",
                    "wordOrder": "BIG",
                    "tag": "16int",
                    "type": "16int",
                    "functionCode": 4,
                    "objectsCount": 1
                }},
                {"long": {
                    "byteOrder": "BIG",
                    "wordOrder": "BIG",
                    "tag": "long",
                    "type": "long",
                    "functionCode": 4,
                    "objectsCount": 1
                }},
                {"long_with_divider": {
                    "byteOrder": "BIG",
                    "wordOrder": "BIG",
                    "tag": "long",
                    "type": "long",
                    "functionCode": 4,
                    "objectsCount": 1,
                    "divider": 10
                }},
                {"32int": {
                    "byteOrder": "BIG",
                    "wordOrder": "BIG",
                    "tag": "32int",
                    "type": "32int",
                    "functionCode": 4,
                    "objectsCount": 2
                }},
                {"64int": {
                    "byteOrder": "BIG",
                    "wordOrder": "BIG",
                    "tag": "64int",
                    "type": "64int",
                    "functionCode": 4,
                    "objectsCount": 4
                }},
            ],
            "timeseries": [
                {"8uint": {
                    "byteOrder": "BIG",
                    "wordOrder": "BIG",
                    "tag": "8uint",
                    "type": "8uint",
                    "functionCode": 4,
                    "objectsCount": 1}},
                {"16uint": {
                    "byteOrder": "BIG",
                    "wordOrder": "BIG",
                    "tag": "16uint",
                    "type": "16uint",
                    "functionCode": 4,
                    "objectsCount": 2}},
                {"32uint": {
                    "byteOrder": "BIG",
                    "wordOrder": "BIG",
                    "tag": "32uint",
                    "type": "32uint",
                    "functionCode": 4,
                    "objectsCount": 4}},
                {"64uint": {
                    "byteOrder": "BIG",
                    "wordOrder": "BIG",
                    "tag": "64uint",
                    "type": "64uint",
                    "functionCode": 4,
                    "objectsCount": 1}},
                {"double": {
                    "byteOrder": "BIG",
                    "wordOrder": "BIG",
                    "tag": "double",
                    "type": "double",
                    "functionCode": 4,
                    "objectsCount": 2}},
                {"16float": {
                    "byteOrder": "BIG",
                    "wordOrder": "BIG",
                    "tag": "16float",
                    "type": "16float",
                    "functionCode": 4,
                    "objectsCount": 1}},
                {"32float": {
                    "byteOrder": "BIG",
                    "wordOrder": "BIG",
                    "tag": "32float",
                    "type": "32float",
                    "functionCode": 4,
                    "objectsCount": 2}},
                {"64float": {
                    "byteOrder": "BIG",
                    "wordOrder": "BIG",
                    "tag": "64float",
                    "type": "64float",
                    "functionCode": 4,
                    "objectsCount": 4}},
            ]
        }
        test_modbus_body_to_convert = {}
        test_modbus_convert_config = {}
        test_modbus_result = {'deviceName': 'Modbus Test',
                              'deviceType': 'default',
                              'telemetry': [
                                  {'8uint': 18},
                                  {'16uint': 4660},
                                  {'32uint': 305419896},
                                  {'64uint': 1311768468603649775},
                                  {'double': 22.5},
                                  {'16float': 1.240234375},
                                  {'32float': 22.34000015258789},
                                  {'64float': -123.45}],
                              'attributes': [
                                  {'string': 'abcdefgh'},
                                  {'bits': [False, True, False, True, False, False, True, True]},
                                  {'bits2': [True, True]},
                                  {'8int': -18},
                                  {'16int': -22136},
                                  {'long': -22136},
                                  {'long_with_divider': -2213.6},
                                  {'32int': -4660},
                                  {'64int': -3735928559}]
                              }

        builder = BinaryPayloadBuilder(byteorder=Endian.BIG)
        builder_registers = {"string": (builder.add_string, 'abcdefgh'),
                             "bits": (builder.add_bits, [0, 1, 0, 1, 0, 0, 1, 1]),
                             "bits2": (builder.add_bits, [1, 1]),
                             "8int": (builder.add_8bit_int, -0x12),
                             "16int": (builder.add_16bit_int, -0x5678),
                             "long": (builder.add_16bit_int, -0x5678),
                             "long_with_divider": (builder.add_16bit_int, -0x5678),
                             "32int": (builder.add_32bit_int, -0x1234),
                             "64int": (builder.add_64bit_int, -0xDEADBEEF),
                             "8uint": (builder.add_8bit_uint, 0x12),
                             "16uint": (builder.add_16bit_uint, 0x1234),
                             "32uint": (builder.add_32bit_uint, 0x12345678),
                             "64uint": (builder.add_64bit_uint, 0x12345678DEADBEEF),
                             "double": (builder.add_32bit_float, 22.5),
                             "16float": (builder.add_16bit_float, 1.24),
                             "32float": (builder.add_32bit_float, 22.34),
                             "64float": (builder.add_64bit_float, -123.45),
                             }

        class DummyResponse:
            def __init__(self, registers):
                self.registers = registers[:]

        for datatype in test_modbus_config:
            test_modbus_body_to_convert[datatype] = {}
            for tag_dict in test_modbus_config[datatype]:
                for tag in tag_dict:
                    builder_registers[tag][0](builder_registers[tag][1])
                    test_modbus_body_to_convert[datatype].update(
                        {tag: {"input_data": DummyResponse(builder.to_registers()), "data_sent": tag_dict[tag]}})
                    builder.reset()

        converter = BytesModbusUplinkConverter({"deviceName": "Modbus Test", "deviceType": "default", "unitId": 1}, logger=self.log)
        result = converter.convert(test_modbus_convert_config, test_modbus_body_to_convert)
        self.assertDictEqual(result, test_modbus_result)

    def test_decode_bits_preserves_original_bit_order(self):
        """Regression test for https://github.com/thingsboard/thingsboard-gateway/issues/2146:
        decoding 'bit'/'bits' values must return the bits in their original order instead of
        reading them backwards."""
        converter = BytesModbusUplinkConverter({"deviceName": "Modbus Test", "deviceType": "default", "unitId": 1},
                                               logger=self.log)

        # Coils/discrete inputs (functionCode 1/2) are decoded via BinaryPayloadDecoder.fromCoils(),
        # which internally reverses bit order within each byte.
        coils = [True, False, False, False, False, False, False, False]
        self.assertEqual(
            converter.decode_data(coils, {'functionCode': 2, 'type': 'bit', 'objectsCount': 2},
                                  Endian.LITTLE, Endian.BIG),
            [True, False])
        self.assertEqual(
            converter.decode_data(coils, {'functionCode': 1, 'type': 'bit', 'objectsCount': 1},
                                  Endian.LITTLE, Endian.BIG),
            True)

        coils_full_byte = [True, True, False, True, False, False, True, False]
        self.assertEqual(
            converter.decode_data(coils_full_byte, {'functionCode': 1, 'type': 'bits', 'objectsCount': 8},
                                  Endian.LITTLE, Endian.BIG),
            coils_full_byte)

        coils_two_bytes = [True, True, False, False, False, False, False, False,
                           True, False, False, False, False, False, False, False]
        self.assertEqual(
            converter.decode_data(coils_two_bytes, {'functionCode': 1, 'type': 'bits', 'objectsCount': 16},
                                  Endian.LITTLE, Endian.BIG),
            coils_two_bytes)

        # Holding/input registers (functionCode 3/4) are decoded via BinaryPayloadDecoder.fromRegisters(),
        # which does not reverse bit order, so no un-reversal should be applied there.
        builder = BinaryPayloadBuilder(byteorder=Endian.BIG)
        builder.add_bits([0, 1, 0, 1, 0, 0, 1, 1])
        registers = builder.to_registers()
        self.assertEqual(
            converter.decode_data(registers, {'functionCode': 4, 'type': 'bits', 'objectsCount': 8},
                                  Endian.BIG, Endian.BIG),
            [False, True, False, True, False, False, True, True])

        builder.reset()
        builder.add_bits([1, 1])
        registers = builder.to_registers()
        self.assertEqual(
            converter.decode_data(registers, {'functionCode': 4, 'type': 'bits', 'objectsCount': 2},
                                  Endian.BIG, Endian.BIG),
            [True, True])


if __name__ == '__main__':
    unittest.main()
