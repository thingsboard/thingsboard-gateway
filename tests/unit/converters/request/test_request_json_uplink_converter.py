from tests.unit.BaseUnitTest import BaseUnitTest
from thingsboard_gateway.connectors.request.json_request_uplink_converter import JsonRequestUplinkConverter
from thingsboard_gateway.gateway.entities.converted_data import ConvertedData
from thingsboard_gateway.gateway.entities.datapoint_key import DatapointKey
from thingsboard_gateway.gateway.entities.telemetry_entry import TelemetryEntry


class RequestJsonUplinkConverterTest(BaseUnitTest):
    def test_convert(self):
        test_request_config = {
            "url": "/last",
            "httpMethod": "GET",
            "httpHeaders": {
                "ACCEPT": "application/json"
            },
            "allowRedirects": True,
            "timeout": 0.5,
            "scanPeriod": 5,
            "converter": {
                "deviceNameJsonExpression": "${$.sensor}",
                "deviceTypeJsonExpression": "default",
                "type": "json",
                "attributes": [
                ],
                "telemetry": [
                    {

                        "key": "${$.name}",
                        "type": "int",
                        "value": "${$.value}"
                    }
                ]
            }
        }
        test_request_body_to_convert = {"name": "Humidity",
                                        "sensor": "aranet:358151000412:100886",
                                        "time": "2020-03-17T16:16:03Z",
                                        "unit": "%RH",
                                        "value": "66"}

        test_request_convert_config = "127.0.0.1:5000/last"

        expected_result = ConvertedData(device_name="aranet:358151000412:100886", device_type="default")
        telemetry_entry = TelemetryEntry({DatapointKey('Humidity'): '66'})
        expected_result.add_to_telemetry(telemetry_entry)

        converter = JsonRequestUplinkConverter(test_request_config, self.log)
        result = converter.convert(test_request_convert_config, test_request_body_to_convert)
        self.assertEqual(expected_result.telemetry[0].values, result.telemetry[0].values)

    def test_convert_missing_placeholder_is_skipped(self):
        test_request_config = {
            "url": "/last",
            "httpMethod": "GET",
            "httpHeaders": {"ACCEPT": "application/json"},
            "allowRedirects": True,
            "timeout": 0.5,
            "scanPeriod": 5,
            "converter": {
                "deviceNameJsonExpression": "SD8500",
                "deviceTypeJsonExpression": "default",
                "type": "json",
                "attributes": [],
                "telemetry": [
                    {"key": "temperature", "type": "float", "value": "${temperature}"},
                    {"key": "humidity", "type": "float", "value": "${humidity}"},
                ],
            },
        }

        test_request_body_to_convert = {"temperature": 21.5}
        test_request_convert_config = "127.0.0.1:5000/last"

        expected_result = ConvertedData(device_name="SD8500", device_type="default")
        telemetry_entry = TelemetryEntry({DatapointKey("temperature"): 21.5})
        expected_result.add_to_telemetry(telemetry_entry)

        converter = JsonRequestUplinkConverter(test_request_config, self.log)
        result = converter.convert(test_request_convert_config, test_request_body_to_convert)

        self.assertEqual(len(result.telemetry), 1)
        self.assertEqual(expected_result.telemetry[0].values, result.telemetry[0].values)
        self.assertNotIn(DatapointKey("humidity"), result.telemetry[0].values)

    def test_convert_nested_jsonpath_missing_placeholder_is_skipped(self):
        test_request_config = {
            "url": "/last",
            "httpMethod": "GET",
            "httpHeaders": {"ACCEPT": "application/json"},
            "allowRedirects": True,
            "timeout": 0.5,
            "scanPeriod": 5,
            "converter": {
                "deviceNameJsonExpression": "${$.meta.sensor}",
                "deviceTypeJsonExpression": "default",
                "type": "json",
                "attributes": [],
                "telemetry": [
                    {"key": "temperature", "type": "float", "value": "${$.data.temperature}"},
                    {"key": "humidity", "type": "float", "value": "${$.data.humidity}"},
                ],
            },
        }

        test_request_body_to_convert = {
            "meta": {"sensor": "aranet:358151000412:100886"},
            "data": {"temperature": 21.5},
        }
        test_request_convert_config = "127.0.0.1:5000/last"

        expected_result = ConvertedData(device_name="aranet:358151000412:100886", device_type="default")
        telemetry_entry = TelemetryEntry({DatapointKey("temperature"): 21.5})
        expected_result.add_to_telemetry(telemetry_entry)

        converter = JsonRequestUplinkConverter(test_request_config, self.log)
        result = converter.convert(test_request_convert_config, test_request_body_to_convert)
        self.assertEqual(len(result.telemetry), 1)
        self.assertEqual(expected_result.telemetry[0].values, result.telemetry[0].values)

    def test_convert_list_root_jsonpath_missing_placeholder_is_skipped(self):
        test_request_config = {
            "url": "/last",
            "httpMethod": "GET",
            "httpHeaders": {"ACCEPT": "application/json"},
            "allowRedirects": True,
            "timeout": 0.5,
            "scanPeriod": 5,
            "converter": {
                "deviceNameJsonExpression": "${$[0].sensor}",
                "deviceTypeJsonExpression": "default",
                "type": "json",
                "attributes": [],
                "telemetry": [
                    {"key": "temperature", "type": "float", "value": "${$[0].temperature}"},
                    {"key": "humidity", "type": "float", "value": "${$[0].humidity}"},
                ],
            },
        }

        test_request_body_to_convert = [
            {"sensor": "SD8500", "temperature": 21.5}
        ]
        test_request_convert_config = "127.0.0.1:5000/last"

        expected_result = ConvertedData(device_name="SD8500", device_type="default")
        telemetry_entry = TelemetryEntry({DatapointKey("temperature"): 21.5})
        expected_result.add_to_telemetry(telemetry_entry)

        converter = JsonRequestUplinkConverter(test_request_config, self.log)
        result = converter.convert(test_request_convert_config, test_request_body_to_convert)

        self.assertEqual(len(result.telemetry), 1)
        self.assertEqual(expected_result.telemetry[0].values, result.telemetry[0].values)
