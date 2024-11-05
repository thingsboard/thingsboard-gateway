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
