from tests.unit.BaseUnitTest import BaseUnitTest
from thingsboard_gateway.connectors.request.json_request_uplink_converter import JsonRequestUplinkConverter


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
        expected_result = {
            "deviceName": "aranet:358151000412:100886",
            "deviceType": "default",
            "attributes": [],
            "telemetry": [{"Humidity": '66'}]
        }

        converter = JsonRequestUplinkConverter(test_request_config, self.log)
        result = converter.convert(test_request_convert_config, test_request_body_to_convert)
        self.assertDictEqual(expected_result, result)
