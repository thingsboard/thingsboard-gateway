#     Copyright 2022. ThingsBoard
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

import struct

from simplejson import dumps

from thingsboard_gateway.connectors.request.request_converter import RequestConverter, log
from thingsboard_gateway.tb_utility.tb_utility import TBUtility


class CustomRequestUplinkConverter(RequestConverter):
    def __init__(self, config):
        self.__config = config.get('converter')

    def convert(self, _, body):
        try:
            data = body["data"]["value"]
            dict_result = {}
            dict_result["deviceName"] = TBUtility.get_value(self.__config.get("deviceNameJsonExpression"), body, expression_instead_none=True)
            dict_result["deviceType"] = TBUtility.get_value(self.__config.get("deviceTypeJsonExpression"), body, expression_instead_none=True)
            dict_result["attributes"] = []
            dict_result["telemetry"] = []
            converted_bytes = bytearray.fromhex(data)
            if self.__config.get("extension-config") is not None:
                for telemetry_key in self.__config["extension-config"]:
                    value = None
                    byteorder = telemetry_key.get("byteorder", "big").lower()
                    signed = telemetry_key.get("signed", True)
                    if telemetry_key.get("byteAddress") is None:
                        interest_bytes = converted_bytes[telemetry_key["fromByte"]: telemetry_key["toByte"]]
                        if telemetry_key["type"] == "float":
                            value = struct.unpack(">f" if byteorder == "big" else "<f", interest_bytes)
                            value = value[0] if isinstance(value, list) else None
                        if telemetry_key["type"] == "int":
                            value = int.from_bytes(interest_bytes, byteorder=byteorder, signed=signed)
                    else:
                        interest_byte = converted_bytes[telemetry_key["byteAddress"]]
                        bits = "{0:{fill}8b}".format(interest_byte, fill='0')
                        bits = bits if byteorder == "big" else bits[::-1]
                        value = int(bits[::-1][telemetry_key.get("fromBit"):telemetry_key.get("toBit")][::-1], 2)
                    if value is not None:
                        value = value * telemetry_key.get("multiplier", 1)
                        telemetry_to_send = {
                            telemetry_key["key"]: value}  # creating telemetry data for sending into Thingsboard
                        # current_byte_position += self.__config["extension-config"][telemetry_key]
                        dict_result["telemetry"].append(telemetry_to_send)  # adding data to telemetry array
            else:
                dict_result["telemetry"] = {
                    "data": int(body, 0)}  # if no specific configuration in config file - just send data which received
            return dict_result

        except Exception as e:
            log.error('Error in converter, for config: \n%s\n and message: \n%s\n', dumps(self.__config), body)
            log.exception(e)
