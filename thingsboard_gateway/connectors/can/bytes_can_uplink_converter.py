#     Copyright 2024. ThingsBoard
#
#     Licensed under the Apache License, Version 2.0 (the "License"];
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

from thingsboard_gateway.connectors.can.can_converter import CanConverter
from thingsboard_gateway.gateway.constants import REPORT_STRATEGY_PARAMETER
from thingsboard_gateway.gateway.entities.converted_data import ConvertedData
from thingsboard_gateway.gateway.entities.report_strategy_config import ReportStrategyConfig
from thingsboard_gateway.gateway.entities.telemetry_entry import TelemetryEntry
from thingsboard_gateway.gateway.statistics.decorators import CollectStatistics
from thingsboard_gateway.gateway.statistics.statistics_service import StatisticsService
from thingsboard_gateway.tb_utility.tb_utility import TBUtility


class BytesCanUplinkConverter(CanConverter):
    def __init__(self, logger):
        self._log = logger

    @CollectStatistics(start_stat_type='receivedBytesFromDevices',
                       end_stat_type='convertedBytesFromDevice')
    def convert(self, configs, can_data):
        device_name = configs.get("deviceName")
        device_type = configs.get("deviceType")

        converted_data = ConvertedData(device_name=device_name, device_type=device_type)

        device_report_strategy = None
        try:
            device_report_strategy = ReportStrategyConfig(configs.get(REPORT_STRATEGY_PARAMETER))
        except ValueError as e:
            self._log.trace("Report strategy config is not specified for device %s: %s", device_name, e)

        for config in configs.get('configs', []):
            try:
                tb_key = config["key"]
                tb_item = "telemetry" if config["is_ts"] else "attributes"

                data_length = config["length"] if config.get("length") is not None and config["length"] != -1 else len(can_data) - config["start"]

                # The 'value' variable is used in eval
                if config["type"][0] == "b":
                    value = bool(can_data[config["start"]])
                elif config["type"][0] == "i" or config["type"][0] == "l":
                    value = int.from_bytes(can_data[config["start"]:config["start"] + data_length],
                                           config["byteorder"],
                                           signed=config["signed"])
                elif config["type"][0] == "f" or config["type"][0] == "d":
                    fmt = ">" + config["type"][0] if config["byteorder"][0] == "b" else "<" + config["type"][0]
                    value = struct.unpack_from(fmt,
                                               bytes(can_data[config["start"]:config["start"] + data_length]))[0]
                elif config["type"][0] == "s":
                    value = can_data[config["start"]:config["start"] + data_length].decode(config["encoding"])
                elif config["type"][0] == "r":
                    value = ""
                    for hex_byte in can_data[config["start"]:config["start"] + data_length]:
                        value += "%02x" % hex_byte
                else:
                    self._log.error("Failed to convert CAN data to TB %s '%s': unknown data type '%s'",
                                    "time series key" if config["is_ts"] else "attribute", tb_key, config["type"])
                    continue

                if config.get("expression", ""):
                    value = eval(config["expression"],
                                                   {"__builtins__": {}} if config["strictEval"] else globals(),
                                                   {"value": value, "can_data": can_data})

                datapoint_key = TBUtility.convert_key_to_datapoint_key(tb_key, device_report_strategy,
                                                                       configs, self._log)
                if tb_item == "attributes":
                    converted_data.add_to_attributes(datapoint_key, value)
                else:
                    telemetry_entry = TelemetryEntry({datapoint_key: value})
                    converted_data.add_to_telemetry(telemetry_entry)
            except Exception as e:
                StatisticsService.count_connector_message(self._log.name, 'convertersMsgDropped')
                self._log.error("Failed to convert CAN data to TB %s '%s': %s",
                                "time series key" if config["is_ts"] else "attribute", tb_key, str(e))
                continue

        StatisticsService.count_connector_message(self._log.name, 'convertersAttrProduced',
                                                  count=converted_data.attributes_datapoints_count)
        StatisticsService.count_connector_message(self._log.name, 'convertersTsProduced',
                                                  count=converted_data.telemetry_datapoints_count)

        return converted_data
