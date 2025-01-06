#     Copyright 2024. ThingsBoard
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


from typing import Any, Tuple
from simplejson import loads

from thingsboard_gateway.connectors.converter import Converter
from thingsboard_gateway.gateway.constants import REPORT_STRATEGY_PARAMETER, TELEMETRY_PARAMETER, TIMESERIES_PARAMETER
from thingsboard_gateway.gateway.entities.converted_data import ConvertedData
from thingsboard_gateway.gateway.entities.datapoint_key import DatapointKey
from thingsboard_gateway.gateway.entities.report_strategy_config import ReportStrategyConfig
from thingsboard_gateway.gateway.entities.telemetry_entry import TelemetryEntry
from thingsboard_gateway.tb_utility.tb_utility import TBUtility


class SerialUplinkConverter(Converter):
    """
    Uplink converter is used to convert incoming data to the format that platform expects.
    Such as we create uplink converter for each configured device,
    this converter is used to convert incoming data from only one device.
    Because config, that we passed to init method, is device specific.
    If your connector can handle multiple devices, you can create one converter for all devices.
    """

    def __init__(self, config, logger):
        self._log = logger
        self.__config = config
        self.__device_report_strategy = None
        self.__device_name = self.__config.get('deviceName', self.__config.get('name', 'SerialDevice'))
        self.__device_type = self.__config.get('deviceType', self.__config.get('type', 'default'))
        try:
            self.__device_report_strategy = ReportStrategyConfig(self.__config.get(REPORT_STRATEGY_PARAMETER))
        except ValueError as e:
            self._log.trace("Report strategy config is not specified for device %s: %s", self.__device_name, e)

    def convert(self, config, data: bytes):
        """Converts incoming data to the format that platform expects. Config is specified only for RPC responses."""
        self._log.debug("Data to convert: %s", data)
        if config is not None:
            converted_data = {"result": self.__convert_value_to_type(data, config)}
            return converted_data
        else:
            converted_data = ConvertedData(self.__device_name, self.__device_type)
            for datapoint_config in self.__config.get(TIMESERIES_PARAMETER, self.__config.get(TELEMETRY_PARAMETER, [])):
                try:
                    telemetry_entry = self.__convert_telemetry_datapoint(data, datapoint_config)
                    if telemetry_entry:
                        converted_data.add_to_telemetry(telemetry_entry)
                except Exception as e:
                    self._log.error("Error converting telemetry datapoint: %s", e)
            for datapoint_config in self.__config.get('attributes', []):
                try:
                    attribute_data = self.__convert_attributes_datapoint(data, datapoint_config)
                    if attribute_data:
                        converted_data.add_to_attributes(*attribute_data)
                except Exception as e:
                    self._log.error("Error converting attribute datapoint: %s", e)
            self._log.debug("Converted data: %s", converted_data)
        return converted_data

    def __convert_telemetry_datapoint(self, data, dp_config) -> TelemetryEntry:
        key = dp_config.get('key')
        datapoint_key = self.__convert_datapoint_key(key, dp_config, self.__device_report_strategy, self._log)
        value = self.__convert_value_to_type(data, dp_config)
        if not datapoint_key or not value:
            self._log.trace("Datapoint %s - not found in incoming data: %s", key, data.hex())
            return None
        return TelemetryEntry({datapoint_key: value})

    def __convert_attributes_datapoint(self, data, dp_config) -> Tuple[DatapointKey, Any]:
        key = dp_config.get('key')
        datapoint_key = self.__convert_datapoint_key(key, dp_config, self.__device_report_strategy, self._log)
        value = self.__convert_value_to_type(data, dp_config)
        if not datapoint_key or not value:
            self._log.trace("Datapoint %s - not found in incoming data: %s", key, data.hex())
            return None
        return (datapoint_key, value)

    @staticmethod
    def __convert_value_to_type(data, dp_config):
        type_ = dp_config.get('type')
        data_for_conversion = data
        if dp_config.get("untilDelimiter") or dp_config.get("fromDelimiter"):
            fromDelimiter = dp_config.get("fromDelimiter")
            untilDelimiter = dp_config.get("untilDelimiter")
            fromDelimiterPosition = data_for_conversion.find(
                fromDelimiter.encode('UTF-8')) if fromDelimiter else 0
            untilDelimiterPosition = data_for_conversion.find(
                untilDelimiter.encode('UTF-8')) if untilDelimiter else -1
            if fromDelimiterPosition != -1 \
                    and untilDelimiterPosition != -1 \
                    and fromDelimiterPosition < untilDelimiterPosition:
                data_for_conversion = data_for_conversion[fromDelimiterPosition:untilDelimiterPosition]
            elif fromDelimiterPosition != -1 and fromDelimiterPosition < len(data_for_conversion):
                data_for_conversion = data_for_conversion[fromDelimiterPosition:]
            elif untilDelimiterPosition != -1 and untilDelimiterPosition < len(data_for_conversion):
                data_for_conversion = data_for_conversion[:untilDelimiterPosition]
        elif dp_config.get("fromByte") or dp_config.get("toByte"):
            if dp_config.get("fromByte") and dp_config.get("toByte") \
                    and dp_config["fromByte"] < dp_config["toByte"] \
                    and len(data_for_conversion) > dp_config["toByte"]:
                data_for_conversion = data_for_conversion[dp_config["toByte"]:dp_config["fromByte"]]
            else:
                if dp_config.get("fromByte") and len(data_for_conversion) > dp_config.get("fromByte", 0):
                    data_for_conversion = data_for_conversion[dp_config["fromByte"]:]
                if dp_config.get("toByte") and \
                        (len(data_for_conversion) > dp_config.get("toByte", 0) or dp_config["toByte"] == -1):
                    data_for_conversion = data_for_conversion[:dp_config["toByte"]]

        if type_ == 'string':
            value = data_for_conversion.decode('UTF-8').strip()
        elif type_ == 'json':
            value = loads(data_for_conversion.decode('UTF-8'))
        elif type_ == 'int':
            value = int(data_for_conversion)
        elif type_ == 'float' or type_ == 'double':
            value = float(data_for_conversion)
        elif type_ == 'bool':
            try:
                value = bool(int(data_for_conversion))
            except ValueError:
                return data_for_conversion.decode('UTF-8').strip().lower() == 'true'
        else:
            value = data_for_conversion.hex()
        return value

    @staticmethod
    def __convert_datapoint_key(key, dp_config, device_report_strategy, logger):
        return TBUtility.convert_key_to_datapoint_key(key, device_report_strategy, dp_config, logger)
