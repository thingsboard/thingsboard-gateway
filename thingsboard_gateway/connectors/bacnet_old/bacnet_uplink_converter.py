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
from time import time

from bacpypes.apdu import APDU, ReadPropertyACK
from bacpypes.constructeddata import ArrayOf
from bacpypes.primitivedata import Tag

from thingsboard_gateway.connectors.bacnet_old.bacnet_converter import BACnetConverter
from thingsboard_gateway.gateway.constants import REPORT_STRATEGY_PARAMETER
from thingsboard_gateway.gateway.entities.converted_data import ConvertedData
from thingsboard_gateway.gateway.entities.report_strategy_config import ReportStrategyConfig
from thingsboard_gateway.gateway.entities.telemetry_entry import TelemetryEntry
from thingsboard_gateway.gateway.statistics.decorators import CollectStatistics
from thingsboard_gateway.gateway.statistics.statistics_service import StatisticsService
from thingsboard_gateway.tb_utility.tb_utility import TBUtility


class BACnetUplinkConverter(BACnetConverter):
    DATATYPES = {"attributes": "attributes",
                 "timeseries": "telemetry",
                 "telemetry": "telemetry"}
    def __init__(self, config, logger):
        self._log = logger
        self.__config = config
        self.__device_report_strategy = None
        try:
            self.__device_report_strategy = ReportStrategyConfig(self.__config.get(REPORT_STRATEGY_PARAMETER))
        except ValueError as e:
            self._log.trace("Report strategy is not set for device %s. Using strategy from upper level.", self.__config.get("deviceName"))

    @CollectStatistics(start_stat_type='receivedBytesFromDevices',
                       end_stat_type='convertedBytesFromDevice')
    def convert(self, config, data):
        converted_data = ConvertedData(
            device_name=self.__config.get("deviceName", config[1].get("name", "BACnet device") if config is not None else "BACnet device"),
            device_type=self.__config.get("deviceType", "default")
        )

        try:
            value = None
            if isinstance(data, ReadPropertyACK):
                value = self.__property_value_from_apdu(data)
            if config is not None:
                datapoint_key = (
                    TBUtility.convert_key_to_datapoint_key(config[1]["key"],
                                                           config[1].get(REPORT_STRATEGY_PARAMETER),
                                                           self.__device_report_strategy,
                                                           self._log))
                if BACnetUplinkConverter.DATATYPES[config[0]] == "attributes":
                    converted_data.add_to_attributes(datapoint_key, value)
                else:
                    converted_data.add_to_telemetry(TelemetryEntry({datapoint_key: value}, time() * 1000))
            else:
                converted_data = value
            self._log.debug("Converted data: %s", converted_data)
        except Exception as e:
            StatisticsService.count_connector_message(self._log.name, 'convertersMsgDropped')
            self._log.exception(e)

        if isinstance(converted_data, ConvertedData):
            StatisticsService.count_connector_message(self._log.name, 'convertersAttrProduced',
                                                      count=converted_data.attributes_datapoints_count)
            StatisticsService.count_connector_message(self._log.name, 'convertersTsProduced',
                                                      count=converted_data.telemetry_datapoints_count)
        return converted_data

    @staticmethod
    def __property_value_from_apdu(apdu: APDU):
        tag_list = apdu.propertyValue.tagList
        non_app_tags = [tag for tag in tag_list if tag.tagClass != Tag.applicationTagClass]
        if non_app_tags:
            raise RuntimeError("Value has some non-application tags")
        first_tag = tag_list[0]
        other_type_tags = [tag for tag in tag_list[1:] if tag.tagNumber != first_tag.tagNumber]
        if other_type_tags:
            raise RuntimeError("All tags must be the same type")
        datatype = Tag._app_tag_class[first_tag.tagNumber]
        if not datatype:
            raise RuntimeError("unknown datatype")
        if len(tag_list) > 1:
            datatype = ArrayOf(datatype)
        value = apdu.propertyValue.cast_out(datatype)
        return value
