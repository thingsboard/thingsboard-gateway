#     Copyright 2026. ThingsBoard
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

import re
from time import time
from datetime import datetime, timedelta, date, timezone
import struct

import snap7

from thingsboard_gateway.connectors.s7.s7_converter import S7Converter
from thingsboard_gateway.gateway.statistics.statistics_service import StatisticsService
from thingsboard_gateway.gateway.entities.converted_data import ConvertedData
from thingsboard_gateway.gateway.entities.report_strategy_config import ReportStrategyConfig
from thingsboard_gateway.tb_utility.tb_utility import TBUtility


class S7UplinkConverter(S7Converter):
    def __init__(self, logger, config):
        self.__log = logger
        self.__config = config

    def convert(self, data):
        StatisticsService.count_connector_message(
            self.__log.name, 'convertersMsgProcessed')
        config = {
            'attributes': self.__config.attributes,
            'timeseries': self.__config.timeseries
        }

        converted_data = ConvertedData(
            device_name=self.__config.device_name, device_type=self.__config.device_profile_name)
        converted_data_append_methods = {
            'attributes': converted_data.add_to_attributes,
            'timeseries': converted_data.add_to_telemetry
        }

        device_report_strategy = self._get_device_report_strategy(self.__config.report_strategy_config,
                                                                  self.__config.device_name)

        received_data_ts = int(time() * 1000)

        for config, value in zip(self.__config.datapoints, data):
            try:
                datapoint_key = TBUtility.convert_key_to_datapoint_key(config['key'],
                                                                       device_report_strategy,
                                                                       config,
                                                                       self.__log)
                converted_value = self.convert_data(config, value)
                payload = {datapoint_key: converted_value}
                if config['type_'] == 'timeseries':
                    payload['ts'] = received_data_ts

                converted_data_append_methods[config['type_']](payload)
            except Exception as e:
                self.__log.exception(
                    "Failed to convert data for device '%s' datapoint '%s': %s", self.__config.device_name, config['key'], e)  # noqa: E501
                StatisticsService.count_connector_message(
                    self.__log.name, 'convertersError', count=1)
                continue

        StatisticsService.count_connector_message(self.__log.name,
                                                  'convertersAttrProduced',
                                                  count=converted_data.attributes_datapoints_count)
        StatisticsService.count_connector_message(self.__log.name,
                                                  'convertersTsProduced',
                                                  count=converted_data.telemetry_datapoints_count)

        self.__log.debug("Converted data: %s", converted_data)
        return converted_data

    def _get_device_report_strategy(self, report_strategy, device_name):
        try:
            return ReportStrategyConfig(report_strategy)
        except ValueError as e:
            self.__log.trace(
                "Report strategy config is not specified for device %s: %s", device_name, e)

    def convert_data(self, config, value):
        if config['type'] == 'vm' or config['type'] == 'tag':
            return value
        elif config['type'] == 'data':
            return self._convert_data_type(config, value)
        else:
            raise ValueError(
                f"Unsupported datapoint type '{config['type']}' for device '{self.__config.device_name}'")

    def _convert_data_type(self, config, value):
        if not value:
            return None

        data_type = str(config.get("dataType", "raw")).lower().strip()
        offset = config.get("offset", 0)
        bit_index = config.get("bit", config.get("bitIndex", 0))

        if data_type in ("bool", "boolean", "bit"):
            return snap7.util.get_bool(value, offset, bit_index)

        elif data_type in ("byte", "usint", "uint8"):
            return snap7.util.get_usint(value, offset)

        elif data_type in ("sint", "int8"):
            return int.from_bytes(value[offset:offset + 1], byteorder="big", signed=True)

        elif data_type in ("int", "int16", "short"):
            return snap7.util.get_int(value, offset)

        elif data_type in ("uint", "uint16", "word"):
            return snap7.util.get_uint(value, offset)

        elif data_type in ("dint", "int32"):
            return snap7.util.get_dint(value, offset)

        elif data_type in ("udint", "uint32", "dword"):
            return snap7.util.get_dword(value, offset)

        elif data_type in ("real", "float", "float32"):
            return float(snap7.util.get_real(value, offset))

        elif data_type in ("lreal", "double", "float64"):
            return float(struct.unpack_from(">d", value, offset)[0])

        elif data_type in ("string", "str", "s7string"):
            return snap7.util.get_string(value, offset)

        elif data_type in ("char", "character"):
            return chr(value[offset])

        elif data_type in ("wchar", "widecharacter"):
            val = struct.unpack_from(">H", value, offset)[0]
            return chr(val)

        elif data_type in ("lint", "int64"):
            return struct.unpack_from(">q", value, offset)[0]

        elif data_type in ("ulint", "uint64", "lword"):
            return struct.unpack_from(">Q", value, offset)[0]

        elif data_type in ("time", "dint_time"):
            # TIME is 32-bit signed int (milliseconds) -> returns seconds as float
            ms = snap7.util.get_dint(value, offset)
            return ms / 1000.0

        elif data_type == "ltime":
            # LTIME is 64-bit signed int (nanoseconds) -> returns seconds as float
            ns = struct.unpack_from(">q", value, offset)[0]
            return ns / 1_000_000_000.0

        elif data_type in ("tod", "time_of_day"):
            # TOD is 32-bit unsigned int (milliseconds since midnight) -> "HH:MM:SS.mmm"
            ms = snap7.util.get_dword(value, offset)
            return str(timedelta(milliseconds=ms))

        elif data_type == "ltod":
            # LTOD is 64-bit unsigned int (nanoseconds since midnight)
            ns = struct.unpack_from(">Q", value, offset)[0]
            seconds = ns / 1_000_000_000.0
            return str(timedelta(seconds=seconds))

        elif data_type == "date":
            # DATE is 16-bit unsigned int (days since 1990-01-01)
            days = snap7.util.get_uint(value, offset)
            base_date = date(1990, 1, 1)
            return (base_date + timedelta(days=days)).isoformat()

        elif data_type in ("dt", "date_and_time"):
            # S7 DATE_AND_TIME (DT) is 8-byte BCD (Year, Month, Day, Hour, Min, Sec, MS, Dow)
            def _bcd_to_int(bcd):
                return ((bcd >> 4) * 10) + (bcd & 0x0F)

            year = _bcd_to_int(value[offset])
            year += 2000 if year < 90 else 1900
            month = _bcd_to_int(value[offset + 1])
            day = _bcd_to_int(value[offset + 2])
            hour = _bcd_to_int(value[offset + 3])
            minute = _bcd_to_int(value[offset + 4])
            second = _bcd_to_int(value[offset + 5])
            ms_bcd1 = _bcd_to_int(value[offset + 6])
            ms_bcd2 = value[offset + 7] >> 4
            ms = (ms_bcd1 * 10) + ms_bcd2

            dt_val = datetime(year, month, day, hour, minute, second, ms * 1000)
            return dt_val.isoformat()

        elif data_type in ("ldt", "date_and_ltime"):
            # LDT is 64-bit signed int (nanoseconds since 1970-01-01 00:00:00 UTC)
            ns = struct.unpack_from(">q", value, offset)[0]
            dt_val = datetime.fromtimestamp(ns / 1_000_000_000.0, tz=timezone.utc)
            return dt_val.isoformat()

        elif data_type == "dtl":
            # DTL is 12 bytes structured date/time
            year = struct.unpack_from(">H", value, offset)[0]
            month = value[offset + 2]
            day = value[offset + 3]
            # offset + 4 is Day of Week
            hour = value[offset + 5]
            minute = value[offset + 6]
            second = value[offset + 7]
            ns = struct.unpack_from(">I", value, offset + 8)[0]

            dt_val = datetime(year, month, day, hour, minute, second, ns // 1000)
            return dt_val.isoformat()

        # --- Fixed Length String (FSTRING[n]) ---
        elif data_type.startswith("fstring") or data_type.startswith("char["):
            # Extract fixed size n from config 'size' or parsing 'fstring[10]' / 'fstring10'
            size = config.get("size")
            if not size:
                match = re.search(r"\[?(\d+)\]?", data_type)
                size = int(match.group(1)) if match else 1

            raw_chars = value[offset:offset + size]
            # Decode ASCII/Latin1 and trim trailing null bytes / space padding
            return raw_chars.decode("latin-1").rstrip("\x00 ").strip()

        # --- Raw Bytes / Bytearray ---
        elif data_type in ("raw", "bytes", "bytearray", "array"):
            return list(value[offset:])

        else:
            raise ValueError(f"Unsupported dataType: '{data_type}'")
