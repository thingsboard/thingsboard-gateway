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

from time import monotonic

from thingsboard_gateway.gateway.constants import ReportStrategy
from thingsboard_gateway.gateway.entities.report_strategy_config import ReportStrategyConfig


class ReportStrategyDataRecord:
    __slots__ = ["_value", "_device_name", "_device_type","_connector_name",
                 "_connector_id", "_report_strategy", "_last_report_time", "_is_telemetry","_ts"]
    def __init__(self, value, device_name, device_type, connector_name, connector_id, report_strategy, is_telemetry):
        self._value = value
        self._device_name = device_name
        self._device_type = device_type
        self._connector_name = connector_name
        self._connector_id = connector_id
        self._report_strategy: ReportStrategyConfig = report_strategy
        self._last_report_time = None
        self._is_telemetry = is_telemetry
        self._ts = None
        # TODO: Add aggregation functionality
        # self.__aggregated_data = {
        #     'sum': 0,
        #     'count': 0,
        #     'min': None,
        #     'max': None,
        #     'median': None,
        # }

    def get_value(self):
        return self._value

    def get_ts(self):
        return self._ts

    def is_telemetry(self):
        return self._is_telemetry

    def update_last_report_time(self):
        self._last_report_time = int(monotonic() * 1000)

    def update_value(self, value):
        self._value = value

    def update_ts(self, ts):
        self._ts = ts

    def should_be_reported_by_period(self, current_time):
        if self._report_strategy.report_strategy == ReportStrategy.ON_REPORT_PERIOD:
            return (self._last_report_time is None
                    or current_time - self._last_report_time >= self._report_strategy.report_period)
        elif self._report_strategy.report_strategy == ReportStrategy.ON_CHANGE_OR_REPORT_PERIOD:
            return (self._last_report_time is not None
                    and current_time - self._last_report_time >= self._report_strategy.report_period)
        else:
            return False

    def to_send_format(self):
        return (self._connector_name, self._connector_id, self._device_name, self._device_type), self._value


class ReportStrategyDataCache:
    def __init__(self, config):
        self._config = config
        self._data_cache = {}

    def put(self, key, data: str, device_name, device_type, connector_name, connector_id, report_strategy, is_telemetry):
        self._data_cache[key] = ReportStrategyDataRecord(data, device_name, device_type, connector_name, connector_id, report_strategy, is_telemetry)

    def get(self, key) -> ReportStrategyDataRecord:
        return self._data_cache.get(key)

    def update_last_report_time(self, key):
        self._data_cache[key].update_last_report_time()

    def update_key_value(self, key, value):
        self._data_cache[key].update_value(value)

    def update_ts(self, key, ts):
        self._data_cache[key].update_ts(ts)
