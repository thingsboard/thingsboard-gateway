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

from time import monotonic
from threading import Thread, Event, Lock
from typing import Optional, Tuple, Dict

from thingsboard_gateway.gateway.constants import ReportStrategy, STRATEGIES_WITH_REPORT_PERIOD
from thingsboard_gateway.gateway.entities.datapoint_key import DatapointKey
from thingsboard_gateway.gateway.entities.report_strategy_config import ReportStrategyConfig


class ReportStrategyDataRecord:
    __slots__ = ["_value", "_device_name", "_device_type", "_connector_name",
                 "_connector_id", "_report_strategy", "_last_report_time", "_is_telemetry", "_ts"]

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

    def update_last_report_time(self, update_time):
        self._last_report_time = update_time

    def update_value(self, value):
        self._value = value

    def update_ts(self, ts):
        self._ts = ts

    def should_be_reported_by_period(self, current_time):
        if self._report_strategy.report_strategy in STRATEGIES_WITH_REPORT_PERIOD:
            return (self._last_report_time is None
                    or current_time - self._last_report_time + 50 >= self._report_strategy.report_period)
        else:
            return False

    def to_send_format(self):
        return (self._connector_name, self._connector_id, self._device_name, self._device_type), self._value

    @property
    def report_strategy(self):
        return self._report_strategy


class ReportStrategyDataCache:
    def __init__(self, config, logger):
        self._config = config
        self._data_cache: Dict[Tuple, Tuple[ReportStrategyDataRecord, float]] = {}
        self._lock = Lock()
        self._cleanup_interval = self._config.get("reportStrategyDataCacheCleanupInterval", 3600)
        self._stop_event = Event()
        self._cleanup_thread = Thread(target=self._cleanup_loop, daemon=True,
                                      name="Reporting strategy data cache cleanup thread")
        self._cleanup_thread.start()
        current_time = monotonic()
        self.__previous_cleanup_time = current_time
        self.__data_cache_current_ts = current_time
        self.__logger = logger

    def put(self, datapoint_key: DatapointKey, data: str, device_name,
            device_type, connector_name, connector_id, report_strategy,
            is_telemetry):
        key = (datapoint_key, device_name, connector_id)
        expire_ts = self.__data_cache_current_ts + report_strategy.ttl if report_strategy.ttl else 0
        record = ReportStrategyDataRecord(
            data, device_name, device_type, connector_name,
            connector_id, report_strategy, is_telemetry
        )
        with self._lock:
            self._data_cache[key] = (record, expire_ts)

    def get(self, datapoint_key: DatapointKey, device_name, connector_id) -> Optional[ReportStrategyDataRecord]:
        key = (datapoint_key, device_name, connector_id)
        with self._lock:
            item = self._data_cache.get(key)
            if not item:
                return None
            record, expire_ts = item
            if 0 < expire_ts < self.__data_cache_current_ts:
                del self._data_cache[key]
                return None
            return record

    def update_last_report_time(self, datapoint_key: DatapointKey, device_name, connector_id, update_time):
        record = self.get(datapoint_key, device_name, connector_id)
        if record:
            record.update_last_report_time(update_time)

    def update_key_value(self, datapoint_key: DatapointKey, device_name, connector_id, value):
        record = self.get(datapoint_key, device_name, connector_id)
        if record:
            record.update_value(value)
            with self._lock:
                expire_ts = self.__data_cache_current_ts + record.report_strategy.ttl if record.report_strategy.ttl else 0
                self._data_cache[(datapoint_key, device_name, connector_id)] = (record, expire_ts)

    def update_ts(self, datapoint_key: DatapointKey, device_name, connector_id, ts):
        record = self.get(datapoint_key, device_name, connector_id)
        if record:
            record.update_ts(ts)
            with self._lock:
                expire_ts = self.__data_cache_current_ts + record.report_strategy.ttl if record.report_strategy.ttl else 0
                self._data_cache[(datapoint_key, device_name, connector_id)] = (record, expire_ts)

    def delete_all_records_for_connector_by_connector_id(self, connector_id):
        with self._lock:
            keys_to_delete = [key for key in self._data_cache if key[2] == connector_id]
            for key in keys_to_delete:
                del self._data_cache[key]

    def clear(self):
        with self._lock:
            self._data_cache.clear()

    def stop(self):
        self._stop_event.set()
        self._cleanup_thread.join()

    def _cleanup_loop(self):
        while not self._stop_event.wait(1):
            self.__data_cache_current_ts = monotonic()
            if self.__previous_cleanup_time - self.__data_cache_current_ts >= self._cleanup_interval:
                self.__previous_cleanup_time = monotonic()
                with self._lock:

                    keys_to_delete = []
                    for key, (report_data_record, expire_ts) in self._data_cache.items():
                        if report_data_record.report_strategy.report_strategy != ReportStrategy.ON_RECEIVED and \
                                0 < expire_ts < self.__data_cache_current_ts:
                            keys_to_delete.append(key)
                    for key in keys_to_delete:
                        del self._data_cache[key]
                        self.__logger.debug("Removed expired record from cache: %s", key)
