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

from queue import SimpleQueue
from threading import Thread
from time import monotonic, sleep, time
from typing import Dict, Set, Union

from thingsboard_gateway.gateway.constants import DEFAULT_REPORT_STRATEGY_CONFIG, ReportStrategy, DEVICE_NAME_PARAMETER, \
    DEVICE_TYPE_PARAMETER, REPORT_STRATEGY_PARAMETER
from thingsboard_gateway.gateway.entities.converted_data import ConvertedData
from thingsboard_gateway.gateway.entities.datapoint_key import DatapointKey
from thingsboard_gateway.gateway.entities.report_strategy_config import ReportStrategyConfig
from thingsboard_gateway.gateway.entities.telemetry_entry import TelemetryEntry
from thingsboard_gateway.gateway.report_strategy.report_strategy_data_cache import ReportStrategyDataCache
from thingsboard_gateway.tb_utility.tb_logger import TbLogger


class ReportStrategyService:
    def __init__(self, config: dict, gateway: 'TBGatewayService', send_data_queue: SimpleQueue, logger: TbLogger):
        self.__gateway = gateway
        self.__send_data_queue = send_data_queue
        self._logger = logger
        report_strategy = config.get(REPORT_STRATEGY_PARAMETER, {})
        self.main_report_strategy = ReportStrategyConfig(report_strategy, DEFAULT_REPORT_STRATEGY_CONFIG)
        self._report_strategy_data_cache = ReportStrategyDataCache(config)
        self._connectors_report_strategies: Dict[str, ReportStrategyConfig] = {}
        self.__keys_to_report_periodically: Set[DatapointKey, str, str] = set()
        self.__periodical_reporting_thread = Thread(target=self.__periodical_reporting, daemon=True, name="Periodical Reporting Thread")
        self.__periodical_reporting_thread.start()

    def get_main_report_strategy(self):
        return self.main_report_strategy

    def register_connector_report_strategy(self, connector_name: str, connector_id: str, report_strategy_config: ReportStrategyConfig):
        if report_strategy_config is None:
            report_strategy_config = self.main_report_strategy.report_strategy
        self._connectors_report_strategies[connector_id] = report_strategy_config
        self._connectors_report_strategies[connector_name] = report_strategy_config

    def filter_data_and_send(self, data: Union[ConvertedData, dict], connector_name: str, connector_id: str):
        data_to_send = data
        if isinstance(data, dict):
            data_to_send = ConvertedData(device_name=data.get(DEVICE_NAME_PARAMETER),
                                 device_type=data.get(DEVICE_TYPE_PARAMETER, "default"),
                                 metadata=data.get("metadata"))
            for ts_kv in data.get("telemetry", []):
                data_to_send.add_to_telemetry(ts_kv)
            data_to_send.add_to_attributes(data.get("attributes", {}))
        converted_data_to_send = ConvertedData(device_name=data_to_send.device_name,
                                               device_type=data_to_send.device_type,
                                               metadata=data_to_send.metadata)

        if self._connectors_report_strategies.get(connector_id) is not None:
            report_strategy_config = self._connectors_report_strategies.get(connector_id)
        elif self._connectors_report_strategies.get(connector_name) is not None:
            report_strategy_config = self._connectors_report_strategies.get(connector_name)
        else:
            report_strategy_config = self.main_report_strategy

        if data_to_send.telemetry:
            telemetry_to_send = []
            original_telemetry = data_to_send.telemetry
            for ts_kv in original_telemetry:
                kv_to_send = {}
                for datapoint_key, value in ts_kv.values.items():
                    report_strategy = report_strategy_config

                    if isinstance(datapoint_key, str):
                        datapoint_key = DatapointKey(datapoint_key) # TODO: remove this DatapointKey creation after refactoring, added to avoid errors with old string keys

                    if datapoint_key.report_strategy is not None:
                        report_strategy = datapoint_key.report_strategy
                    if self.filter_datapoint_and_cache(datapoint_key,
                                                       (value, ts_kv.ts),
                                                       data_to_send.device_name,
                                                       data_to_send.device_type,
                                                       connector_name,
                                                       connector_id,
                                                       report_strategy,
                                                       True):
                        kv_to_send[datapoint_key] = value
                if kv_to_send:
                    telemetry_to_send.append(TelemetryEntry(kv_to_send, ts_kv.ts))
            if telemetry_to_send:
                converted_data_to_send.add_to_telemetry(telemetry_to_send)
        if data_to_send.attributes:
            attributes_to_send = {}
            for datapoint_key, value in data_to_send.attributes.items():
                report_strategy = report_strategy_config

                if isinstance(datapoint_key, str):
                    datapoint_key = DatapointKey(datapoint_key) # TODO: remove this DatapointKey creation after refactoring, added to avoid errors with old string keys

                if datapoint_key.report_strategy is not None:
                    report_strategy = datapoint_key.report_strategy
                if self.filter_datapoint_and_cache(datapoint_key,
                                                   value,
                                                   data_to_send.device_name,
                                                   data_to_send.device_type,
                                                   connector_name,
                                                   connector_id,
                                                   report_strategy,
                                                   False):
                    attributes_to_send[datapoint_key] = value
            if attributes_to_send:
                converted_data_to_send.add_to_attributes(attributes_to_send)
        if converted_data_to_send.telemetry or converted_data_to_send.attributes:
            self.__send_data_queue.put_nowait((connector_name, connector_id, converted_data_to_send))

    def filter_datapoint_and_cache(self, datapoint_key: DatapointKey, data, device_name, device_type, connector_name, connector_id, report_strategy_config: ReportStrategyConfig, is_telemetry: bool):
        if report_strategy_config is None:
            report_strategy_config = self.main_report_strategy.report_strategy
        if datapoint_key.report_strategy is not None:
            report_strategy_config = datapoint_key.report_strategy

        ts = None

        if isinstance(data, tuple):
            data, ts = data
        if ts is None:
            ts = int(time() * 1000)
        report_strategy_data_record = self._report_strategy_data_cache.get(datapoint_key, device_name, connector_id)

        if report_strategy_config.report_strategy == ReportStrategy.ON_RECEIVED:
            if report_strategy_data_record is not None:
                if is_telemetry:
                    self._report_strategy_data_cache.update_ts(datapoint_key, device_name, connector_id, ts)
            else:
                self._report_strategy_data_cache.put(datapoint_key, data, device_name, device_type, connector_name, connector_id, report_strategy_config, is_telemetry)
                if is_telemetry:
                    self._report_strategy_data_cache.update_ts(datapoint_key, device_name, connector_id, ts)
            return True

        if report_strategy_data_record is not None:
            if not report_strategy_data_record.get_value() == data:
                if report_strategy_config.report_strategy == ReportStrategy.ON_CHANGE:
                    self._report_strategy_data_cache.update_key_value(datapoint_key, device_name, connector_id, data)
                    if is_telemetry:
                        self._report_strategy_data_cache.update_ts(datapoint_key, device_name, connector_id, ts)
                    return True
                elif report_strategy_config.report_strategy == ReportStrategy.ON_REPORT_PERIOD:
                    self._report_strategy_data_cache.update_key_value(datapoint_key, device_name, connector_id, data)
                    if is_telemetry:
                        self._report_strategy_data_cache.update_ts(datapoint_key, device_name, connector_id, ts)
                    return False
                elif report_strategy_config.report_strategy == ReportStrategy.ON_CHANGE_OR_REPORT_PERIOD:
                    self._report_strategy_data_cache.update_key_value(datapoint_key, device_name, connector_id, data)
                    self._report_strategy_data_cache.update_last_report_time(datapoint_key, device_name, connector_id)
                    if is_telemetry:
                        self._report_strategy_data_cache.update_ts(datapoint_key, device_name, connector_id, ts)
                    return True
            else:
                return False
        else:
            self._report_strategy_data_cache.put(datapoint_key, data, device_name, device_type, connector_name, connector_id, report_strategy_config, is_telemetry)
            if report_strategy_config.report_strategy in (ReportStrategy.ON_REPORT_PERIOD, ReportStrategy.ON_CHANGE_OR_REPORT_PERIOD):
                if isinstance(datapoint_key, tuple):
                    datapoint_key, _ = datapoint_key
                self.__keys_to_report_periodically.add((datapoint_key, device_name, connector_id))
                self._report_strategy_data_cache.update_last_report_time(datapoint_key, device_name, connector_id)
                if is_telemetry:
                    self._report_strategy_data_cache.update_ts(datapoint_key, device_name, connector_id, ts)
            return True

    def __periodical_reporting(self):
        previous_error_printed_time = 0
        occurred_errors = 0
        report_strategy_data_cache_get = self._report_strategy_data_cache.get
        send_data_queue_put_nowait = self.__send_data_queue.put_nowait
        while not self.__gateway.stopped:
            try:
                if not self.__keys_to_report_periodically:
                    sleep(0.1)
                    continue

                current_time = int(monotonic() * 1000)
                data_to_report = {}
                keys_set = set(self.__keys_to_report_periodically)

                check_report_strategy_start = int(time() * 1000)
                reported_data_length = 0

                for key, device_name, connector_id in keys_set:
                    report_strategy_data_record = report_strategy_data_cache_get(key, device_name, connector_id)
                    if report_strategy_data_record is None:
                        raise ValueError(f"Data record for key '{key}' is absent in the cache")

                    if not report_strategy_data_record.should_be_reported_by_period(current_time):
                        continue

                    data_report_key, value = report_strategy_data_record.to_send_format()
                    if data_report_key not in data_to_report:
                        connector_name, _, _, device_type = data_report_key
                        metadata = {"connector": connector_name, "receivedTs": int(time() * 1000)}
                        data_to_report[data_report_key] = ConvertedData(device_name, device_type, metadata)

                    data_entry = data_to_report[data_report_key]
                    if report_strategy_data_record.is_telemetry():
                        data_entry.add_to_telemetry(TelemetryEntry({key: value},
                                                                   report_strategy_data_record.get_ts()))
                        reported_data_length += 1
                    else:
                        data_entry.add_to_attributes(key, value)
                        reported_data_length += 1

                    report_strategy_data_record.update_last_report_time()

                if data_to_report:
                    for data_report_key, data in data_to_report.items():
                        connector_name, connector_id, _, _ = data_report_key
                        send_data_queue_put_nowait((connector_name, connector_id, data))
                    data_to_report.clear()

                check_report_strategy_end = int(time() * 1000)
                if check_report_strategy_end - check_report_strategy_start > 100:
                    self._logger.warning("The periodical reporting took too long: %d ms", check_report_strategy_end - check_report_strategy_start)
                    self._logger.warning("The number of keys to report periodically: %d", len(keys_set))
                    self._logger.warning("The number of reported data: %d", reported_data_length)

                sleep(0.01)

            except Exception as e:
                occurred_errors += 1
                current_monotonic = int(monotonic())
                if current_monotonic - previous_error_printed_time > 5:
                    self._logger.exception("An error occurred in the Periodical Reporting Thread: %s", e)
                    if occurred_errors > 1:
                        self._logger.error(
                            "Too many errors occurred in the Periodical Reporting Thread for 5 seconds. Suppressing further error messages."
                        )
                    previous_error_printed_time = current_monotonic
                    occurred_errors = 0

    def delete_all_records_for_connector_by_connector_id_and_connector_name(self, connector_id, connector_name):
        self._report_strategy_data_cache.delete_all_records_for_connector_by_connector_id(connector_id)
        for key, device_name, connector_id in self.__keys_to_report_periodically.copy():
            if connector_id == connector_id:
                self.__keys_to_report_periodically.remove((key, device_name, connector_id))
        self._connectors_report_strategies.pop(connector_id, None)
        self._connectors_report_strategies.pop(connector_name, None)