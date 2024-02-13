#      Copyright 2024. ThingsBoard
#  #
#      Licensed under the Apache License, Version 2.0 (the "License");
#      you may not use this file except in compliance with the License.
#      You may obtain a copy of the License at
#  #
#          http://www.apache.org/licenses/LICENSE-2.0
#  #
#      Unless required by applicable law or agreed to in writing, software
#      distributed under the License is distributed on an "AS IS" BASIS,
#      WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#      See the License for the specific language governing permissions and
#      limitations under the License.

from time import time
from logging import getLogger
from thingsboard_gateway.gateway.constants import SEND_ON_CHANGE_PARAMETER, DEVICE_NAME_PARAMETER, \
    ATTRIBUTES_PARAMETER, TELEMETRY_PARAMETER, TELEMETRY_TIMESTAMP_PARAMETER, TELEMETRY_VALUES_PARAMETER, \
    DEVICE_TYPE_PARAMETER, SEND_ON_CHANGE_TTL_PARAMETER, DEFAULT_SEND_ON_CHANGE_INFINITE_TTL_VALUE

log = getLogger("service")


class DuplicateDetector:
    ABSENT_DATA_PAIR_VALUES = [None, 0]

    def __init__(self, connectors):
        self._latest_data = {}
        self._connectors = connectors

    def rename_device(self, old_device_name, new_device_name):
        self._latest_data[new_device_name] = self._latest_data.pop(old_device_name,
                                                                   DuplicateDetector._create_device_latest_data())

    def delete_device(self, device_name):
        self._latest_data.pop(device_name, None)

    def persist_latest_values(self):
        raise NotImplementedError("Persisting feature for latest attributes/telemetry values is not implemented!")

    def filter_data(self, connector_name, new_data):
        if new_data:
            in_data_filter_enabled = new_data.get(SEND_ON_CHANGE_PARAMETER)
            if not in_data_filter_enabled or not isinstance(in_data_filter_enabled, bool):
                return new_data

            ttl = new_data.get(SEND_ON_CHANGE_TTL_PARAMETER)
            device_name = new_data[DEVICE_NAME_PARAMETER]
            if not in_data_filter_enabled:
                connector = self._connectors.get(connector_name)
                if not connector or not connector.is_filtering_enable(device_name):
                    return new_data
                elif ttl is None:
                    ttl = connector.get_ttl_for_duplicates(device_name)

            now = int(time() * 1000)
            to_send = {ATTRIBUTES_PARAMETER: [], TELEMETRY_PARAMETER: []}

            remaining_attributes_count = 0
            filtered_attributes_count = 0
            for attribute in new_data[ATTRIBUTES_PARAMETER]:
                for key, new_value in attribute.items():
                    if self._update_latest_attribute_value(device_name, key, new_value, now, ttl):
                        to_send[ATTRIBUTES_PARAMETER].append(attribute)
                        remaining_attributes_count += 1
                    else:
                        filtered_attributes_count += 1

            remaining_telemetry_count = 0
            filtered_telemetry_count = 0
            for ts_kv_list in new_data[TELEMETRY_PARAMETER]:
                ts_added = False
                ts = ts_kv_list.get(TELEMETRY_TIMESTAMP_PARAMETER)
                ts_values = {}
                for key, new_value in ts_kv_list.get(TELEMETRY_VALUES_PARAMETER, ts_kv_list).items():
                    if self._update_latest_telemetry_value(device_name, key, new_value, ts if ts else now, ttl):
                        ts_values[key] = new_value
                        ts_added = True
                        remaining_telemetry_count += 1
                    else:
                        filtered_telemetry_count += 1
                if ts_added:
                    to_send[TELEMETRY_PARAMETER].append(
                        {TELEMETRY_TIMESTAMP_PARAMETER: ts, TELEMETRY_VALUES_PARAMETER: ts_values} if ts else ts_values)

            if remaining_attributes_count > 0 or remaining_telemetry_count > 0:
                log.debug("[%s] '%s' changed attributes %d from %d, changed telemetry %d from %d",
                          connector_name, device_name,
                          remaining_attributes_count, remaining_attributes_count + filtered_attributes_count,
                          remaining_telemetry_count, remaining_telemetry_count + filtered_telemetry_count)
                to_send[DEVICE_NAME_PARAMETER] = device_name
                to_send[DEVICE_TYPE_PARAMETER] = new_data[DEVICE_TYPE_PARAMETER]
                return to_send

            log.info("[%s] '%s' device data has not been changed", connector_name, device_name)
            return None

    @staticmethod
    def _create_device_latest_data():
        return {
            ATTRIBUTES_PARAMETER: {},
            TELEMETRY_PARAMETER: {}
        }

    def _update_latest_attribute_value(self, device_name, key, value, ts, ttl):
        return self._update_latest_value(device_name, ATTRIBUTES_PARAMETER, key, value, ts, ttl)

    def _update_latest_telemetry_value(self, device_name, key, value, ts, ttl):
        return self._update_latest_value(device_name, TELEMETRY_PARAMETER, key, value, ts, ttl)

    def _update_latest_value(self, device_name, data_type, key, value, ts, ttl):
        if device_name not in self._latest_data:
            self._latest_data[device_name] = DuplicateDetector._create_device_latest_data()
            self._latest_data[device_name][data_type][key] = [value, ts]
            return True

        data_pair = self._latest_data[device_name][data_type].get(key, self.ABSENT_DATA_PAIR_VALUES)
        if data_pair[0] != value or (ttl and (ts - data_pair[1]) > ttl):
            self._latest_data[device_name][data_type][key] = [value, ts]
            return True
        return False
