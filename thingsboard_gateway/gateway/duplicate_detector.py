#      Copyright 2022. ThingsBoard
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

from logging import getLogger

from thingsboard_gateway.gateway.constants import SEND_ON_CHANGE_PARAMETER, DEVICE_NAME_PARAMETER, \
    ATTRIBUTES_PARAMETER, TELEMETRY_PARAMETER, TELEMETRY_TIMESTAMP_PARAMETER, TELEMETRY_VALUES_PARAMETER, \
    DEVICE_TYPE_PARAMETER

log = getLogger("service")


class DuplicateDetector:
    def __init__(self, connectors):
        self._connectors_data = {}
        self._connectors = connectors

    def persist_latest_values(self):
        raise NotImplementedError("Persisting feature for latest attributes/telemetry values is not implemented!")

    def filter_data(self, connector_name, new_data):
        in_data_filter_enabled = new_data.get(SEND_ON_CHANGE_PARAMETER)
        if isinstance(in_data_filter_enabled, bool) and not in_data_filter_enabled:
            return new_data

        device_name = new_data[DEVICE_NAME_PARAMETER]
        if not in_data_filter_enabled:
            connector = self._connectors.get(connector_name)
            if not connector or not connector.is_filtering_enable(device_name):
                return new_data

        to_send = {ATTRIBUTES_PARAMETER: [], TELEMETRY_PARAMETER: []}

        remaining_attributes_count = 0
        filtered_attributes_count = 0
        for attribute in new_data[ATTRIBUTES_PARAMETER]:
            for key, new_value in attribute.items():
                if self._update_latest_attribute_value(connector_name, device_name, key, new_value):
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
                if self._update_latest_telemetry_value(connector_name, device_name, key, new_value):
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

    def _update_latest_attribute_value(self, connector_name, device_name, key, value):
        return self._update_latest_value(connector_name, device_name, ATTRIBUTES_PARAMETER, key, value)

    def _update_latest_telemetry_value(self, connector_name, device_name, key, value):
        return self._update_latest_value(connector_name, device_name, TELEMETRY_PARAMETER, key, value)

    def _update_latest_value(self, connector_name, device_name, data_type, key, value):
        if connector_name not in self._connectors_data:
            self._connectors_data[connector_name] = {}

        if device_name not in self._connectors_data[connector_name]:
            self._connectors_data[connector_name][device_name] = {
                ATTRIBUTES_PARAMETER: {},
                TELEMETRY_PARAMETER: {}
            }
            self._connectors_data[connector_name][device_name][data_type][key] = value
            return True

        if self._connectors_data[connector_name][device_name][data_type].get(key) != value:
            self._connectors_data[connector_name][device_name][data_type][key] = value
            return True
        return False
