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
from typing import Tuple, Dict, Any

from thingsboard_gateway.gateway.constants import TELEMETRY_TIMESTAMP_PARAMETER, TELEMETRY_VALUES_PARAMETER
from thingsboard_gateway.gateway.entities.datapoint_key import DatapointKey
from thingsboard_gateway.gateway.entities.report_strategy_config import ReportStrategyConfig
from thingsboard_gateway.tb_utility.tb_utility import TBUtility


class TelemetryEntry:
    def __init__(self, values: Dict[DatapointKey, Any], ts=None):
        if values.get(TELEMETRY_TIMESTAMP_PARAMETER) and values.get(TELEMETRY_VALUES_PARAMETER):
            ts = values[TELEMETRY_TIMESTAMP_PARAMETER]
            values = values[TELEMETRY_VALUES_PARAMETER]
        elif ts is None:
            ts = int(time() * 1000)
        self.ts = ts
        self.values: Dict[DatapointKey, Any] = values
        self.data_size = TBUtility.get_data_size(self.to_dict())

    def __str__(self):
        return f"Telemetry(ts={self.ts}, values={self.values})"

    def __repr__(self):
        return self.__str__()

    def __hash__(self):
        return hash((self.ts, tuple(self.values.items())))

    def to_dict(self) -> Dict[str, Any]:
        res = {}
        for datapoint_key, value in self.values.items():
            if isinstance(datapoint_key, DatapointKey):
                res[datapoint_key.key] = value
            else:
                res[datapoint_key.key] = value
        return {TELEMETRY_TIMESTAMP_PARAMETER: self.ts, TELEMETRY_VALUES_PARAMETER: res}

    def __getitem__(self, item):
        if item == TELEMETRY_TIMESTAMP_PARAMETER:
            return self.ts
        elif item == TELEMETRY_VALUES_PARAMETER:
            return self.values
