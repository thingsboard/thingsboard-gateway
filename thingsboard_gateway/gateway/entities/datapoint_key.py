# ------------------------------------------------------------------------------
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
#
# ------------------------------------------------------------------------------

from thingsboard_gateway.gateway.entities.report_strategy_config import ReportStrategyConfig


class DatapointKey:
    def __init__(self, key, report_strategy: ReportStrategyConfig = None):
        self.key = key
        self.report_strategy = report_strategy

    def __str__(self):
        return f"DatapointKey(key={self.key}, report_strategy={self.report_strategy})"

    def __repr__(self):
        return self.__str__()

    def __hash__(self):
        return hash((self.key, self.report_strategy))

    def __eq__(self, other):
        if isinstance(other, DatapointKey):
            return self.key == other.key and self.report_strategy == other.report_strategy
        return False
