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

from enum import Enum

from thingsboard_gateway.gateway.constants import REPORT_PERIOD_PARAMETER, ReportStrategy, \
    DEFAULT_REPORT_STRATEGY_CONFIG, TYPE_PARAMETER, AGGREGATION_FUNCTION_PARAMETER


class AggregationFunction(Enum):
    NONE = "NONE"
    SUM = "SUM"
    COUNT = "COUNT"
    MIN = "MIN"
    MAX = "MAX"
    AVERAGE = "AVERAGE"

    @classmethod
    def from_string(cls, value: str):
        for agg_function in cls:
            if agg_function.value.upper() == value.upper():
                return agg_function
        raise ValueError("Invalid aggregation function value: %r" % value)


class ReportStrategyConfig:
    def __init__(self, config):
        self.report_period = max(config.get(REPORT_PERIOD_PARAMETER, DEFAULT_REPORT_STRATEGY_CONFIG[REPORT_PERIOD_PARAMETER]) - 10, 1)
        report_strategy_type = config.get(TYPE_PARAMETER, DEFAULT_REPORT_STRATEGY_CONFIG[TYPE_PARAMETER])
        self.report_strategy = ReportStrategy.from_string(report_strategy_type)
        self.aggregation_function = config.get(AGGREGATION_FUNCTION_PARAMETER)
        self.__validate_config()
        self.__hash = hash((self.report_period, self.report_strategy, self.aggregation_function))

    def __validate_config(self):
        if self.report_strategy not in (ReportStrategy.ON_CHANGE, ReportStrategy.ON_RECEIVED) and (self.report_period is None or self.report_period <= 0):
            raise ValueError("Invalid report period value: %r" % str(self.report_period))
        if self.aggregation_function is not None and self.aggregation_function not in AggregationFunction.__members__.values():
            raise ValueError("Invalid aggregation function value: %r" % self.aggregation_function)

    def __hash__(self):
        return self.__hash

    def __repr__(self):
        return self.__str__()

    def __eq__(self, other):
        return (isinstance(other, ReportStrategyConfig)
                and self.report_period == other.report_period
                and self.report_strategy == other.report_strategy
                and self.aggregation_function == other.aggregation_function)

    def __str__(self):
        return f"ReportStrategyConfig(report_period={self.report_period}, report_strategy={self.report_strategy}, aggregation_function={self.aggregation_function})"