#     Copyright 2025. ThingsBoard
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
    TYPE_PARAMETER, AGGREGATION_FUNCTION_PARAMETER


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
    def __init__(self, config, default_report_strategy_config={}):
        if isinstance(config, ReportStrategyConfig):
            self.report_period = config.report_period
            self.report_strategy = config.report_strategy
            self.aggregation_function = config.aggregation_function
            self.__hash = config.__hash
            return

        if config is None or not isinstance(config, dict):
            raise ValueError("Report strategy config is not specified")
        if (config.get(REPORT_PERIOD_PARAMETER) is None and config.get(TYPE_PARAMETER) is None
                and not default_report_strategy_config):
            raise ValueError("Report strategy config is not specified")
        if default_report_strategy_config:
            self.report_period = max(config.get(REPORT_PERIOD_PARAMETER,
                                                default_report_strategy_config[REPORT_PERIOD_PARAMETER]) - 10, 1)
            report_strategy_type = config.get(TYPE_PARAMETER, default_report_strategy_config[TYPE_PARAMETER])
        else:
            self.report_period = max(config.get(REPORT_PERIOD_PARAMETER, 0), 1)
            report_strategy_type = config.get(TYPE_PARAMETER)
        self.report_strategy = ReportStrategy.from_string(report_strategy_type)
        if self.report_strategy not in (ReportStrategy.ON_REPORT_PERIOD, ReportStrategy.ON_CHANGE_OR_REPORT_PERIOD):
            self.report_period = None
        self.aggregation_function = config.get(AGGREGATION_FUNCTION_PARAMETER)
        self.__validate_config()
        self.__hash = hash((self.report_period, self.report_strategy, self.aggregation_function))

    def __validate_config(self):
        if (self.report_strategy in (ReportStrategy.ON_REPORT_PERIOD, ReportStrategy.ON_CHANGE_OR_REPORT_PERIOD)
                and (self.report_period is None or self.report_period <= 0)):
            raise ValueError("Invalid report period value: %r" % str(self.report_period))
        if (self.aggregation_function is not None
                and self.aggregation_function not in AggregationFunction.__members__.values()):
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
        return f"ReportStrategyConfig(report_period={self.report_period}, report_strategy={self.report_strategy},\
            aggregation_function={self.aggregation_function})"
