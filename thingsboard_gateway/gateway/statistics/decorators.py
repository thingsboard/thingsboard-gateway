#      Copyright 2026. ThingsBoard
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

from thingsboard_gateway.gateway.statistics.statistics_service import StatisticsService


class StatisticsDecorator:
    def __init__(self, stat_type):
        self.stat_type = stat_type


class CollectStorageEventsStatistics(StatisticsDecorator):
    def __call__(self, func):
        def inner(*args, **kwargs):
            try:
                data = args[1]
                connector_name = args[2]
                if data:
                    StatisticsService.count_connector_message(connector_name, self.stat_type)
            except IndexError:
                pass

            func(*args, **kwargs)

        return inner


class CountMessage(StatisticsDecorator):
    def __call__(self, func):
        def inner(*args, **kwargs):
            StatisticsService.add_count(self.stat_type)
            result = func(*args, **kwargs)
            return result

        return inner
