from thingsboard_gateway.gateway.statistics_service import StatisticsService


class CustomCollectStatistics(StatisticsService.CollectStatistics):
    def __call__(self, func):
        def inner(*args, **kwargs):
            try:
                _, __, ___, data = args
                self.collect(self.start_stat_type, data)
            except ValueError:
                pass

            func(*args, **kwargs)

        return inner
