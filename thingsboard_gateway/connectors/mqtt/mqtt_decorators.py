from thingsboard_gateway.gateway.statistics.decorators import CollectStatistics


class CustomCollectStatistics(CollectStatistics):
    def __call__(self, func):
        def inner(*args, **kwargs):
            try:
                _, __, data, ___ = args
                self.collect(self.start_stat_type, data)
            except ValueError:
                pass

            return func(*args, **kwargs)

        return inner
