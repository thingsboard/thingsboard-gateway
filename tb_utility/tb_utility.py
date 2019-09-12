class TBUtility:
    @staticmethod
    def get_parameter(data, param, default_value):
        if param not in data:
            return default_value
        else:
            return data[param]
