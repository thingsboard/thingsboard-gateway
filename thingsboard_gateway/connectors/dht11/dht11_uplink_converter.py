from tb_utility.tb_utility import TBUtility

class DhtUplinkConverter(TBUtility):
    def __init__(self, config):
        self.__config = config

    def convert(self, config, data):
        try:
            telemetry = {}
            telemetry["temperature"] = data["values"]["temperature"]
            telemetry["humidity"] = data["values"]["humidity"]
            telemetry["ts"] = data["ts"]
            return {"attributes": [],
                    "telemetry": [telemetry]}
        except Exception as e:
            print(f'Failed to convert telemetry: {e}')