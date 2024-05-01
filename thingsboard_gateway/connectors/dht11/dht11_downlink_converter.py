from tb_utility.tb_utility import TBUtility

class DhtDownlinkConverter(TBUtility):
    def __init__(self, config):
        self.__config = config

    def convert(self, config, data):
        try:
            if data["method"] == "set_interval":
                return {"device": config["deviceName"], "data": data}
            else:
                print(f'Received unknown RPC method: {data["method"]}')
        except Exception as e:
            print(f'Failed to convert RPC request: {e}')