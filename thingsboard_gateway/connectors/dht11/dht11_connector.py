import json
import time
import threading
from tb_utility.tb_loader import TBModuleLoader
import dht11

class Dht11Connector(TBModuleLoader):
    def __init__(self, config):
        self.devices = []
        self.load_config(config)
        self.connected = False
        self.stopped = False

    def load_config(self, config):
        self.__config = config
        devices = config.get("devices")
        for device in devices:
            self.devices.append(device)

    def open(self):
        self.connected = True
        self.__collect_thread = threading.Thread(target=self.__collect_data, daemon=True, name='DHT11 Collect Thread')
        self.__collect_thread.start()

    def close(self):
        self.stopped = True
        if self.__collect_thread is not None:
            self.__collect_thread.join()

    def get_name(self):
        return 'DHT11 Connector'

    def is_connected(self):
        return self.connected

    def on_attributes_update(self, content):
        pass

    def server_side_rpc_handler(self, content):
        # 处理Thingsboard下发的RPC请求
        print(f'Received RPC command: {content}')
        device = content["device"]  
        data = content["data"]
        if data.get("method") == "set_interval":
            interval = data["params"] 
            for dev in self.devices:
                if dev["name"] == device:
                    dev["collectInterval"] = interval
                    print(f'Set collect interval to {interval} for {device}')
        
    def __collect_data(self):
        while not self.stopped:
            for device in self.devices:
                try:
                    temp, humi = dht11.read_data(device["gpioData"])
                    telemetry = {
                        "ts": int(time.time()*1000),
                        "values": {
                            "temperature": temp,
                            "humidity": humi
                        }
                    }
                    self.send_to_storage(device["name"], telemetry) 
                except Exception as e:
                    print(f'Failed to collect data from DHT11: {e}')
                time.sleep(device["collectInterval"]/1000)