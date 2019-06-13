from threading import Thread
from json import load, dump
import logging
import time
log = logging.getLogger(__name__)


class TBDeviceStorage(Thread):
    def __init__(self, gateway):
        super(TBDeviceStorage).__init__()
        self.gateway = gateway
        self.start()
        self.__run()

    def __run(self):
        while True:
            item = self.gateway.q.get()
            is_method_connect = item[0]
            device_name = item[1]
            # if method is "connect device"
            if is_method_connect:
                handler = item[2]
                rpc_handlers = item[3]
                self.gateway.mqtt_gateway.gw_connect_device(device_name)
                self.gateway.dict_ext_by_device_name.update({device_name: handler})
                self.gateway.dict_rpc_handlers_by_device.update({device_name: rpc_handlers})

                with open("connectedDevices.json") as f:
                    try:
                        connected_devices = load(f)
                    except:
                        connected_devices = {}
                if device_name in connected_devices:
                    log.debug("{} already in connected devices json".format(device_name))
                else:
                    connected_devices.update({device_name: {}})
                    with open("connectedDevices.json", "w") as f:
                        dump(connected_devices, f)
            # if method is "disconnect device"
            else:
                try:
                    self.gateway.dict_ext_by_device_name.pop(device_name)
                    with open("connectedDevices.json") as f:
                        try:
                            connected_devices = load(f)
                        except:
                            log.debug("there are no connected devices json")
                    if device_name not in connected_devices:
                        log.debug("{} not connected in json file".format(device_name))
                    else:
                        connected_devices.pop(device_name)
                        with open("connectedDevices.json", "w") as f:
                            dump(connected_devices, f)
                except KeyError:
                    log.warning("tried to remove {}, device not found".format(device_name))
            queue_size = self.gateway.q.qsize()
            if queue_size == 0:
                timeout = 0.5
            elif queue_size < 5:
                timeout = 0.1
            else:
                timeout = 0.05
            time.sleep(timeout)
