import logging
import time
import threading
from gateway._tb_mqtt_client import GatewayClient


log = logging.getLogger(__name__)

class tb_gateway():
    def __init__(self):

        self.devices = {}
        self.gateway = GatewayClient()
        while not self.gateway._TBDeviceMqttClient__is_connected:
            try:
                self.gateway.connect()

            except Exception as e:
                log.error(e)
            log.debug("connecting to ThingsBoard...")
            time.sleep(.1)
        self.gateway.gw_connect_device('TEST_DEV')
        self.gateway.gw_send_telemetry("TEST_DEV", {"ts": int(round(time.time() * 1000)), "values": {"temperature": 42.2}})
        self.gateway.gw_send_attributes("TEST_DEV", {"firmwareVersion": "2.3.1"})
        log.debug(self.get_connected_devices())

        while True:
            try:
                time.sleep(.1)
            except Exception as e:
                self.disconnect_all()
                self.gateway.disconnect()
                raise(e)
            except(KeyboardInterrupt,SystemExit) as e:
                self.disconnect_all()
                self.gateway.disconnect()
                raise(e)




    def get_connected_devices(self):
        return self.gateway.get_devices()

    def disconnect_all(self):
        devices = [dev for dev in self.get_connected_devices() if dev != '*']
        for device in devices:
            log.debug('Disconnecting device: '+device)
            self.gateway.gw_disconnect_device(device)