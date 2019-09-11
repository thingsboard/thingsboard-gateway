import logging
import time
from threading import Lock
from gateway._tb_mqtt_client import GatewayClient


log = logging.getLogger(__name__)

class TB_Gateway():
    def __init__(self):

        self.devices = {}
        self.gateway = GatewayClient()
        while not self.gateway._TBDeviceMqttClient__is_connected:
            try:
                self.gateway.connect()

            except Exception as e:
                log.error(e)
            log.debug("connecting to ThingsBoard...")
            time.sleep(1)

        device_name = "TEST_DEV"

        self.gateway.gw_connect_device(device_name)
        self.gateway.gw_send_telemetry(device_name,{"ts": int(round(time.time() * 1000)), "values": {"temperature": -10.5}}).get()
        self.gateway.gw_send_attributes(device_name, {"firmwareVersion": "2.4"})
        self.gateway.gw_disconnect_device(device_name)
        self.gateway.disconnect()
