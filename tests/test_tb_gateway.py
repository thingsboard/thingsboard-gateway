import sys
import unittest
import logging
import time
import threading
sys.path.append('.')
print(sys.path)
from gateway._tb_mqtt_client import gateway_client

class DisconnectException(Exception):
    pass

log = logging.getLogger()
log.level = logging.DEBUG

class gateway_test(unittest.TestCase):
    def test_disconnect(self):
        gateway = gateway_client()
        print(gateway)
        gateway.connect()
        while not gateway._TBDeviceMqttClient__is_connected:
            time.sleep(.1)
        gateway.disconnect()
        for thread in threading.enumerate():
            if thread != threading.main_thread():
                self.assertEqual(thread.daemon,True)


if __name__ == "__main__":
    unittest.main()