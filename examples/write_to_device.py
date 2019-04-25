from tb_modbus_init import TBModbusInitializer
import logging
from time import sleep
logging.basicConfig(level=logging.DEBUG)
init = TBModbusInitializer("test.json")
init.write_to_device({"deviceName": "Temp Sensor", "Temperature": "41.2"})
# works in a background, so we need script to not stop
while True:
    sleep(1)
