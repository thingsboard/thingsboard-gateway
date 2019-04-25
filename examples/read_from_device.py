from tb_modbus_init import TBModbusInitializer
import logging
from time import sleep
logging.basicConfig(level=logging.DEBUG)
init = TBModbusInitializer()
# works in a background, so we need script to not stop
while True:
    sleep(1)
