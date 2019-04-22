from tb_modbus_init import TBModbusInitializer
import logging
from time import sleep
logging.basicConfig(level=logging.DEBUG)
init = TBModbusInitializer()
init.start()
while True:
    sleep(1)
