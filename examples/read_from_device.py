from tb_modbus_init import TBModbusInitializer
import logging
from time import sleep
logging.basicConfig(level=logging.DEBUG)
init = TBModbusInitializer(extension_id="modbus", gateway=None)
# works in a background, so we need script to not stop
while True:
    sleep(1)
