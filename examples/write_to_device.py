from tb_modbus_init import TBModbusInitializer
import logging
from time import sleep
logging.basicConfig(level=logging.DEBUG)
init = TBModbusInitializer()
# todo this is not final look of config, change it
#init.write_to_device({"deviceName": "Temp Sensor", "tag": "WriteCoil", "value": True, "functionCode": 5, "address": 2, "byteOrder": "BIG"})
sleep(3)
init.write_to_device({"deviceName": "Temp Sensor", "tag": "WriteString", "value": "abcd", "functionCode": 16,
                      "address": 0, "byteOrder": "BIG", "unitId": 1})
# works in a background, so we need script to not stop
while True:
    sleep(1)
