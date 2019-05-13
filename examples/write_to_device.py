from tb_modbus_init import TBModbusInitializer
import logging
from time import sleep
logging.basicConfig(level=logging.DEBUG)
# initializer is not meant to be used solo, None is for gateway which would create it and use
# "modbus 1" is extension id
init = TBModbusInitializer(None, "modbus")
# todo this is not final look of config, change it
#init.write_to_device({"deviceName": "Temp Sensor", "tag": "WriteCoil", "value": True, "functionCode": 5, "address": 2, "byteOrder": "BIG"})
sleep(1.5)
# init.write_to_device({"deviceName": "Temp Sensor", "tag": "WriteString", "value": "abcd", "functionCode": 16,
#                       "address": 0, "byteOrder": "BIG", "unitId": 1})
init.write_to_device({"deviceName": "Temp Sensor", "tag": "WriteInteger", "value": 42, "functionCode": 16,
                      "address": 0, "unitId": 1,"byteOrder": "LITTLE", "registerCount": 1})
# works in a background, so we need script to not stop
while True:
    sleep(1)
