import logging
import time

from tb_event_storage import TBEventStorage
logging.basicConfig(level=logging.DEBUG)

storage = TBEventStorage("./data", 3, 1, 10)

i = 0
while True:
    i += 1
    storage.write(str(i) + '\n')
    time.sleep(1)


