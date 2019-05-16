import logging
import time
from apscheduler.schedulers.background import BackgroundScheduler
from tb_event_storage import TBEventStorage
scheduler = BackgroundScheduler()

logging.basicConfig(level=logging.DEBUG)
storage = TBEventStorage("./data", 3, 1, 10, 1, 2, scheduler, None)
i = 0
while True:
    i += 1
    storage.write(str(i) + '\n')
    time.sleep(1)
