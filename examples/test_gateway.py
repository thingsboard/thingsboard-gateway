import logging
from tb_gateway import TBGateway
logging.basicConfig(level=logging.DEBUG)
gateway = TBGateway("gateway_config.json")
import time
while True:
    time.sleep(1)