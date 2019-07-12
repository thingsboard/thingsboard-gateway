import logging
import time
from tb_gateway import TBGateway
logging.basicConfig(level=logging.DEBUG)
gateway = TBGateway("GatewayConfig.json")
while True:
    time.sleep(1)

