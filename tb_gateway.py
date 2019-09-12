import logging

from gateway._tb_gateway_service import tb_gateway

logging.basicConfig(level=logging.DEBUG)

gateway = tb_gateway()
