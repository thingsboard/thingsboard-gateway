import logging

from gateway._tb_gateway_service import TBGatewayService

logging.basicConfig(level=logging.DEBUG)

gateway = TBGatewayService("config/tb_gateway.json")

