import logging

from gateway._tb_gateway_service import TBGatewayService

logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s - %(module)s - %(lineno)d - %(levelname)s - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')

gateway = TBGatewayService("config/tb_gateway.yaml")
