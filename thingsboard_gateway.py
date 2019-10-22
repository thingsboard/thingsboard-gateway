from gateway.tb_gateway_service import TBGatewayService


def main():
    gateway = TBGatewayService("config/tb_gateway.yaml")

def daemon():
    gateway = TBGatewayService("/etc/thingsboard-gateway/tb_gateway.yaml")

if __name__ == '__main__':
    main()
