from thingsboard_gateway.gateway.grpc_service.grpc_connector import GrpcConnector
from thingsboard_gateway.gateway.grpc_service.tb_grpc_manager import TBGRPCServerManager


class GrpcSocketConnector(GrpcConnector):
    def __init__(self, gateway, config, tb_grpc_server_manager: TBGRPCServerManager, session_id):
        super().__init__(gateway, config, tb_grpc_server_manager, session_id)
