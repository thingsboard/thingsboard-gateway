from thingsboard_gateway.connectors.modbus.constants import PymodbusDefaults

from pymodbus.client.tls import AsyncModbusTlsClient as TlsClient
from pymodbus.framer.base import FramerType


class AsyncModbusTlsClient(TlsClient):
    def __init__(
        self,
        host: str,
        port: int = PymodbusDefaults.TlsPort,
        framer: FramerType = FramerType.TLS,
        certfile: str = None,
        keyfile: str = None,
        password: str = None,
        **kwargs: any,
    ):
        sslctx = AsyncModbusTlsClient.generate_ssl(certfile=certfile, keyfile=keyfile, password=password)
        retries = kwargs.pop('retries', PymodbusDefaults.Retries)
        timeout = kwargs.pop('timeout', PymodbusDefaults.Timeout)
        super().__init__(host=host, port=port, framer=framer, sslctx=sslctx, retries=retries, timeout=timeout)
