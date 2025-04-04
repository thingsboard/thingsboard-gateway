import functools
from pymodbus.client import AsyncModbusTcpClient, AsyncModbusUdpClient
from pymodbus.client.tcp import Defaults, ModbusFramer
from pymodbus.framer.tls_framer import ModbusTlsFramer
from pymodbus.client.tls import sslctx_provider


class AsyncModbusTcpClient(AsyncModbusTcpClient):
    async def _connect(self):
        transport, protocol = await self.loop.create_connection(
            self._create_protocol, host=self.params.host, port=self.params.port
        )
        return transport, protocol

    def protocol_lost_connection(self, protocol):
        """Notify lost connection."""
        self.connected = False
        self.protocol = None


class AsyncModbusUdpClient(AsyncModbusUdpClient):
    async def _connect(self):
        endpoint = await self.loop.create_datagram_endpoint(
            functools.partial(
                self._create_protocol, host=self.params.host, port=self.params.port
            ),
            remote_addr=(self.params.host, self.params.port),
        )
        return endpoint

    def protocol_lost_connection(self, protocol):
        """Notify lost connection.

        :meta private:
        """
        if self.connected:
            self.connected = False
            self.protocol = None


class AsyncModbusTlsClient(AsyncModbusTcpClient):
    def __init__(
        self,
        host: str,
        port: int = Defaults.TlsPort,
        framer: ModbusFramer = ModbusTlsFramer,
        sslctx: str = None,
        certfile: str = None,
        keyfile: str = None,
        password: str = None,
        server_hostname: str = None,
        **kwargs: any,
    ):
        """Initialize Asyncio Modbus TLS Client."""
        super().__init__(host, port=port, framer=framer, **kwargs)
        self.sslctx = sslctx_provider(sslctx, certfile, keyfile, password)
        self.params.sslctx = sslctx
        self.params.certfile = certfile
        self.params.keyfile = keyfile
        self.params.password = password
        self.params.server_hostname = server_hostname
        AsyncModbusTcpClient.__init__(self, host, port=port, framer=framer, **kwargs)

    async def _connect(self):
        return await self.loop.create_connection(
            self._create_protocol,
            self.params.host,
            self.params.port,
            ssl=self.sslctx,
            server_hostname=self.params.server_hostname,
        )
