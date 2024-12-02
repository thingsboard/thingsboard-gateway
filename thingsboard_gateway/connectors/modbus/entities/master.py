#     Copyright 2024. ThingsBoard
#
#     Licensed under the Apache License, Version 2.0 (the "License");
#     you may not use this file except in compliance with the License.
#     You may obtain a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#     Unless required by applicable law or agreed to in writing, software
#     distributed under the License is distributed on an "AS IS" BASIS,
#     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#     See the License for the specific language governing permissions and
#     limitations under the License.

from asyncio import Lock

from pymodbus.client import AsyncModbusTlsClient, AsyncModbusTcpClient, AsyncModbusUdpClient, AsyncModbusSerialClient
from pymodbus.framer.ascii_framer import ModbusAsciiFramer
from pymodbus.framer.rtu_framer import ModbusRtuFramer
from pymodbus.framer.socket_framer import ModbusSocketFramer

from thingsboard_gateway.connectors.modbus.constants import SERIAL_CONNECTION_TYPE_PARAMETER

FRAMER_TYPE = {
    'rtu': ModbusRtuFramer,
    'socket': ModbusSocketFramer,
    'ascii': ModbusAsciiFramer
}

def with_lock_for_serial(func):
    async def wrapper(master, *args, **kwargs):
        if master.client_type == SERIAL_CONNECTION_TYPE_PARAMETER:
            await master.lock.acquire()

        resp = await func(master, *args, **kwargs)

        if master.client_type == SERIAL_CONNECTION_TYPE_PARAMETER:
            master.lock.release()

        return resp

    return wrapper


class Master:
    def __init__(self, client_type, client):
        self.lock = Lock()
        self.client_type = client_type.lower()
        self.__client = client

    def connected(self):
        return self.__client.connected

    @with_lock_for_serial
    async def connect(self):
        await self.__client.connect()

    @with_lock_for_serial
    async def close(self):
        await self.__client.close()

    @with_lock_for_serial
    async def read_coils(self, address, count, unit_id):
        return await self.__client.read_coils(address=address, count=count, slave=unit_id)

    @with_lock_for_serial
    async def read_discrete_inputs(self, address, count, unit_id):
        return await self.__client.read_discrete_inputs(address=address, count=count, slave=unit_id)

    @with_lock_for_serial
    async def read_holding_registers(self, address, count, unit_id):
        return await self.__client.read_holding_registers(address=address, count=count, slave=unit_id)

    @with_lock_for_serial
    async def read_input_registers(self, address, count, unit_id):
        return await self.__client.read_input_registers(address=address, count=count, slave=unit_id)

    @with_lock_for_serial
    async def write_coil(self, address, value, unit_id):
        return await self.__client.write_coil(address=address, value=value, slave=unit_id)

    @with_lock_for_serial
    async def write_register(self, address, value, unit_id):
        return await self.__client.write_register(address=address, value=value, slave=unit_id)

    @with_lock_for_serial
    async def write_coils(self, address, values, unit_id):
        return await self.__client.write_coils(address=address, values=values, slave=unit_id)

    @with_lock_for_serial
    async def write_registers(self, address, values, unit_id):
        return await self.__client.write_registers(address=address, values=values, slave=unit_id)

    def get_available_functions(self):
        return {
            1: self.read_coils,
            2: self.read_discrete_inputs,
            3: self.read_holding_registers,
            4: self.read_input_registers,
            5: self.write_coil,
            6: self.write_register,
            15: self.write_coils,
            16: self.write_registers,
        }

    @staticmethod
    def configure_master(config):
        framer = FRAMER_TYPE[config.method]

        if config.type == 'tcp' and config.tls:
            master = AsyncModbusTlsClient(config.host,
                                          config.port,
                                          framer,
                                          timeout=config.timeout,
                                          retry_on_empty=config.retry_on_empty,
                                          retry_on_invalid=config.retry_on_invalid,
                                          retries=config.retries,
                                          **config.tls)
        elif config.type == 'tcp':
            master = AsyncModbusTcpClient(config.host,
                                          config.port,
                                          framer,
                                          timeout=config.timeout,
                                          retry_on_empty=config.retry_on_empty,
                                          retry_on_invalid=config.retry_on_invalid,
                                          retries=config.retries)
        elif config.type == 'udp':
            master = AsyncModbusUdpClient(config.host,
                                          config.port,
                                          framer,
                                          timeout=config.timeout,
                                          retry_on_empty=config.retry_on_empty,
                                          retry_on_invalid=config.retry_on_invalid,
                                          retries=config.retries)
        elif config.type == 'serial':
            master = AsyncModbusSerialClient(method=config.method,
                                             port=config.port,
                                             timeout=config.timeout,
                                             retry_on_empty=config.retry_on_empty,
                                             retry_on_invalid=config.retry_on_invalid,
                                             retries=config.retries,
                                             baudrate=config.baudrate,
                                             stopbits=config.stopbits,
                                             bytesize=config.bytesize,
                                             parity=config.parity,
                                             strict=config.strict)
        else:
            raise Exception("Invalid Modbus transport type.")

        return master
