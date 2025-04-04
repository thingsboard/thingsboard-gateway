#     Copyright 2025. ThingsBoard
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

from time import monotonic
from asyncio import Lock

from pymodbus.client import AsyncModbusSerialClient
from pymodbus.framer.ascii_framer import ModbusAsciiFramer
from pymodbus.framer.rtu_framer import ModbusRtuFramer
from pymodbus.framer.socket_framer import ModbusSocketFramer

from thingsboard_gateway.connectors.modbus.constants import SERIAL_CONNECTION_TYPE_PARAMETER
from thingsboard_gateway.connectors.modbus.entities.clients import (
    AsyncModbusTcpClient,
    AsyncModbusUdpClient,
    AsyncModbusTlsClient
)


FRAMER_TYPE = {
    'rtu': ModbusRtuFramer,
    'socket': ModbusSocketFramer,
    'ascii': ModbusAsciiFramer
}


def with_lock_for_serial(func):
    async def wrapper(master, *args, **kwargs):
        if master.client_type == SERIAL_CONNECTION_TYPE_PARAMETER:
            await master.lock.acquire()
        try:
            resp = await func(master, *args, **kwargs)
        finally:
            if master.client_type == SERIAL_CONNECTION_TYPE_PARAMETER:
                master.lock.release()

        return resp

    return wrapper


class Master:
    def __init__(self, client_type, client):
        self.lock = Lock()
        self.client_type = client_type.lower()
        self.__client = client
        self.__previous_request_time = 0

    def get_time_to_pass_delay_between_requests(self, delay_ms) -> int:
        if delay_ms == 0:
            return 0
        next_possible_request_time = self.__previous_request_time + delay_ms
        current_time = int(monotonic() * 1000)
        if current_time >= next_possible_request_time:
            return 0
        return next_possible_request_time - current_time

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
        result = await self.__client.read_coils(address=address, count=count, slave=unit_id) # noqa
        self.__previous_request_time = int(monotonic() * 1000)
        return result

    @with_lock_for_serial
    async def read_discrete_inputs(self, address, count, unit_id):
        result = await self.__client.read_discrete_inputs(address=address, count=count, slave=unit_id) # noqa
        self.__previous_request_time = int(monotonic() * 1000)
        return result

    @with_lock_for_serial
    async def read_holding_registers(self, address, count, unit_id):
        result = await self.__client.read_holding_registers(address=address, count=count, slave=unit_id) # noqa
        self.__previous_request_time = int(monotonic() * 1000)
        return result

    @with_lock_for_serial
    async def read_input_registers(self, address, count, unit_id):
        result = await self.__client.read_input_registers(address=address, count=count, slave=unit_id) # noqa
        self.__previous_request_time = int(monotonic() * 1000)
        return result

    @with_lock_for_serial
    async def write_coil(self, address, value, unit_id):
        result = await self.__client.write_coil(address=address, value=value, slave=unit_id) # noqa
        self.__previous_request_time = int(monotonic() * 1000)
        return result

    @with_lock_for_serial
    async def write_register(self, address, value, unit_id):
        result = await self.__client.write_register(address=address, value=value, slave=unit_id) # noqa
        self.__previous_request_time = int(monotonic() * 1000)
        return result

    @with_lock_for_serial
    async def write_coils(self, address, values, unit_id):
        result = await self.__client.write_coils(address=address, values=values, slave=unit_id) # noqa
        self.__previous_request_time = int(monotonic() * 1000)
        return result

    @with_lock_for_serial
    async def write_registers(self, address, values, unit_id):
        result = await self.__client.write_registers(address=address, values=values, slave=unit_id) # noqa
        self.__previous_request_time = int(monotonic() * 1000)
        return result

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
            master = AsyncModbusSerialClient(port=config.port,
                                             timeout=config.timeout,
                                             retry_on_empty=config.retry_on_empty,
                                             retries=config.retries,
                                             baudrate=config.baudrate,
                                             stopbits=config.stopbits,
                                             bytesize=config.bytesize,
                                             parity=config.parity,
                                             strict=config.strict,
                                             framer=framer)
        else:
            raise Exception("Invalid Modbus transport type.")

        return master
