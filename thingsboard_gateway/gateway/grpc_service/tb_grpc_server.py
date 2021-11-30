import asyncio
from threading import Thread
from queue import SimpleQueue

import thingsboard_gateway.gateway.proto.messages_pb2_grpc as messages_pb2_grpc
from thingsboard_gateway.gateway.proto.messages_pb2 import *


class TBGRPCServer(messages_pb2_grpc.TBGatewayProtoServiceServicer):
    def __init__(self, read_callback):
        self._read_callback = read_callback
        self.__write_queue = SimpleQueue()

    def write(self, context, msg: FromServiceMessage):
        self.__write_queue.put(msg, True, 100)

    async def __read_task(self, context, request_iter):
        async for msg in request_iter:
            self._read_callback(context, msg)

    @staticmethod
    async def __write_task(context, msg: FromServiceMessage):
        await context.write(msg)

    async def stream(self,
                     request_iterator,
                     context,
                     options=(),
                     channel_credentials=None,
                     call_credentials=None,
                     insecure=False,
                     compression=None,
                     wait_for_ready=None,
                     timeout=None,
                     metadata=None):
        if not self.__write_queue.empty():
            data_to_send = self.__write_queue.get_nowait()
            write_task = asyncio.create_task(self.__write_task(context, data_to_send))
            await write_task
        else:
            read_task = asyncio.create_task(self.__read_task(context, request_iterator))
            await read_task

    @staticmethod
    def get_response(name):
        msg = FromServiceMessage()
        msg.response.status = ResponseStatus.Value(name)
        return msg
