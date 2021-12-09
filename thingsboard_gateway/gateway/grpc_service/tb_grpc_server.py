from queue import SimpleQueue
from threading import RLock, Thread
from time import sleep

import thingsboard_gateway.gateway.proto.messages_pb2_grpc as messages_pb2_grpc
from thingsboard_gateway.gateway.proto.messages_pb2 import *


class TBGRPCServer(messages_pb2_grpc.TBGatewayProtoServiceServicer):
    def __init__(self, read_callback):
        self._read_callback = read_callback
        self.__writing_queue_creation_lock = RLock()
        self.__writing_queues = {}
        self.__read_queue = SimpleQueue()
        self.__processing_thread = Thread(target=self.__processing_read, daemon=True, name="Read processing thread")
        self.__processing_thread.start()

    def write(self, session_id, msg: FromServiceMessage):
        if session_id not in self.__writing_queues:
            with self.__writing_queue_creation_lock:
                self.__writing_queues[session_id] = SimpleQueue()
        self.__writing_queues[session_id].put(msg, True, 10)

    def stream(self, request, context):
        session_id = self.get_session_id(context)
        self.__read_queue.put((session_id, request))
        data_to_send = None
        if self.__writing_queues.get(session_id) is not None and not self.__writing_queues[session_id].empty():
            data_to_send = self.__writing_queues[session_id].get_nowait()
            if self.__writing_queues[session_id].qsize() == 0:  # TODO Check writing and removing at the same time
                with self.__writing_queue_creation_lock:
                    del self.__writing_queues[session_id]
        if data_to_send is None:
            basic_msg = FromServiceMessage()
            basic_msg.response.MergeFrom(Response())
            data_to_send = basic_msg
        return data_to_send

    def __processing_read(self):
        while True:
            if not self.__read_queue.empty():
                context, request = self.__read_queue.get()
                self._read_callback(context, request)
            else:
                sleep(.2)

    @staticmethod
    def get_response(status, connector_message):
        msg = FromServiceMessage()
        if isinstance(status, str):
            msg.response.status = ResponseStatus.Value(status)
        else:
            msg.response.status = status
        if connector_message is not None:
            msg.response.connectorMessage.MergeFrom(connector_message)
        return msg

    @staticmethod
    def get_session_id(context):
        client_metadata = context.invocation_metadata()
        for key, value in client_metadata:
            if key == "identifier":
                return value
