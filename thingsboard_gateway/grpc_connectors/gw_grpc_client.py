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

from threading import Thread
from uuid import uuid4
import grpc
from grpc._channel import _MultiThreadedRendezvous
import queue
from time import sleep, time
from logging import getLogger

from thingsboard_gateway.gateway.proto import messages_pb2_grpc as messages_pb2_grpc
from thingsboard_gateway.grpc_connectors.gw_grpc_msg_creator import GrpcMsgCreator

log = getLogger('grpc')


class GrpcClient(Thread):
    def __init__(self, connect_callback, response_callback, host, port):
        super().__init__()
        self.__identifier = [("identifier", str(uuid4()))]
        self.__host = host
        self.__port = port
        self.on_connect = connect_callback
        self.daemon = True
        self.name = "TBGrpcClient thread"
        self.stopped = False
        self.connected = False
        self.channel = grpc.insecure_channel("%s:%i" % (self.__host, self.__port))
        self.stub = messages_pb2_grpc.TBGatewayProtoServiceStub(self.channel)
        self.output_queue = queue.SimpleQueue()
        self.__service_queue = queue.SimpleQueue()
        self.__request_data_queue = queue.SimpleQueue()
        self.__response_queue = queue.SimpleQueue()
        self.__response_callback = response_callback
        self.__processing_response_thread = Thread(target=self.__process_responses, daemon=True,
                                                   name="GRPC response processing thread")
        self.__processing_response_thread.start()
        self.__connection_thread = Thread(target=self.__reconnect, name="Grpc client connection thread", daemon=True)
        self.__connection_thread.start()
        self.last_response_received_time = time() * 1000

    def output_iter(self):
        if not self.__service_queue.empty():
            yield self.__service_queue.get()
        elif not self.output_queue.empty() and self.connected:
            yield self.output_queue.get()
        elif not self.__request_data_queue.empty() and self.connected:
            yield self.__request_data_queue.get()
        else:
            return None

    def send(self, message):
        # log.debug("Sending message to gateway %r", message)
        self.output_queue.put(message)

    def send_service_message(self, message):
        self.__service_queue.put(message)

    def send_get_data_message(self):
        message_to_gateway = GrpcMsgCreator.create_response_connector_msg(None)
        self.__request_data_queue.put(message_to_gateway)

    def run(self):
        while not self.stopped:
            try:
                data_exists = False
                for request in self.output_iter():
                    response = self.stub.stream(request, metadata=self.__identifier, timeout=15)
                    data_exists = True
                    if not self.connected:
                        self.__request_data_queue = queue.SimpleQueue()
                        self.connected = True
                    if response.HasField("response") and response.response.ByteSize() == 0 or \
                            (response.response.HasField(
                                "connectorMessage") and response.response.connectorMessage.HasField("response")):
                        self.last_response_received_time = time() * 1000
                        continue
                    log.debug("Received response; %r", response)
                    self.__response_queue.put(response)
                if not data_exists:
                    sleep(.2)
                else:
                    self.last_response_received_time = time() * 1000
            except grpc._channel._MultiThreadedRendezvous as e:
                log.error("Exception during exchanging data: %r", e.details())
                self.connected = False
            except grpc._channel._InactiveRpcError as e:
                self.connected = False
            except Exception as e:
                log.exception(e)

    def __reconnect(self):
        while not self.stopped:
            sleep(20)
            if self.stopped:
                break
            if not self.connected:
                if self.channel._connectivity_state.connectivity in [grpc.ChannelConnectivity.TRANSIENT_FAILURE,
                                                                     grpc.ChannelConnectivity.SHUTDOWN, None]:
                    if not self.is_server_available():
                        continue
                    else:
                        self.channel.close()
                        self.channel = grpc.insecure_channel("%s:%i" % (self.__host, self.__port))
                        self.stub = messages_pb2_grpc.TBGatewayProtoServiceStub(self.channel)
                        self.on_connect()

    def __process_responses(self):
        while not self.stopped:
            if not self.__response_queue.empty():
                self.__response_callback(self.__response_queue.get())
            else:
                sleep(.2)

    def is_server_available(self) -> bool:
        try:
            grpc.channel_ready_future(self.channel).result(timeout=15)
            return True
        except grpc.FutureTimeoutError:
            return False

    def stop(self):
        self.stopped = True
        self.channel.close()
