#     Copyright 2020. ThingsBoard
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


import threading
from thingsboard_gateway.tb_utility.tb_utility import TBUtility
from socketserver import ThreadingTCPServer, BaseRequestHandler
import binascii
import time
from thingsboard_gateway.connectors.connector import Connector, log
from thingsboard_gateway.extensions.tcpserver.tcpserver_converter import BytesTcpUplinkConverter


global TCPServer_gateway
global TCPServer_devices
RequestHandlerDict = {}


class MyBaseRequestHandler(BaseRequestHandler):
    def setup(self):
        self.__devices = TCPServer_devices
        print("self.client_address:", self.client_address)

    def get_name(self):
        return 'TCP Server Connector'

    def handle(self):
        self.addr = self.request.getpeername()
        message = "IP " + self.addr[0] + ":" + str(self.addr[1]) + " Connected..."
        print(message)
        device_config = {}
        device_name = self.request.recv(128).decode()
        print(f'receive from {self.client_address}:{device_name}')
        RequestHandlerDict[device_name] = self.request

        if self.__devices.keys().__contains__(device_name):
            device_config = self.__devices[device_name]
        while True:
            time.sleep(0.1)
            self.process_device(device_config)

    def process_device(self, device_config):
        if not device_config:
            return
        current_time = time.time()
        device_responses = {"timeseries": {},
                            "attributes": {},
                            }
        for config_data in device_responses:
            if device_config["config"].get(config_data) is not None:
                if device_config["next_" + config_data + "_check"] < current_time:
                    send_data = device_config['config']['getDataCommand']
                    self.request.sendall(binascii.a2b_hex(send_data))
                    input_data = self.request.recv(128)
                    # print("self.client_address:", self.client_address, "send_data:", send_data, "recv_data:", binascii.b2a_hex(input_data))
                    log.debug("Checking %s for device %s", config_data, device_config['config']['deviceName'])
                    device_config["next_" + config_data + "_check"] = \
                        current_time + device_config["config"][config_data + "PollPeriod"] / 1000

                    converted_data = {}
                    try:
                        converted_data = device_config["converter"].convert(data=input_data)
                    except Exception as e:
                        log.error(e)

                    if converted_data and device_config["config"].get("sendDataOnlyOnChange"):
                        to_send = {"deviceName": converted_data["deviceName"],
                                   "deviceType": converted_data["deviceType"]}
                        if to_send.get("telemetry") is None:
                            to_send["telemetry"] = []
                        if to_send.get("attributes") is None:
                            to_send["attributes"] = []
                        for telemetry_dict in converted_data["timeseries"]:
                            for key, value in telemetry_dict.items():
                                if device_config["last_telemetry"].get(key) is None or \
                                        device_config["last_telemetry"][key] != value:
                                    device_config["last_telemetry"][key] = value
                                    to_send["telemetry"].append({key: value})
                        for attribute_dict in converted_data["attributes"]:
                            for key, value in attribute_dict.items():
                                if device_config["last_attributes"].get(key) is None or \
                                        device_config["last_attributes"][key] != value:
                                    device_config["last_attributes"][key] = value
                                    to_send["attributes"].append({key: value})
                        if to_send.get("attributes") or to_send.get("telemetry"):
                            TCPServer_gateway.send_to_storage(self.get_name(), to_send)
                        else:
                            log.debug("Data has not been changed.")
                    elif converted_data and device_config["config"].get(
                            "sendDataOnlyOnChange") is None or not device_config["config"].get(
                            "sendDataOnlyOnChange"):
                        to_send = {"deviceName": converted_data["deviceName"],
                                   "deviceType": converted_data["deviceType"]}
                        # if converted_data["telemetry"] != self.__devices[device]["telemetry"]:
                        device_config["last_telemetry"] = converted_data["timeseries"]
                        to_send["telemetry"] = converted_data["timeseries"]
                        # if converted_data["attributes"] != self.__devices[device]["attributes"]:
                        device_config["last_attributes"] = converted_data["attributes"]
                        to_send["attributes"] = converted_data["attributes"]
                        TCPServer_gateway.send_to_storage(self.get_name(), to_send)

class TCPServerConnector(Connector, threading.Thread):
    def __init__(self, gateway, config, connector_type):
        super().__init__()
        self.statistics = {'MessagesReceived': 0,
                           'MessagesSent': 0}
        self.__gateway = gateway
        self._connector_type = connector_type
        self.__server = None
        self.__config = config

        global TCPServer_gateway
        global TCPServer_devices
        self.__devices = {}
        self.__load_converters()

        TCPServer_gateway = gateway
        TCPServer_devices = self.__devices

        self.__connected = False
        self.__stopped = False
        self.setName(self.__config.get("name"))
        server_address = self.__config.get('host', 'localhost')
        server_port = self.__config.get('port', 502)
        self.tcp_server = ThreadingTCPServer((server_address, server_port), MyBaseRequestHandler)
        self.tcp_server.daemon_threads = True

    def get_name(self):
        return self.name

    def is_connected(self):
        return self.__connected

    def open(self):
        self.__stopped = False
        self.start()
        log.info("Starting TCPServer connector")

    def run(self):
        self.tcp_server.serve_forever()

    def close(self):
        self.__stopped = True
        self.__thread.join()
        log.info('%s has been stopped.', self.get_name())

    def __load_converters(self):
        try:
            for device in self.__config["devices"]:
                if self.__config.get("converter") is not None:
                    converter = TBUtility.check_and_import(self._connector_type, self.__config["converter"])(device)
                else:
                    converter = BytesTcpUplinkConverter(device)
                if device.get('deviceName') not in self.__gateway.get_devices():
                    self.__gateway.add_device(device.get('deviceName'), {"connector": self},
                                              device_type=device.get("deviceType"))
                self.__devices[device["deviceName"]] = {"config": device,
                                                        "converter": converter,
                                                        "next_attributes_check": 0,
                                                        "next_timeseries_check": 0,
                                                        "telemetry": {},
                                                        "attributes": {},
                                                        "last_telemetry": {},
                                                        "last_attributes": {}
                                                        }
        except Exception as e:
            log.exception(e)

    def on_attributes_update(self, content):
        try:
            for attribute_updates_command_config in self.__devices[content["device"]]["config"]["attributeUpdates"]:
                for attribute_updated in content["data"]:
                    if attribute_updates_command_config["tag"] == attribute_updated:
                        to_process = {
                            "device": content["device"],
                            "data": {
                                "method": attribute_updated,
                                "params": content["data"][attribute_updated]
                            }
                        }
                        self.__process_rpc_request(to_process, attribute_updates_command_config)
        except Exception as e:
            log.exception(e)

    def server_side_rpc_handler(self, content):
        try:
            if content.get("device") is not None:

                log.debug("TCPServer connector received rpc request for %s with content: %s", content["device"], content)
                if isinstance(self.__devices[content["device"]]["config"]["rpc"], dict):
                    rpc_command_config = self.__devices[content["device"]]["config"]["rpc"].get(content["data"]["method"])
                    if rpc_command_config is not None:
                        self.__process_rpc_request(content, rpc_command_config)
                elif isinstance(self.__devices[content["device"]]["config"]["rpc"], list):
                    for rpc_command_config in self.__devices[content["device"]]["config"]["rpc"]:
                        if rpc_command_config["tag"] == content["data"]["method"]:
                            self.__process_rpc_request(content, rpc_command_config)
                            break
                else:
                    log.error("Received rpc request, but method %s not found in config for %s.",
                              content["data"].get("method"),
                              self.get_name())
                    self.__gateway.send_rpc_reply(content["device"],
                                                  content["data"]["id"],
                                                  {content["data"]["method"]: "METHOD NOT FOUND!"})
            else:
                log.debug("Received RPC to connector: %r", content)
        except Exception as e:
            log.exception(e)

    def __process_rpc_request(self, content, rpc_command_config):
        if rpc_command_config is not None:
            print("content:", content)
            send_data = ""
            if content['data']['method'] == 'setValue':
                if content["data"]["params"]:
                    send_data = rpc_command_config['onCommand']
                else:
                    send_data = rpc_command_config['offCommand']
            if content['data']['method'] == 'openAlarm':
                print(content['data']['params'])
                vol = content['data']['params']['vol']
                song = content['data']['params']['song']
                send_data = "7EFF06080000" + song + "EF"
            if content['data']['method'] == 'closeAlarm':
                 send_data = '7EFF0616000000EF'
            if RequestHandlerDict.__contains__(content['device']):
                currentRequest = RequestHandlerDict[content['device']]
                currentRequest.sendall(binascii.a2b_hex(send_data))
                response = currentRequest.recv(128)
                self.__gateway.send_rpc_reply(content['device'], content['data']['id'],
                                              {content['data']['method']:str(response)})
