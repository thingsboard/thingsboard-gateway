#     Copyright 2022. ThingsBoard
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

from queue import Queue
from random import choice
from string import ascii_lowercase
from threading import Thread
from time import sleep, time

from thingsboard_gateway.gateway.statistics_service import StatisticsService
from thingsboard_gateway.tb_utility.tb_loader import TBModuleLoader
from thingsboard_gateway.tb_utility.tb_utility import TBUtility

try:
    from bacpypes.core import run, stop
except ImportError:
    print("BACnet library not found - installing...")
    TBUtility.install_package("bacpypes", ">=0.18.0")
    from bacpypes.core import run, stop

from bacpypes.pdu import Address, GlobalBroadcast, LocalBroadcast, LocalStation, RemoteStation

from thingsboard_gateway.connectors.connector import Connector, log
from thingsboard_gateway.connectors.bacnet.bacnet_utilities.tb_gateway_bacnet_application import TBBACnetApplication


class BACnetConnector(Thread, Connector):
    def __init__(self, gateway, config, connector_type):
        self._connector_type = connector_type
        self.statistics = {'MessagesReceived': 0,
                           'MessagesSent': 0}
        super().__init__()
        self.__config = config
        self.setName(config.get('name', 'BACnet ' + ''.join(choice(ascii_lowercase) for _ in range(5))))
        self.__devices = []
        self.__device_indexes = {}
        self.__devices_address_name = {}
        self.__gateway = gateway
        self._application = TBBACnetApplication(self, self.__config)
        self.__bacnet_core_thread = Thread(target=run, name="BACnet core thread", daemon=True,
                                           kwargs={"sigterm": None, "sigusr1": None})
        self.__bacnet_core_thread.start()
        self.__stopped = False
        self.__config_devices = self.__config["devices"]
        self.default_converters = {
            "uplink_converter": TBModuleLoader.import_module(self._connector_type, "BACnetUplinkConverter"),
            "downlink_converter": TBModuleLoader.import_module(self._connector_type, "BACnetDownlinkConverter")}
        self.__request_functions = {"writeProperty": self._application.do_write_property,
                                    "readProperty": self._application.do_read_property,
                                    "risingEdge": self._application.do_binary_rising_edge}
        self.__available_object_resources = {}
        self.rpc_requests_in_progress = {}
        self.__connected = False
        self.daemon = True
        self.__convert_and_save_data_queue = Queue()

    def open(self):
        self.__stopped = False
        self.start()

    def run(self):
        self.__connected = True
        self.scan_network()
        self._application.do_whois()
        log.debug("WhoIsRequest has been sent.")
        self.scan_network()
        while not self.__stopped:
            sleep(.2)
            for device in self.__devices:
                try:
                    if device.get("previous_check") is None or time() * 1000 - device["previous_check"] >= device[
                            "poll_period"]:
                        for mapping_type in ["attributes", "telemetry"]:
                            for config in device[mapping_type]:
                                if config.get("uplink_converter") is None or config.get("downlink_converter") is None:
                                    self.__load_converters(device)
                                data_to_application = {
                                    "device": device,
                                    "mapping_type": mapping_type,
                                    "config": config,
                                    "callback": self.__bacnet_device_mapping_response_cb
                                }
                                self._application.do_read_property(**data_to_application)
                        device["previous_check"] = time() * 1000
                    else:
                        sleep(.2)
                except Exception as e:
                    log.exception(e)

            if not self.__convert_and_save_data_queue.empty():
                for _ in range(self.__convert_and_save_data_queue.qsize()):
                    thread = Thread(target=self.__convert_and_save_data, args=(self.__convert_and_save_data_queue,),
                                    daemon=True)
                    thread.start()

    def close(self):
        self.__stopped = True
        self.__connected = False
        stop()

    def get_name(self):
        return self.name

    def is_connected(self):
        return self.__connected

    @StatisticsService.CollectAllReceivedBytesStatistics(start_stat_type='allReceivedBytesFromTB')
    def on_attributes_update(self, content):
        try:
            log.debug('Recieved Attribute Update Request: %r', str(content))
            for device in self.__devices:
                if device["deviceName"] == content["device"]:
                    for request in device["attribute_updates"]:
                        if request["config"].get("requestType") is not None:
                            for attribute in content["data"]:
                                if attribute == request["key"]:
                                    request["iocb"][1]["config"].update({"propertyValue": content["data"][attribute]})
                                    kwargs = request["iocb"][1]
                                    iocb = request["iocb"][0](device, **kwargs)
                                    self.__request_functions[request["config"]["requestType"]](iocb)
                                    return
                        else:
                            log.error("\"requestType\" not found in request configuration for key %s device: %s",
                                      request.get("key", "[KEY IS EMPTY]"),
                                      device["deviceName"])
        except Exception as e:
            log.exception(e)

    @StatisticsService.CollectAllReceivedBytesStatistics(start_stat_type='allReceivedBytesFromTB')
    def server_side_rpc_handler(self, content):
        try:
            log.debug('Recieved RPC Request: %r', str(content))
            for device in self.__devices:
                if device["deviceName"] == content["device"]:
                    method_found = False
                    for request in device["server_side_rpc"]:
                        if request["config"].get("requestType") is not None:
                            if content["data"]["method"] == request["method"]:
                                method_found = True
                                kwargs = request["iocb"][1]
                                timeout = time() * 1000 + request["config"].get("requestTimeout", 200)
                                if content["data"].get("params") is not None:
                                    kwargs["config"].update({"propertyValue": content["data"]["params"]})
                                iocb = request["iocb"][0](device, **kwargs)
                                self.__request_functions[request["config"]["requestType"]](device=iocb,
                                                                                           callback=self.__rpc_response_cb)
                                self.rpc_requests_in_progress[iocb] = {"content": content,
                                                                       "uplink_converter": request["uplink_converter"]}
                                # self.__gateway.register_rpc_request_timeout(content,
                                #                                             timeout,
                                #                                             iocb,
                                #                                             self.__rpc_cancel_processing)
                        else:
                            log.error("\"requestType\" not found in request configuration for key %s device: %s",
                                      request.get("key", "[KEY IS EMPTY]"),
                                      device["deviceName"])
                    if not method_found:
                        log.error("RPC method %s not found in configuration", content["data"]["method"])
                        self.__gateway.send_rpc_reply(content["device"], content["data"]["id"], success_sent=False)
        except Exception as e:
            log.exception(e)

    def __rpc_response_cb(self, iocb, callback_params=None):
        device = self.rpc_requests_in_progress[iocb]
        converter = device["uplink_converter"]
        content = device["content"]
        if iocb.ioResponse:
            apdu = iocb.ioResponse
            log.debug("Received callback with Response: %r", apdu)
            converted_data = converter.convert(None, apdu)
            if converted_data is None:
                converted_data = {"success": True}
            self.__gateway.send_rpc_reply(content["device"], content["data"]["id"], converted_data)
            # self.__gateway.rpc_with_reply_processing(iocb, converted_data or {"success": True})
        elif iocb.ioError:
            log.exception("Received callback with Error: %r", iocb.ioError)
            data = {"error": str(iocb.ioError)}
            self.__gateway.send_rpc_reply(content["device"], content["data"]["id"], data)
            log.debug(iocb.ioError)
        else:
            log.error("Received unknown RPC response callback from device: %r", iocb)

    def __rpc_cancel_processing(self, iocb):
        log.info("RPC with iocb %r - cancelled.", iocb)

    def scan_network(self):
        self._application.do_whois()
        log.debug("WhoIsRequest has been sent.")
        for device in self.__config_devices:
            try:
                if self._application.check_or_add(device):
                    for mapping_type in ["attributes", "timeseries"]:
                        for config in device[mapping_type]:
                            if config.get("uplink_converter") is None or config.get("downlink_converter") is None:
                                self.__load_converters(device)
                            data_to_application = {
                                "device": device,
                                "mapping_type": mapping_type,
                                "config": config,
                                "callback": self.__bacnet_device_mapping_response_cb
                            }
                            self._application.do_read_property(**data_to_application)
            except Exception as e:
                log.exception(e)

    def __convert_and_save_data(self, queue):
        converter, mapping_type, config, iocb = queue.get()
        converted_data = {}
        try:
            converted_data = converter.convert((mapping_type, config),
                                               iocb.ioResponse if iocb.ioResponse else iocb.ioError)
        except Exception as e:
            log.exception(e)
        self.__gateway.send_to_storage(self.name, converted_data)

    def __bacnet_device_mapping_response_cb(self, iocb, callback_params):
        mapping_type = callback_params["mapping_type"]
        config = callback_params["config"]
        converted_data = {}
        converter = callback_params["config"].get("uplink_converter")
        if converter is None:
            for device in self.__devices:
                self.__load_converters(device)
            else:
                converter = callback_params["config"].get("uplink_converter")
        try:
            converted_data = converter.convert((mapping_type, config),
                                               iocb.ioResponse if iocb.ioResponse else iocb.ioError)
        except Exception as e:
            log.exception(e)
        self.__gateway.send_to_storage(self.name, converted_data)

    def __load_converters(self, device):
        datatypes = ["attributes", "telemetry", "attribute_updates", "server_side_rpc"]
        for datatype in datatypes:
            for datatype_config in device.get(datatype, []):
                try:
                    for converter_type in self.default_converters:
                        converter_object = self.default_converters[converter_type] if datatype_config.get(
                            "class") is None else TBModuleLoader.import_module(self._connector_type,
                                                                               device.get("class"))
                        datatype_config[converter_type] = converter_object(device)
                except Exception as e:
                    log.exception(e)

    def add_device(self, data):
        if self.__devices_address_name.get(data["address"]) is None:
            for device in self.__config_devices:
                if device["address"] == data["address"]:
                    try:
                        config_address = Address(device["address"])
                        device_name_tag = TBUtility.get_value(device["deviceName"], get_tag=True)
                        device_name = device["deviceName"].replace("${" + device_name_tag + "}", data.pop("name"))
                        device_information = {
                            **data,
                            **self.__get_requests_configs(device),
                            "type": device["deviceType"],
                            "config": device,
                            "attributes": device.get("attributes", []),
                            "telemetry": device.get("timeseries", []),
                            "poll_period": device.get("pollPeriod", 5000),
                            "deviceName": device_name,
                        }
                        if config_address == data["address"] or \
                                (config_address, GlobalBroadcast) or \
                                (isinstance(config_address, LocalBroadcast) and isinstance(device["address"],
                                                                                           LocalStation)) or \
                                (isinstance(config_address, (LocalStation, RemoteStation)) and isinstance(
                                    data["address"], (
                                            LocalStation, RemoteStation))):
                            self.__devices_address_name[data["address"]] = device_information["deviceName"]
                            self.__devices.append(device_information)

                        log.debug(data["address"].addrType)
                    except Exception as e:
                        log.exception(e)

    def __get_requests_configs(self, device):
        result = {"attribute_updates": [], "server_side_rpc": []}
        for request in device.get("attributeUpdates", []):
            kwarg_dict = {
                "config": request,
                "request_type": request["requestType"]
            }
            request_config = {
                "key": request["key"],
                "iocb": (self._application.form_iocb, kwarg_dict),
                "config": request
            }
            result["attribute_updates"].append(request_config)
        for request in device.get("serverSideRpc", []):
            kwarg_dict = {
                "config": request,
                "request_type": request["requestType"]
            }
            request_config = {
                "method": request["method"],
                "iocb": (self._application.form_iocb, kwarg_dict),
                "config": request
            }
            result["server_side_rpc"].append(request_config)
        return result

    def get_config(self):
        return self.__config
