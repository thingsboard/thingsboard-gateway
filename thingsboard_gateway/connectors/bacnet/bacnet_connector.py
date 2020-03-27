#      Copyright 2020. ThingsBoard
#  #
#      Licensed under the Apache License, Version 2.0 (the "License");
#      you may not use this file except in compliance with the License.
#      You may obtain a copy of the License at
#  #
#          http://www.apache.org/licenses/LICENSE-2.0
#  #
#      Unless required by applicable law or agreed to in writing, software
#      distributed under the License is distributed on an "AS IS" BASIS,
#      WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#      See the License for the specific language governing permissions and
#      limitations under the License.

from copy import deepcopy
from random import choice
from threading import Thread
from time import time, sleep
from string import ascii_lowercase
from bacpypes.core import run, stop
from bacpypes.pdu import Address, GlobalBroadcast, LocalBroadcast, LocalStation, RemoteStation
from thingsboard_gateway.connectors.connector import Connector, log
from thingsboard_gateway.connectors.bacnet.bacnet_utilities.tb_gateway_bacnet_application import TBBACnetApplication
from thingsboard_gateway.tb_utility.tb_utility import TBUtility


class BACnetConnector(Thread, Connector):
    def __init__(self, gateway, config, connector_type):
        self.__connector_type = connector_type
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
        self.__bacnet_core_thread = Thread(target=run, name="BACnet core thread")
        self.__bacnet_core_thread.start()
        self.__stopped = False
        self.__config_devices = self.__config["devices"]
        self.__send_whois_broadcast = self.__config["general"].get("sendWhoIsBroadcast", False)
        self.__send_whois_broadcast_period = self.__config.get("sendWhoIsBroadcastPeriod", 5000)
        self.__send_whois_broadcast_previous_time = 0
        self.default_converters = {"uplink_converter": TBUtility.check_and_import(self.__connector_type, "BACnetUplinkConverter"),
                                     "downlink_converter": TBUtility.check_and_import(self.__connector_type, "BACnetDownlinkConverter")}
        self.__request_functions = {"writeProperty": self._application.do_write_property}
        self.__available_object_resources = {}
        self.rpc_requests_in_progress = {}
        self.__connected = False
        self.daemon = True

    def open(self):
        self.__stopped = False
        self.start()

    def run(self):
        self.__connected = True
        self.scan_network()
        self._application.do_whois()
        log.debug("WhoIsRequest has been sent.")
        while not self.__stopped:
            sleep(1)
            cur_time = time() * 1000
            if self.__send_whois_broadcast and cur_time - self.__send_whois_broadcast_previous_time >= self.__send_whois_broadcast_period:
                self.__send_whois_broadcast_previous_time = cur_time
                self.scan_network()
            for device in self.__devices:
                try:
                    if device.get("previous_check") is None or cur_time - device["previous_check"] >= device["poll_period"]:
                        for mapping_type in ["attributes", "telemetry"]:
                            for mapping_object in device[mapping_type]:
                                if mapping_object.get("uplink_converter") is None or mapping_object.get("downlink_converter") is None:
                                    self.__load_converters(device)
                                data_to_application = {
                                    "device": device,
                                    "mapping_type": mapping_type,
                                    "mapping_object": mapping_object,
                                    "callback": self.__bacnet_device_mapping_response_cb
                                }
                                self._application.do_read_property(**data_to_application)
                        device["previous_check"] = cur_time
                    else:
                        sleep(.1)
                except Exception as e:
                    log.exception(e)

    def close(self):
        self.__stopped = True
        self.__connected = False
        stop()

    def get_name(self):
        return self.name

    def is_connected(self):
        return self.__connected

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
                                kwargs["config"].update({"propertyValue": content["data"]["params"]})
                                iocb = request["iocb"][0](device, **kwargs)
                                self.__request_functions[request["config"]["requestType"]](iocb, self.__rpc_response_cb)
                                self.rpc_requests_in_progress[iocb] = {"content": content,
                                                                       "converter": request["uplink_converter"]}
                                self.__gateway.register_rpc_request_timeout(content,
                                                                            request["config"].get("requestTimeout", 200),
                                                                            iocb,
                                                                            self.__rpc_cancel_processing)
                        else:
                            log.error("\"requestType\" not found in request configuration for key %s device: %s",
                                      request.get("key", "[KEY IS EMPTY]"),
                                      device["deviceName"])
                    if not method_found:
                        log.error("RPC method %s not found in configuration", content["data"]["method"])
                        self.__gateway.send_rpc_reply(content["device"], content["data"]["id"], success_sent=False)
        except Exception as e:
            log.exception(e)

    def __rpc_response_cb(self, iocb):
        device = self.rpc_requests_in_progress[iocb]
        converter = device["uplink_converter"]
        content = device["content"]
        if iocb.ioResponse:
            apdu = iocb.ioResponse
            log.debug("Received callback with Response: %r", apdu)
            converted_data = converter.convert(None, apdu)
            self.__gateway.send_rpc_reply(content["device"], content["data"]["id"], converted_data)
        elif iocb.ioError:
            log.exception("Received callback with Error: %r", iocb.ioError)
            data = {"error": str(iocb.ioError)}
            self.__gateway.send_rpc_reply(content["device"], content["data"]["id"], data)
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
                        for mapping_object in device[mapping_type]:
                            data_to_application = {
                                "device": device,
                                "mapping_type": mapping_type,
                                "mapping_object": mapping_object,
                                "callback": self.__bacnet_device_mapping_response_cb
                            }
                            self._application.do_read_property(**data_to_application)
            except Exception as e:
                log.exception(e)

    def __bacnet_device_mapping_response_cb(self, iocb, callback_params):
        converter = callback_params["mapping_object"]["uplink_converter"]
        mapping_type = callback_params["mapping_type"]
        config = callback_params["mapping_object"]
        converted_data = {}
        try:
            converted_data = converter.convert((mapping_type, config), iocb.ioResponse if iocb.ioResponse else iocb.ioError)
        except Exception as e:
            log.exception(e)
        self.__gateway.send_to_storage(self.name, converted_data)

    def __load_converters(self, device):
        datatypes = ["attributes", "timeseries", "attributeUpdates", "serverSideRpc"]
        for datatype in datatypes:
            for datatype_config in device.get(datatype, []):
                try:
                    for converter_type in self.default_converters:
                        converter_object = self.default_converters[converter_type] if datatype_config.get("class") is None else TBUtility.check_and_import(self.__connector_type, device.get("class"))
                        datatype_config[converter_type] = converter_object(device)
                except Exception as e:
                    log.exception(e)

    def add_device(self, data):
        if self.__devices_address_name.get(data["address"]) is None:
            for device in self.__config_devices:
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
                            (isinstance(config_address, LocalBroadcast) and isinstance(device["address"], LocalStation)) or \
                            (isinstance(config_address, (LocalStation, RemoteStation)) and isinstance(data["address"], (
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
