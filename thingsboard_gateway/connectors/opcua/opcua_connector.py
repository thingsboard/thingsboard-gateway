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

import re
from simplejson import dumps
import time
from threading import Thread
from random import choice
from string import ascii_lowercase
from opcua import Client, ua
from thingsboard_gateway.tb_utility.tb_utility import TBUtility
from opcua.ua.uaerrors._auto import BadWaitingForInitialData
from thingsboard_gateway.connectors.connector import Connector, log
from thingsboard_gateway.connectors.opcua.opcua_uplink_converter import OpcUaUplinkConverter


class OpcUaConnector(Thread, Connector):
    def __init__(self, gateway, config, connector_type):
        self.__connector_type = connector_type
        self.statistics = {'MessagesReceived': 0,
                           'MessagesSent': 0}
        super().__init__()
        self.__gateway = gateway
        self.__server_conf = config.get("server")
        self.__interest_nodes = []
        self.__available_object_resources = {}
        self.__show_map = config.get("showMap", False)
        for mapping in self.__server_conf["mapping"]:
            if mapping.get("deviceNodePattern") is not None:
                self.__interest_nodes.append({mapping["deviceNodePattern"]: mapping})
            else:
                log.error("deviceNodePattern in mapping: %s - not found, add property deviceNodePattern to processing this mapping",
                          dumps(mapping))
        if "opc.tcp" not in self.__server_conf.get("url"):
            opcua_url = "opc.tcp://"+self.__server_conf.get("url")
        else:
            opcua_url = self.__server_conf.get("url")
        self.client = Client(opcua_url, timeout=self.__server_conf.get("timeoutInMillis", 4000)/1000)
        if self.__server_conf["identity"]["type"] == "cert.PEM":
            try:
                ca_cert = self.__server_conf["identity"].get("caCert")
                private_key = self.__server_conf["identity"].get("privateKey")
                cert = self.__server_conf["identity"].get("cert")
                security_mode = self.__server_conf["identity"].get("mode", "SignAndEncrypt")
                policy = self.__server_conf["security"]
                if cert is None or private_key is None:
                    log.exception("Error in ssl configuration - cert or privateKey parameter not found")
                    raise
                security_string = policy+','+security_mode+','+cert+','+private_key
                if ca_cert is not None:
                    security_string = security_string + ',' + ca_cert
                self.client.set_security_string(security_string)

            except Exception as e:
                log.exception(e)
        if self.__server_conf["identity"].get("username"):
            self.client.set_user(self.__server_conf["identity"].get("username"))
            if self.__server_conf["identity"].get("password"):
                self.client.set_password(self.__server_conf["identity"].get("password"))

        self.setName(self.__server_conf.get("name", 'OPC-UA ' + ''.join(choice(ascii_lowercase) for _ in range(5))) + " Connector")
        self.__opcua_nodes = {}
        self._subscribed = {}
        self.data_to_send = []
        self.__sub_handler = SubHandler(self)
        self.__stopped = False
        self.__connected = False
        self.daemon = True

    def is_connected(self):
        return self.__connected

    def open(self):
        self.__stopped = False
        self.start()
        log.info("Starting OPC-UA Connector")

    def run(self):
        while not self.__connected:
            try:
                self.__connected = self.client.connect()
                self.client.load_type_definitions()
                log.debug(self.client.get_namespace_array()[-1])
                log.debug(self.client.get_namespace_index(self.client.get_namespace_array()[-1]))
            except ConnectionRefusedError:
                log.error("Connection refused on connection to OPC-UA server with url %s", self.__server_conf.get("url"))
                time.sleep(10)
            except Exception as e:
                log.debug("error on connection to OPC-UA server.")
                log.error(e)
                time.sleep(10)
            else:
                self.__connected = True
                log.info("OPC-UA connector %s connected to server %s", self.get_name(), self.__server_conf.get("url"))
        self.__opcua_nodes["root"] = self.client.get_root_node()
        self.__opcua_nodes["objects"] = self.client.get_objects_node()
        self.__sub = self.client.create_subscription(self.__server_conf.get("scanPeriodInMillis", 500), self.__sub_handler)
        self.__scan_nodes_from_config()
        log.debug('Subscriptions: %s', self.subscribed)
        log.debug("Available methods: %s", self.__available_object_resources)
        while True:
            try:
                time.sleep(1)
                if self.data_to_send:
                    self.__gateway.send_to_storage(self.get_name(), self.data_to_send.pop())
                if self.__stopped:
                    self.close()
                    break
            except (KeyboardInterrupt, SystemExit):
                self.close()
                raise
            except Exception as e:
                self.close()
                log.exception(e)

    def close(self):
        self.__stopped = True
        if self.__connected:
            self.client.disconnect()
        self.__connected = False
        log.info('%s has been stopped.', self.get_name())

    def get_name(self):
        return self.name

    def on_attributes_update(self, content):
        log.debug(content)
        try:
            for server_variables in self.__available_object_resources[content["device"]]['variables']:
                for attribute in content["data"]:
                    for variable in server_variables:
                        if attribute == variable:
                            server_variables[variable].set_value(content["data"][variable])
        except Exception as e:
            log.exception(e)

    def server_side_rpc_handler(self, content):
        try:
            for method in self.__available_object_resources[content["device"]]['methods']:
                rpc_method = content["data"].get("method")
                if rpc_method is not None and method.get(rpc_method) is not None:
                    arguments_from_config = method["arguments"]
                    arguments = content["data"].get("params") if content["data"].get("params") is not None else arguments_from_config
                    try:
                        if type(arguments) is list:
                            result = method["node"].call_method(method[rpc_method], *arguments)
                        elif arguments is not None:
                            result = method["node"].call_method(method[rpc_method], arguments)
                        else:
                            result = method["node"].call_method(method[rpc_method])

                        self.__gateway.send_rpc_reply(content["device"],
                                                      content["data"]["id"],
                                                      {content["data"]["method"]: result, "code": 200})
                        log.debug("method %s result is: %s", method[rpc_method], result)
                    except Exception as e:
                        log.exception(e)
                        self.__gateway.send_rpc_reply(content["device"], content["data"]["id"],
                                                      {"error": str(e), "code": 500})
                else:
                    log.error("Method %s not found for device %s", rpc_method, content["device"])
                    self.__gateway.send_rpc_reply(content["device"], content["data"]["id"], {"error": "%s - Method not found" % (rpc_method), "code": 404})
        except Exception as e:
            log.exception(e)

    def __scan_nodes_from_config(self):
        try:
            if self.__interest_nodes:
                for device_object in self.__interest_nodes:
                    for current_device in device_object:
                        try:
                            device_configuration = device_object[current_device]
                            device_info = self.__search_general_info(device_configuration)
                            if device_info is not None and device_info.get("deviceNode") is not None:
                                self.__search_nodes_and_subscribe(device_configuration, device_info)
                                self.__save_methods(device_info, device_configuration)
                                self.__search_attribute_update_variables(device_configuration, device_info)
                            else:
                                log.error("Device node is None, please check your configuration.")
                                log.debug("Current device node is: %s", str(device_configuration.get("deviceNodePattern")))
                                break
                        except Exception as e:
                            log.exception(e)
                log.debug(self.__interest_nodes)
        except Exception as e:
            log.exception(e)

    def __search_nodes_and_subscribe(self, device_configuration, device_info):
        device_configuration.update(**device_info)
        information_types = {"attributes": "attributes", "timeseries": "telemetry"}
        for information_type in information_types:
            for information in device_configuration[information_type]:
                information_key = information["key"]
                config_path = TBUtility.get_value(information["path"], get_tag=True)
                information_path = self.__check_path(config_path, device_info["deviceNode"])
                information["path"] = '${%s}' % information_path
                information_node = self.__search_node(device_info["deviceNode"], information_path)
                if information_node is not None:
                    information_value = information_node.get_value()
                    self.__sub.subscribe_data_change(information_node)
                    log.debug("Node for %s \"%s\" with path: %s - FOUND! Current values is: %s",
                              information_type,
                              information_key,
                              information_path,
                              str(information_value))
                    if device_configuration.get("uplink_converter") is None:
                        if device_configuration.get('converter') is None:
                            converter = OpcUaUplinkConverter(device_configuration)
                        else:
                            converter = TBUtility.check_and_import(self.__connector_type, device_configuration['converter'])
                        device_configuration["uplink_converter"] = converter
                    else:
                        converter = device_configuration["uplink_converter"]
                    self.subscribed[information_node] = {"converter": converter,
                                                         "path": information_path}
                    if not device_info.get(information_types[information_type]):
                        device_info[information_types[information_type]] = []
                    converted_data = converter.convert(information_path, information_value)
                    self.statistics['MessagesReceived'] += 1
                    self.data_to_send.append(converted_data)
                    self.statistics['MessagesSent'] += 1
                    log.debug("Data to ThingsBoard: %s", converted_data)
                else:
                    log.error("Node for %s \"%s\" with path %s - NOT FOUND!", information_type, information_key, information_path)
        device_configuration.update(**device_info)

    def __save_methods(self, device, configuration):
        try:
            if self.__available_object_resources.get(device["deviceName"]) is None:
                self.__available_object_resources[device["deviceName"]] = {}
            if self.__available_object_resources[device["deviceName"]].get("methods") is None:
                self.__available_object_resources[device["deviceName"]]["methods"] = []
            if configuration.get("rpc_methods"):
                node = device["deviceNode"]
                for method_object in configuration["rpc_methods"]:
                    method_node_path = self.__check_path(method_object["method"], node)
                    method = self.__search_node(node, method_node_path)
                    if method is not None:
                        node_method_name = method.get_display_name().Text
                        self.__available_object_resources[device["deviceName"]]["methods"].append({node_method_name: method, "node": node, "arguments": method_object.get("arguments")})
                    else:
                        log.error("Node for method with path %s - NOT FOUND!", method_node_path)
        except Exception as e:
            log.exception(e)

    def __search_attribute_update_variables(self, device_configuration, device_info):
        try:
            if device_configuration.get("attributes_updates"):
                node = device_info["deviceNode"]
                device_name = device_info["deviceName"]
                if self.__available_object_resources.get(device_name) is None:
                    self.__available_object_resources[device_name] = {}
                if self.__available_object_resources[device_name].get("variables") is None:
                    self.__available_object_resources[device_name]["variables"] = []
                for attribute_update in device_configuration["attributes_updates"]:
                    attribute_path = self.__check_path(attribute_update["attributeOnDevice"], node)
                    attribute_node = self.__search_node(node, attribute_path)
                    if attribute_node is not None:
                        self.__available_object_resources[device_name]["variables"].append({attribute_update["attributeOnThingsBoard"]: attribute_node})
                    else:
                        log.error("Attribute update node with path \"%s\" - NOT FOUND!", attribute_path)
        except Exception as e:
            log.exception(e)

    def __search_general_info(self, device):
        result = {"deviceName": None, "deviceType": None, "deviceNode": None}
        result["deviceNode"] = self.__search_node(self.__opcua_nodes["root"], TBUtility.get_value(device["deviceNodePattern"], get_tag=True))
        if result["deviceNode"] is not None:
            name_pattern_config = device["deviceNamePattern"]
            name_expression = TBUtility.get_value(name_pattern_config, get_tag=True)
            device_name_node = self.__search_node(self.__opcua_nodes["root"], name_expression)
            if device_name_node is not None:
                device_name_from_node = device_name_node.get_value()
                full_device_name = name_pattern_config.replace("${" + name_expression + "}", device_name_from_node).replace(
                    name_expression, device_name_from_node)
                result["deviceName"] = full_device_name
                log.debug("Device name: %s", full_device_name)
                if device.get("deviceTypePattern"):
                    device_type_expression = TBUtility.get_value(device["deviceTypePattern"],
                                                                 get_tag=True)
                    device_type_node = self.__search_node(self.__opcua_nodes["root"], device_type_expression)
                    if device_type_node is not None:
                        device_type = device_type_node.get_value()
                        full_device_type = device_type_expression.replace("${" + device_type_expression + "}",
                                                                          device_type).replace(device_type_expression,
                                                                                               device_type)
                        result["deviceType"] = full_device_type
                        log.debug("Device type: %s", full_device_type)
                    else:
                        log.error("Device type node not found with expression: %s", device_type_expression)
                else:
                    result["deviceType"] = "default"
                return result
            else:
                log.error("Device name node not found with expression: %s", name_expression)
                return
        else:
            log.error("Device node not found with expression: %s", TBUtility.get_value(device["deviceNodePattern"], get_tag=True))

    def __search_node(self, current_node, fullpath):
        try:
            result = None
            for child_node in current_node.get_children():
                new_node = self.client.get_node(child_node)
                new_node_path = '\\\\.'.join(char.split(":")[1] for char in new_node.get_path(200000, True))
                new_node_class = new_node.get_node_class()
                regex_fullmatch = re.fullmatch(new_node_path.replace('\\\\', '\\'), fullpath) or new_node_path.replace('\\\\', '\\') == fullpath
                if regex_fullmatch:
                    if self.__show_map:
                        log.debug("SHOW MAP: Current node path: %s - NODE FOUND", new_node_path.replace('\\\\', '\\'))
                    return new_node
                regex_search = re.search(new_node_path, fullpath.replace('\\\\', '\\'))
                if regex_search:
                    if self.__show_map:
                        log.debug("SHOW MAP: Current node path: %s - NODE FOUND", new_node_path)
                    if new_node_class == ua.NodeClass.Object:
                        log.debug("Search in %s", new_node_path)
                        result = self.__search_node(new_node, fullpath)
                    elif new_node_class == ua.NodeClass.Variable:
                        log.debug("Found in %s", new_node_path)
                        result = new_node
                    elif new_node_class == ua.NodeClass.Method:
                        log.debug("Found in %s", new_node_path)
                        result = new_node
            return result
        except Exception as e:
            log.exception(e)

    def __check_path(self, config_path, node):
        if re.search("^root", config_path.lower()) is None:
            node_path = '\\\\.'.join(
                char.split(":")[1] for char in node.get_path(200000, True))
            if config_path[-3:] != '\\.':
                information_path = node_path + '\\\\.' + config_path.replace('\\', '\\\\')
            else:
                information_path = node_path + config_path.replace('\\', '\\\\')
        else:
            information_path = config_path
        return information_path

    @property
    def subscribed(self):
        return self._subscribed


class SubHandler(object):
    def __init__(self, connector: OpcUaConnector):
        self.connector = connector

    def datachange_notification(self, node, val, data):
        try:
            log.debug("Python: New data change event on node %s, with val: %s", node, val)
            subscription = self.connector.subscribed[node]
            converted_data = subscription["converter"].convert(subscription["path"], val)
            self.connector.statistics['MessagesReceived'] += 1
            self.connector.data_to_send.append(converted_data)
            self.connector.statistics['MessagesSent'] += 1
            log.debug("Data to ThingsBoard: %s", converted_data)
        except Exception as e:
            log.exception(e)

    def event_notification(self, event):
        try:
            log.debug("Python: New event %s", event)
        except Exception as e:
            log.exception(e)
