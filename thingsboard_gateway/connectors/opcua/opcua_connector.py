#     Copyright 2021. ThingsBoard
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
import time
from concurrent.futures import CancelledError, TimeoutError as FuturesTimeoutError
from copy import deepcopy
from random import choice
from threading import Thread
from string import ascii_lowercase
import regex
from simplejson import dumps

from thingsboard_gateway.tb_utility.tb_loader import TBModuleLoader
from thingsboard_gateway.tb_utility.tb_utility import TBUtility

try:
    from opcua import Client, Node, ua
except ImportError:
    print("OPC-UA library not found")
    TBUtility.install_package("opcua")
    from opcua import Client, ua
from thingsboard_gateway.connectors.connector import Connector, log
from thingsboard_gateway.connectors.opcua.opcua_uplink_converter import OpcUaUplinkConverter


class OpcUaConnector(Thread, Connector):
    def __init__(self, gateway, config, connector_type):
        self._connector_type = connector_type
        self.statistics = {'MessagesReceived': 0,
                           'MessagesSent': 0}
        super().__init__()
        self.__gateway = gateway
        self.__server_conf = config.get("server")
        self.__interest_nodes = []
        self.__available_object_resources = {}
        self.__show_map = self.__server_conf.get("showMap", False)
        self.__previous_scan_time = 0
        for mapping in self.__server_conf["mapping"]:
            if mapping.get("deviceNodePattern") is not None:
                self.__interest_nodes.append({mapping["deviceNodePattern"]: mapping})
            else:
                log.error("deviceNodePattern in mapping: %s - not found, add property deviceNodePattern to processing this mapping",
                          dumps(mapping))
        if "opc.tcp" not in self.__server_conf.get("url"):
            self.__opcua_url = "opc.tcp://"+self.__server_conf.get("url")
        else:
            self.__opcua_url = self.__server_conf.get("url")
        self.client = Client(self.__opcua_url, timeout=self.__server_conf.get("timeoutInMillis", 4000)/1000)
        if self.__server_conf["identity"].get("type") == "cert.PEM":
            try:
                ca_cert = self.__server_conf["identity"].get("caCert")
                private_key = self.__server_conf["identity"].get("privateKey")
                cert = self.__server_conf["identity"].get("cert")
                security_mode = self.__server_conf["identity"].get("mode", "SignAndEncrypt")
                policy = self.__server_conf["security"]
                if cert is None or private_key is None:
                    log.exception("Error in ssl configuration - cert or privateKey parameter not found")
                    raise RuntimeError("Error in ssl configuration - cert or privateKey parameter not found")
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
        self.__sub = None
        self.__sub_handler = SubHandler(self)
        self.data_to_send = []
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
                self.client.connect()
                try:
                    self.client.load_type_definitions()
                except Exception as e:
                    log.debug(e)
                    log.debug("Error on loading type definitions.")
                log.debug(self.client.get_namespace_array()[-1])
                log.debug(self.client.get_namespace_index(self.client.get_namespace_array()[-1]))
            except ConnectionRefusedError:
                log.error("Connection refused on connection to OPC-UA server with url %s", self.__server_conf.get("url"))
                time.sleep(10)
            except OSError:
                log.error("Connection refused on connection to OPC-UA server with url %s", self.__server_conf.get("url"))
                time.sleep(10)
            except Exception as e:
                log.debug("error on connection to OPC-UA server.")
                log.error(e)
                time.sleep(10)
            else:
                self.__connected = True
                log.info("OPC-UA connector %s connected to server %s", self.get_name(), self.__server_conf.get("url"))
        self.__initialize_client()
        while not self.__stopped:
            try:
                time.sleep(.1)
                self.__check_connection()
                if not self.__connected and not self.__stopped:
                    self.client.connect()
                    self.__initialize_client()
                    log.info("Reconnected to the OPC-UA server - %s", self.__server_conf.get("url"))
                elif not self.__stopped:
                    if self.__server_conf.get("disableSubscriptions", False) and time.time()*1000 - self.__previous_scan_time > self.__server_conf.get("scanPeriodInMillis", 60000):
                        self.scan_nodes_from_config()
                        self.__previous_scan_time = time.time() * 1000
                    # giusguerrini, 2020-09-24: Fix: flush event set and send all data to platform,
                    # so data_to_send doesn't grow indefinitely in case of more than one value change
                    # per cycle, and platform doesn't lose events.
                    # NOTE: possible performance improvement: use a map to store only one event per
                    # variable to reduce frequency of messages to platform.
                    while self.data_to_send:
                        self.__gateway.send_to_storage(self.get_name(), self.data_to_send.pop())
                if self.__stopped:
                    self.close()
                    break
            except (KeyboardInterrupt, SystemExit):
                self.close()
                raise
            except FuturesTimeoutError:
                self.__check_connection()
            except Exception as e:
                log.error("Connection failed on connection to OPC-UA server with url %s",
                          self.__server_conf.get("url"))
                log.exception(e)
                self.client = Client(self.__opcua_url, timeout=self.__server_conf.get("timeoutInMillis", 4000) / 1000)
                self._subscribed = {}
                self.__available_object_resources = {}
                time.sleep(10)

    def __check_connection(self):
        try:
            node = self.client.get_root_node()
            node.get_children()
            if not self.__server_conf.get("disableSubscriptions", False) and (not self.__connected or not self.subscribed):
                self.__sub = self.client.create_subscription(self.__server_conf.get("subCheckPeriodInMillis", 500), self.__sub_handler)
            self.__connected = True
        except ConnectionRefusedError:
            self.__connected = False
            self._subscribed = {}
            self.__available_object_resources = {}
            self.__sub = None
        except OSError:
            self.__connected = False
            self._subscribed = {}
            self.__available_object_resources = {}
            self.__sub = None
        except FuturesTimeoutError:
            self.__connected = False
            self._subscribed = {}
            self.__available_object_resources = {}
            self.__sub = None
        except AttributeError:
            self.__connected = False
            self._subscribed = {}
            self.__available_object_resources = {}
            self.__sub = None
        except Exception as e:
            self.__connected = False
            self._subscribed = {}
            self.__available_object_resources = {}
            self.__sub = None
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
                            try:
                                server_variables[variable].set_value(content["data"][variable])
                            except Exception:
                                server_variables[variable].set_attribute(ua.AttributeIds.Value, ua.DataValue(content["data"][variable]))
        except Exception as e:
            log.exception(e)

    def server_side_rpc_handler(self, content):
        try:
            rpc_method = content["data"].get("method")
            for method in self.__available_object_resources[content["device"]]['methods']:
                if rpc_method is not None and method.get(rpc_method) is not None:
                    arguments_from_config = method["arguments"]
                    arguments = content["data"].get("params") if content["data"].get("params") is not None else arguments_from_config
                    try:
                        if isinstance(arguments, list):
                            result = method["node"].call_method(method[rpc_method], *arguments)
                        elif arguments is not None:
                            try:
                                result = method["node"].call_method(method[rpc_method], arguments)
                            except ua.UaStatusCodeError as e:
                                if "BadTypeMismatch" in str(e) and isinstance(arguments, int):
                                    result = method["node"].call_method(method[rpc_method], float(arguments))
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

    def __initialize_client(self):
        self.__opcua_nodes["root"] = self.client.get_objects_node()
        self.__opcua_nodes["objects"] = self.client.get_objects_node()
        self.scan_nodes_from_config()
        self.__previous_scan_time = time.time() * 1000
        log.debug('Subscriptions: %s', self.subscribed)
        log.debug("Available methods: %s", self.__available_object_resources)

    def scan_nodes_from_config(self):
        try:
            if self.__interest_nodes:
                for device_object in self.__interest_nodes:
                    for current_device in device_object:
                        try:
                            device_configuration = device_object[current_device]
                            devices_info_array = self.__search_general_info(device_configuration)
                            for device_info in devices_info_array:
                                if device_info is not None and device_info.get("deviceNode") is not None:
                                    self.__search_nodes_and_subscribe(device_info)
                                    self.__save_methods(device_info)
                                    self.__search_attribute_update_variables(device_info)
                                else:
                                    log.error("Device node is None, please check your configuration.")
                                    log.debug("Current device node is: %s", str(device_configuration.get("deviceNodePattern")))
                                    break
                        except BrokenPipeError:
                            log.debug("Broken Pipe. Connection lost.")
                        except OSError:
                            log.debug("Stop on scanning.")
                        except FuturesTimeoutError:
                            self.__check_connection()
                        except Exception as e:
                            log.exception(e)
                log.debug(self.__interest_nodes)
        except Exception as e:
            log.exception(e)

    def __search_nodes_and_subscribe(self, device_info):
        sub_nodes = []
        information_types = {"attributes": "attributes", "timeseries": "telemetry"}
        for information_type in information_types:
            for information in device_info["configuration"][information_type]:
                information_key = information["key"]
                config_path = TBUtility.get_value(information["path"], get_tag=True)
                information_path = self._check_path(config_path, device_info["deviceNode"])
                information["path"] = '${%s}' % information_path
                information_nodes = []
                self.__search_node(device_info["deviceNode"], information_path, result=information_nodes)
                for information_node in information_nodes:
                    if information_node is not None:
                        try:
                            information_value = information_node.get_value()
                        except:
                            log.error("Err get_value: %s", str(information_node))
                            continue
                        log.debug("Node for %s \"%s\" with path: %s - FOUND! Current values is: %s",
                                  information_type,
                                  information_key,
                                  information_path,
                                  str(information_value))
                        if device_info.get("uplink_converter") is None:
                            configuration = {**device_info["configuration"],
                                             "deviceName": device_info["deviceName"],
                                             "deviceType": device_info["deviceType"]}
                            if device_info["configuration"].get('converter') is None:
                                converter = OpcUaUplinkConverter(configuration)
                            else:
                                converter = TBModuleLoader.import_module(self._connector_type, configuration)
                            device_info["uplink_converter"] = converter
                        else:
                            converter = device_info["uplink_converter"]
                        self.subscribed[information_node] = {"converter": converter,
                                                             "path": information_path,
                                                             "config_path": config_path}
                        if not device_info.get(information_types[information_type]):
                            device_info[information_types[information_type]] = []
                        converted_data = converter.convert((config_path, information_path), information_value)
                        self.statistics['MessagesReceived'] = self.statistics['MessagesReceived'] + 1
                        self.data_to_send.append(converted_data)
                        self.statistics['MessagesSent'] = self.statistics['MessagesSent'] + 1
                        log.debug("Data to ThingsBoard: %s", converted_data)
                        if not self.__server_conf.get("disableSubscriptions", False):
                            sub_nodes.append(information_node)
                    else:
                        log.error("Node for %s \"%s\" with path %s - NOT FOUND!", information_type, information_key, information_path)
        if not self.__server_conf.get("disableSubscriptions", False):
            if self.__sub is None:
                self.__sub = self.client.create_subscription(self.__server_conf.get("subCheckPeriodInMillis", 500),
                                                             self.__sub_handler)
            if sub_nodes:
                self.__sub.subscribe_data_change(sub_nodes)
                log.debug("Added subscription to nodes: %s", str(sub_nodes))

    def __save_methods(self, device_info):
        try:
            if self.__available_object_resources.get(device_info["deviceName"]) is None:
                self.__available_object_resources[device_info["deviceName"]] = {}
            if self.__available_object_resources[device_info["deviceName"]].get("methods") is None:
                self.__available_object_resources[device_info["deviceName"]]["methods"] = []
            if device_info["configuration"].get("rpc_methods", []):
                node = device_info["deviceNode"]
                for method_object in device_info["configuration"]["rpc_methods"]:
                    method_node_path = self._check_path(method_object["method"], node)
                    methods = []
                    self.__search_node(node, method_node_path, True, result=methods)
                    for method in methods:
                        if method is not None:
                            node_method_name = method.get_display_name().Text
                            self.__available_object_resources[device_info["deviceName"]]["methods"].append({node_method_name: method, "node": node, "arguments": method_object.get("arguments")})
                        else:
                            log.error("Node for method with path %s - NOT FOUND!", method_node_path)
        except Exception as e:
            log.exception(e)

    def __search_attribute_update_variables(self, device_info):
        try:
            if device_info["configuration"].get("attributes_updates", []):
                node = device_info["deviceNode"]
                device_name = device_info["deviceName"]
                if self.__available_object_resources.get(device_name) is None:
                    self.__available_object_resources[device_name] = {}
                if self.__available_object_resources[device_name].get("variables") is None:
                    self.__available_object_resources[device_name]["variables"] = []
                for attribute_update in device_info["configuration"]["attributes_updates"]:
                    attribute_path = self._check_path(attribute_update["attributeOnDevice"], node)
                    attribute_nodes = []
                    self.__search_node(node, attribute_path, result=attribute_nodes)
                    for attribute_node in attribute_nodes:
                        if attribute_node is not None:
                            self.__available_object_resources[device_name]["variables"].append({attribute_update["attributeOnThingsBoard"]: attribute_node})
                        else:
                            log.error("Attribute update node with path \"%s\" - NOT FOUND!", attribute_path)
        except Exception as e:
            log.exception(e)

    def __search_general_info(self, device):
        result = []
        match_devices = []
        self.__search_node(self.__opcua_nodes["root"], TBUtility.get_value(device["deviceNodePattern"], get_tag=True), result=match_devices)
        for device_node in match_devices:
            if device_node is not None:
                result_device_dict = {"deviceName": None, "deviceType": None, "deviceNode": device_node, "configuration": deepcopy(device)}
                name_pattern_config = device["deviceNamePattern"]
                name_expression = TBUtility.get_value(name_pattern_config, get_tag=True)
                if "${" in name_pattern_config and "}" in name_pattern_config:
                    log.debug("Looking for device name")
                    device_name_from_node=""
                    if name_expression == "$DisplayName":
                        device_name_from_node = device_node.get_display_name().Text
                    elif name_expression == "$BrowseName":
                        device_name_from_node = device_node.get_browse_name().Name
                    elif name_expression == "$NodeId.Identifier":
                        device_name_from_node = str(device_node.nodeid.Identifier)
                    else:
                        name_path = self._check_path(name_expression, device_node)
                        device_name_node = []
                        self.__search_node(device_node, name_path, result=device_name_node)
                        if len(device_name_node) == 0:
                            log.warn("Device name node - not found, skipping device...")
                            continue
                        device_name_node = device_name_node[0]
                        if device_name_node is not None:
                            device_name_from_node = device_name_node.get_value()
                    if device_name_from_node == "":
                        log.error("Device name node not found with expression: %s", name_expression)
                        return None
                    full_device_name = name_pattern_config.replace("${" + name_expression + "}", str(device_name_from_node)).replace(
                        name_expression, str(device_name_from_node))
                else:
                    full_device_name = name_expression
                result_device_dict["deviceName"] = full_device_name
                log.debug("Device name: %s", full_device_name)
                if device.get("deviceTypePattern"):
                    device_type_expression = TBUtility.get_value(device["deviceTypePattern"],
                                                                 get_tag=True)
                    if "${" in device_type_expression and "}" in device_type_expression:
                        type_path = self._check_path(device_type_expression, device_node)
                        device_type_node = []
                        self.__search_node(device_node, type_path, result=device_type_node)
                        device_type_node = device_type_node[0]
                        if device_type_node is not None:
                            device_type = device_type_node.get_value()
                            full_device_type = device_type_expression.replace("${" + device_type_expression + "}",
                                                                              device_type).replace(device_type_expression,
                                                                                                   device_type)
                        else:
                            log.error("Device type node not found with expression: %s", device_type_expression)
                            full_device_type = "default"
                    else:
                        full_device_type = device_type_expression
                    result_device_dict["deviceType"] = full_device_type
                    log.debug("Device type: %s", full_device_type)
                else:
                    result_device_dict["deviceType"] = "default"
                result.append(result_device_dict)
            else:
                log.error("Device node not found with expression: %s", TBUtility.get_value(device["deviceNodePattern"], get_tag=True))
        return result

    def get_node_path(self, node: Node):
        return '\\.'.join(node.get_browse_name().Name for node in node.get_path(200000))

    def __search_node(self, current_node, fullpath, search_method=False, result=None):
        if result is None:
            result = []
        try:
            if regex.match(r"ns=\d*;[isgb]=.*", fullpath, regex.IGNORECASE):
                if self.__show_map:
                    log.debug("Looking for node with config")
                node = self.client.get_node(fullpath)
                if node is None:
                    log.warning("NODE NOT FOUND - using configuration %s", fullpath)
                else:
                    log.debug("Found in %s", node)
                    result.append(node)
            else:
                fullpath_pattern = regex.compile(fullpath)
                full1 = fullpath.replace('\\\\.', '.')
                #current_node_path = '\\.'.join(char.split(":")[1] for char in current_node.get_path(200000, True))
                current_node_path = self.get_node_path(current_node)
                # we are allways the parent
                child_node_parent_class = current_node.get_node_class()
                new_parent = current_node
                for child_node in current_node.get_children():
                    new_node_class = child_node.get_node_class()
                    # this will not change you can do it outside th loop
                    # basis Description of node.get_parent() function, sometime child_node.get_parent() return None
                    #new_parent = child_node.get_parent()
                    #if (new_parent is None):
                    #    child_node_parent_class = current_node.get_node_class()
                    #else:
                    #    child_node_parent_class = child_node.get_parent().get_node_class()
                    #current_node_path = '\\.'.join(char.split(":")[1] for char in current_node.get_path(200000, True))
                    #new_node_path = '\\\\.'.join(char.split(":")[1] for char in child_node.get_path(200000, True))
                    new_node_path = self.get_node_path(child_node)
                    if child_node_parent_class == ua.NodeClass.View and new_parent is not None:
                        parent_path = self.get_node_path(new_parent)
                        #parent_path = '\\.'.join(char.split(":")[1] for char in new_parent.get_path(200000, True))
                        fullpath = fullpath.replace(current_node_path, parent_path)
                    nnp1 = new_node_path.replace('\\\\.', '.')
                    nnp2 = new_node_path.replace('\\\\', '\\')
                    if self.__show_map:
                        log.debug("SHOW MAP: Current node path: %s", new_node_path)
                    regex_fullmatch = regex.fullmatch(fullpath_pattern,  nnp1) or \
                                      nnp2 == full1 or \
                                      nnp2 == fullpath or \
                                      nnp1 == full1
                    if regex_fullmatch:
                        if self.__show_map:
                            log.debug("SHOW MAP: Current node path: %s - NODE FOUND", nnp2)
                        result.append(child_node)
                    else:
                        regex_search = fullpath_pattern.fullmatch(nnp1, partial=True) or \
                                          nnp2 in full1 or \
                                          nnp1 in full1
                        if regex_search:
                            if self.__show_map:
                                log.debug("SHOW MAP: Current node path: %s - NODE FOUND", new_node_path)
                            if new_node_class == ua.NodeClass.Object:
                                if self.__show_map:
                                    log.debug("SHOW MAP: Search in %s", new_node_path)
                                self.__search_node(child_node, fullpath, result=result)
                            elif new_node_class == ua.NodeClass.Variable:
                                log.debug("Found in %s", new_node_path)
                                result.append(child_node)
                            elif new_node_class == ua.NodeClass.Method and search_method:
                                log.debug("Found in %s", new_node_path)
                                result.append(child_node)
        except CancelledError:
            log.error("Request during search has been canceled by the OPC-UA server.")
        except BrokenPipeError:
            log.error("Broken Pipe. Connection lost.")
        except OSError:
            log.debug("Stop on scanning.")
        except Exception as e:
            log.exception(e)

    def _check_path(self, config_path, node):
        if regex.match(r"ns=\d*;[isgb]=.*", config_path, regex.IGNORECASE):
            return config_path
        if re.search(r"^root", config_path.lower()) is None:
            node_path = self.get_node_path(node)
            #node_path = '\\\\.'.join(char.split(":")[1] for char in node.get_path(200000, True))
            if config_path[-3:] != '\\.':
                information_path = node_path + '\\\\.' + config_path.replace('\\', '\\\\')
            else:
                information_path = node_path + config_path.replace('\\', '\\\\')
        else:
            information_path = config_path
        result = information_path[:]
        return result

    @property
    def subscribed(self):
        return self._subscribed


class SubHandler(object):
    def __init__(self, connector: OpcUaConnector):
        self.connector = connector

    def datachange_notification(self, node, val, data):
        try:
            log.debug("Python: New data change event on node %s, with val: %s and data %s", node, val, str(data))
            subscription = self.connector.subscribed[node]
            converted_data = subscription["converter"].convert((subscription["config_path"], subscription["path"]), val)
            self.connector.statistics['MessagesReceived'] = self.connector.statistics['MessagesReceived'] + 1
            self.connector.data_to_send.append(converted_data)
            self.connector.statistics['MessagesSent'] = self.connector.statistics['MessagesSent'] + 1
            log.debug("[SUBSCRIPTION] Data to ThingsBoard: %s", converted_data)
        except KeyError:
            self.connector.scan_nodes_from_config()
        except Exception as e:
            log.exception(e)

    def event_notification(self, event):
        try:
            log.debug("Python: New event %s", event)
        except Exception as e:
            log.exception(e)
