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

        self.setName(self.__server_conf.get("name", 'OPC-UA Default ' + ''.join(choice(ascii_lowercase) for _ in range(5))) + " Connector")
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
        sub = self.client.create_subscription(self.__server_conf.get("scanPeriodInMillis", 500), self.__sub_handler)
        self.__search_name(self.__opcua_nodes["objects"], 2)
        self.__search_tags(self.__opcua_nodes["objects"], 2, sub)
        log.debug('Subscriptions: %s', self.subscribed)

        log.debug("Available methods: %s", self.__available_object_resources)
        while True:
            try:
                time.sleep(1)
                if self.data_to_send:
                    self.__gateway.send_to_storage(self.get_name(), self.data_to_send.pop())
                if self.__stopped:
                    break
            except (KeyboardInterrupt, SystemExit):
                self.close()
                raise
            except Exception as e:
                self.close()
                log.exception(e)

    def close(self):
        self.__stopped = True
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
                    arguments = content["data"].get("params")
                    if type(arguments) is list:
                        result = method["node"].call_method(method[rpc_method], *arguments)
                    elif arguments is not None:
                        result = method["node"].call_method(method[rpc_method], arguments)
                    else:
                        result = method["node"].call_method(method[rpc_method])

                    self.__gateway.send_rpc_reply(content["device"],
                                                  content["data"]["id"],
                                                  {content["data"]["method"]: result})

                    log.debug("method %s result is: %s", method[rpc_method], result)
        except Exception as e:
            log.exception(e)

    def __search_name(self, node, recursion_level):
        try:
            for childId in node.get_children():
                ch = self.client.get_node(childId)
                current_var_path = '.'.join(x.split(":")[1] for x in ch.get_path(20000, True))
                if self.__interest_nodes:
                    if ch.get_node_class() == ua.NodeClass.Object:
                        for interest_node in self.__interest_nodes:
                            for int_node in interest_node:
                                subrecursion_level = recursion_level
                                if subrecursion_level != recursion_level + len(interest_node[int_node]["deviceNamePattern"].split("\\.")):
                                    if ch.get_display_name().Text in TBUtility.get_value(interest_node[int_node]["deviceNamePattern"], get_tag=True).split('.') or \
                                            re.search(TBUtility.get_value(interest_node[int_node]["deviceNodePattern"], get_tag=True), ch.get_display_name().Text):
                                        self.__search_name(ch, subrecursion_level+1)
                                else:
                                    return
                    elif ch.get_node_class() == ua.NodeClass.Variable:
                        try:
                            for interest_node in self.__interest_nodes:
                                for int_node in interest_node:
                                    if interest_node[int_node].get("deviceName") is None:
                                        try:
                                            name_pattern = TBUtility.get_value(interest_node[int_node]["deviceNamePattern"],
                                                                               get_tag=True)
                                            log.debug(current_var_path)
                                            device_name_node = re.search(name_pattern.split('\\.')[-1], current_var_path)
                                            if device_name_node is not None:
                                                device_name = ch.get_value()
                                                if "${" + name_pattern + "}" in interest_node[int_node]["deviceNamePattern"]:
                                                    full_device_name = interest_node[int_node]["deviceNamePattern"].replace("${"+name_pattern+"}", device_name)
                                                elif device_name in interest_node[int_node]["deviceNamePattern"]:
                                                    full_device_name = interest_node[int_node]["deviceNamePattern"].replace(name_pattern, device_name)
                                                else:
                                                    log.error("Name pattern not found.")
                                                    break
                                                interest_node[int_node]["deviceName"] = full_device_name
                                                if self.__available_object_resources.get(full_device_name) is None:
                                                    self.__available_object_resources[full_device_name] = {'methods': [],
                                                                                                           'variables': []}
                                                if not self.__gateway.get_devices().get(full_device_name):
                                                    self.__gateway.add_device(full_device_name, {"connector": None})
                                                self.__gateway.update_device(full_device_name, "connector", self)
                                            else:
                                                try:
                                                    if re.search(int_node.split('\\.')[recursion_level-2], ch.get_display_name().Text):
                                                        self.__search_name(ch, recursion_level+1)
                                                except IndexError:
                                                    if re.search(int_node.split('\\.')[-1], ch.get_display_name().Text):
                                                        self.__search_name(ch, recursion_level+1)

                                        except Exception as e:
                                            log.exception(e)
                                    else:
                                        break
                        except BadWaitingForInitialData:
                            pass
                elif not self.__interest_nodes:
                    log.error("Nodes in mapping not found, check your settings.")
        except Exception as e:
            log.exception(e)

    def __search_tags(self, node, recursion_level, sub=None):
        try:
            for childId in node.get_children():
                ch = self.client.get_node(childId)
                current_var_path = '.'.join(x.split(":")[1] for x in ch.get_path(20000, True))
                if self.__interest_nodes:
                    if ch.get_node_class() == ua.NodeClass.Object:
                        for interest_node in self.__interest_nodes:
                            for int_node in interest_node:
                                try:
                                    name_to_check = int_node.split('\\.')[recursion_level-1] if '\\.' in int_node else int_node
                                    name_to_check = int_node.split('.')[recursion_level-1] if '.' in int_node else name_to_check
                                except IndexError:
                                    name_to_check = int_node.split('\\.')[-1] if '\\.' in int_node else int_node
                                    name_to_check = int_node.split('.')[-1] if '.' in int_node else name_to_check
                                if re.search(name_to_check, ch.get_display_name().Text):
                                    try:
                                        methods = ch.get_methods()
                                        for method in methods:
                                            self.__available_object_resources[interest_node[int_node]["deviceName"]]["methods"].append({method.get_display_name().Text: method,
                                                                                                                                       "node": ch})
                                    except Exception as e:
                                        log.exception(e)
                                for tag in interest_node[int_node]["timeseries"] + interest_node[int_node]["attributes"]:
                                    subrecursion_level = recursion_level
                                    if subrecursion_level != recursion_level + len(tag["path"].split("\\.")):
                                        self.__search_tags(ch, subrecursion_level+1, sub)
                                    else:
                                        return
                                self.__search_tags(ch, recursion_level+1, sub)
                    elif ch.get_node_class() == ua.NodeClass.Variable:
                        try:
                            for interest_node in self.__interest_nodes:
                                for int_node in interest_node:
                                    if interest_node[int_node].get("attributes_updates"):
                                        try:
                                            for attribute_update in interest_node[int_node]["attributes_updates"]:
                                                if attribute_update["attributeOnDevice"] == ch.get_display_name().Text:
                                                    self.__available_object_resources[interest_node[int_node]["deviceName"]]['variables'].append({attribute_update["attributeOnThingsBoard"]: ch, })
                                        except Exception as e:
                                            log.exception(e)
                                    name_to_check = int_node.split('\\.')[-1] if '\\.' in int_node else int_node
                                    name_to_check = int_node.split('.')[-1] if '.' in int_node else name_to_check
                                    if re.search(name_to_check.replace('$', ''), current_var_path):
                                        tags = []
                                        if interest_node[int_node].get("attributes"):
                                            tags.extend(interest_node[int_node]["attributes"])
                                        if interest_node[int_node].get("timeseries"):
                                            tags.extend(interest_node[int_node]["timeseries"])
                                        for tag in tags:
                                            target = TBUtility.get_value(tag["path"], get_tag=True)
                                            try:
                                                tag_name_for_check = target.split('\\.')[recursion_level-1] if '\\.' in target else target
                                                tag_name_for_check = target.split('.')[recursion_level-1] if '.' in target else tag_name_for_check
                                            except IndexError:
                                                tag_name_for_check = target.split('\\.')[-1] if '\\.' in target else target
                                                tag_name_for_check = target.split('.')[-1] if '.' in target else tag_name_for_check
                                            current_node_name = ch.get_display_name().Text
                                            if current_node_name == tag_name_for_check:
                                                sub.subscribe_data_change(ch)
                                                if interest_node[int_node].get("uplink_converter") is None:
                                                    if interest_node[int_node].get('converter') is None:
                                                        converter = OpcUaUplinkConverter(interest_node[int_node])
                                                    else:
                                                        converter = TBUtility.check_and_import(self.__connector_type, interest_node[int_node]['converter'])
                                                    interest_node[int_node]["uplink_converter"] = converter
                                                else:
                                                    converter = interest_node[int_node]["uplink_converter"]
                                                self.subscribed[ch] = {"converter": converter,
                                                                       "path": current_var_path}
                                    else:
                                        return
                        except BadWaitingForInitialData:
                            pass
                    elif not self.__interest_nodes:
                        log.error("Nodes in mapping not found, check your settings.")
        except Exception as e:
            log.exception(e)

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
