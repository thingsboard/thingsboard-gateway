import re
from json import dumps
import time
from threading import Thread
from random import choice
from string import ascii_lowercase
from opcua import Client, ua
from tb_utility.tb_utility import TBUtility
from opcua.ua.uaerrors._auto import BadWaitingForInitialData
from connectors.connector import Connector, log
from connectors.opcua.opcua_uplink_converter import OpcUaUplinkConverter


class OpcUaConnector(Thread, Connector):
    def __init__(self, gateway, config):
        Thread.__init__(self)
        self.__gateway = gateway
        self.__server_conf = config.get("server")
        self.__interest_nodes = []
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
        self.setName(self.__server_conf.get("name", 'OPC-UA Default ' + ''.join(choice(ascii_lowercase) for _ in range(5))) + " Connector")
        self.__opcua_nodes = {}
        self._subscribed = {}
        self.data_to_send = []
        self.__sub_handler = SubHandler(self)
        self.__stopped = False
        self.__connected = False
        self.daemon = True

    def open(self):
        self.__stopped = False
        self.start()
        log.info("Starting OPC-UA Connector...")

    def run(self):
        while not self.__connected:
            try:
                self.__connected = self.client.connect()
                self.client.load_type_definitions()
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
        log.debug(self.subscribed)
        while True:
            try:
                time.sleep(1)
                if self.data_to_send:
                    self.__gateway._send_to_storage(self.get_name(), self.data_to_send.pop())
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
        pass

    def server_side_rpc_handler(self, content):
        pass

    def __search_name(self, node, recursion_level):
        try:
            for childId in node.get_children():
                ch = self.client.get_node(childId)
                current_var_path = '.'.join(x.split(":")[1] for x in ch.get_path(20000, True))
                if self.__interest_nodes:
                    if ch.get_node_class() == ua.NodeClass.Object:
                        for interest_node in self.__interest_nodes:
                            for int_node in interest_node:
                                if re.search(int_node.split('\\.')[recursion_level-2], ch.get_display_name().Text):
                                    self.__search_name(ch, recursion_level+1)
                    elif ch.get_node_class() == ua.NodeClass.Variable:
                        try:
                            for interest_node in self.__interest_nodes:
                                for int_node in interest_node:
                                    if interest_node[int_node].get("deviceName") is None:
                                        name_pattern = TBUtility.get_value(interest_node[int_node]["deviceNamePattern"],
                                                                           get_tag=True)
                                        device_name_node = re.search(name_pattern.split('.')[-1], current_var_path)
                                        if device_name_node is not None:
                                            device_name = ch.get_value()
                                            full_device_name = interest_node[int_node]["deviceNamePattern"].replace("${"+name_pattern+"}",
                                                                                                                    device_name)
                                            interest_node[int_node]["deviceName"] = full_device_name
                                            if not self.__gateway.get_devices().get(full_device_name):
                                                self.__gateway.add_device(full_device_name, {"connector": None})
                                            self.__gateway.update_device(full_device_name, "connector", self)
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
                                if re.search(int_node.split('\\.')[recursion_level-2], ch.get_display_name().Text):
                                    self.__search_tags(ch, recursion_level+1, sub)
                    elif ch.get_node_class() == ua.NodeClass.Variable:
                        try:
                            for interest_node in self.__interest_nodes:
                                for int_node in interest_node:
                                    if re.search(int_node.replace('$', ''), current_var_path):
                                        tags = []
                                        if interest_node[int_node].get("attributes"):
                                            tags.extend(interest_node[int_node]["attributes"])
                                        if interest_node[int_node].get("timeseries"):
                                            tags.extend(interest_node[int_node]["timeseries"])
                                        for tag in tags:
                                            target = TBUtility.get_value(tag["path"], get_tag=True)
                                            if ch.get_display_name().Text == target:
                                                sub.subscribe_data_change(ch)
                                                if interest_node[int_node].get("uplink_converter") is None:
                                                    if interest_node[int_node].get('converter') is None:
                                                        converter = OpcUaUplinkConverter(interest_node[int_node])
                                                    else:
                                                        converter = TBUtility.check_and_import('opcua', interest_node[int_node]['converter'])
                                                    interest_node[int_node]["uplink_converter"] = converter
                                                else:
                                                    converter = interest_node[int_node]["uplink_converter"]
                                                self.subscribed[ch] = {"converter": converter,
                                                                       "path": current_var_path}
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
            self.connector.data_to_send.append(converted_data)
            log.debug(converted_data)
        except Exception as e:
            log.exception(e)

    def event_notification(self, event):
        try:
            log.debug("Python: New event %s", event)
        except Exception as e:
            log.exception(e)

