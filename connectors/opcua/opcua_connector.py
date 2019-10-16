import re
import sys
from json import dumps
import time
import threading
from random import choice
from string import ascii_lowercase
from opcua import Client, ua
from tb_utility.tb_utility import TBUtility
from opcua.ua.uaerrors._auto import BadWaitingForInitialData
from connectors.connector import Connector, log


class OpcUaConnector(Connector, threading.Thread):
    def __init__(self, gateway, config):
        super(Connector, self).__init__()
        super(threading.Thread, self).__init__()
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
        self.setName(self.__server_conf.get("name",
                                            'OPC-UA Default ' + ''.join(choice(ascii_lowercase) for _ in range(5))))
        self.__opcua_nodes = {}
        self._subscribed = {}
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
        sub = self.client.create_subscription(500, self.__sub_handler)
        self.__recursive_search(self.__opcua_nodes["objects"], 2, sub)
        while True:
            try:
                time.sleep(.1)
                if self.__stopped:
                    break
            except (KeyboardInterrupt, SystemExit):
                self.close()
                raise
            except Exception as e:
                self.close()
                log.exception(e)

    def __on_change(self, *data):
        log.debug(data)

    def close(self):
        self.__stopped = True
        self.client.disconnect()
        log.info('%s has been stopped.', self.get_name())

    def get_name(self):
        return self.name

    def on_attributes_update(self, content):
        pass

    def server_side_rpc_handler(self, content):
        pass

    def __recursive_search(self, node, recursion_level, sub):
        try:
            for childId in node.get_children():
                ch = self.client.get_node(childId)
                if self.__interest_nodes:
                    if ch.get_node_class() == ua.NodeClass.Object:
                        for interest_node in self.__interest_nodes:
                            for int_node in interest_node:
                                if re.search(int_node.split('\\.')[recursion_level-2], ch.get_display_name().Text):
                                    self.__recursive_search(ch, recursion_level+1, sub)
                    elif ch.get_node_class() == ua.NodeClass.Variable:
                        try:
                            current_var_path = '.'.join(x.split(":")[1] for x in ch.get_path(20000, True))
                            for interest_node in self.__interest_nodes:
                                for int_node in interest_node:
                                    if re.search(int_node.replace('$', ''), current_var_path):
                                        tags = []
                                        if interest_node[int_node].get("attributes"):
                                            tags.extend(interest_node[int_node]["attributes"])
                                        if interest_node[int_node].get("timeseries"):
                                            tags.extend(interest_node[int_node]["timeseries"])
                                        for tag in tags:
                                            target = TBUtility.get_value(tag["value"], get_tag=True)
                                            if ch.get_display_name().Text == target:
                                                sub.subscribe_data_change(ch)
                                                if not self.subscribed.get(ch):
                                                    self.subscribed[ch] = []
                                                self.subscribed[ch].append(current_var_path)
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
            curr_path = self.connector.subscribed[node]
            log.debug(curr_path)
        except Exception as e:
            log.exception(e)

    def event_notification(self, event):
        try:
            log.debug("Python: New event %s", event)
        except Exception as e:
            log.exception(e)

