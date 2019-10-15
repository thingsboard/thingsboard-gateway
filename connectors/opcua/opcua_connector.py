import threading
import time
import re
from random import choice
from string import ascii_lowercase
from opcua import Client, ua
from opcua.ua.uaerrors._auto import BadWaitingForInitialData
from connectors.connector import Connector, log


class OpcUaConnector(Connector, threading.Thread):
    def __init__(self, gateway, config):
        super(Connector, self).__init__()
        super(threading.Thread, self).__init__()
        self.__server_conf = config.get("server")
        self.__mapping = self.__server_conf["mapping"]
        self.__interest_nodes = [mapping["deviceNodePattern"] for mapping in self.__mapping if mapping.get("deviceNodePattern")]
        log.debug(self.__interest_nodes)
        if "opc.tcp" not in self.__server_conf.get("url"):
            opcua_url = "opc.tcp://"+self.__server_conf.get("url")
        else:
            opcua_url = self.__server_conf.get("url")
        self.client = Client(opcua_url, timeout=self.__server_conf.get("timeoutInMillis", 4000)/1000)
        self.setName(self.__server_conf.get("name",
                                            'OPC-UA Default ' + ''.join(choice(ascii_lowercase) for _ in range(5))))
        self.__opcua_nodes = {}
        self.__subscribed = {}
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
            except ConnectionRefusedError as e:
                log.error("Connection refused on connection to OPC-UA server with url %s", self.__server_conf.get("url"))
                time.sleep(10)
            except Exception as e:
                log.debug("error on connection to OPC-UA server.")
                log.error(e)
                time.sleep(10)
            else:
                self.__connected = True
                log.info("OPC-UA connector %s connected to server %s", self.get_name(), self.__server_conf.get("url"))
                # log.debug("Endpoints of OPC-UA server is: %s", self.__opcua_endpoints)
        self.__opcua_nodes["root"] = self.client.get_root_node()
        self.__opcua_nodes["objects"] = self.client.get_objects_node()
        sub = self.client.create_subscription(500, self.__on_change)
        log.debug(self.__subscribed)
        self.__recursive_search(self.__opcua_nodes["objects"], 2, sub)
        log.debug(self.__subscribed)
        while True:
            try:
                time.sleep(1)
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
        for childId in node.get_children():
            ch = self.client.get_node(childId)
            if ch.get_node_class() == ua.NodeClass.Object:
                for interest_node in self.__interest_nodes:
                    if interest_node.split('\\.')[recursion_level-2] == ch.get_display_name().Text:
                        self.__recursive_search(ch, recursion_level+1, sub)
            elif ch.get_node_class() == ua.NodeClass.Variable:
                try:
                    current_var_path = '.'.join(x.split(":")[1] for x in ch.get_path(200, True))
                    # log.debug(current_var_path)
                    for interest_node in self.__interest_nodes:
                        if re.search(interest_node, current_var_path):
                            self.__subscribed[current_var_path] = ch
                            sub.subscribe_data_change(ch)
                    # log.debug(sub)
                except BadWaitingForInitialData:
                    pass


class SubHandler(object):
    def datachange_notification(self, node, val, data):
        log.error("Python: New data change event", node, val)

    def event_notification(self, event):
        log.debug("Python: New event", event)
