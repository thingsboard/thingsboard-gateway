import asyncio
import re
from random import choice
from string import ascii_lowercase
from threading import Thread
from time import sleep, time
from queue import Queue

from thingsboard_gateway.connectors.connector import Connector, log
from thingsboard_gateway.tb_utility.tb_utility import TBUtility

try:
    import asyncua
except ImportError:
    print("OPC-UA library not found")
    TBUtility.install_package("asyncua")
    import asyncua


class OpcUaConnectorAsyncIO(Connector, Thread):
    DATA_TO_SEND = Queue(-1)

    def __init__(self, gateway, config, connector_type):
        self.statistics = {'MessagesReceived': 0,
                           'MessagesSent': 0}
        self.__log = log
        super().__init__()
        self._connector_type = connector_type
        self.__gateway = gateway
        self.__server_conf = config['server']

        self.setName(
            self.__server_conf.get("name", 'OPC-UA Connector ' + ''.join(choice(ascii_lowercase) for _ in range(5))))

        if "opc.tcp" not in self.__server_conf.get("url"):
            self.__opcua_url = "opc.tcp://" + self.__server_conf.get("url")
        else:
            self.__opcua_url = self.__server_conf.get("url")

        self.__loop = asyncio.new_event_loop()

        self.__client = None

        self.__connected = False
        self.__stopped = False
        self.daemon = True

        self.__validated_nodes = {}
        self.__last_poll = 0

    def open(self):
        self.__stopped = False
        self.start()
        log.info("Starting OPC-UA Connector (Async IO)")

    def close(self):
        self.__stopped = True
        self.__connected = False
        self.__log.info('%s has been stopped.', self.get_name())

    def get_name(self):
        return self.name

    def is_connected(self):
        return self.__connected

    def run(self):
        thread = Thread(name='Send Data Thread', target=self.__send_data, daemon=True)
        thread.start()
        self.__loop.run_until_complete(self.start_client())

    async def start_client(self):
        async with asyncua.Client(url=self.__opcua_url,
                                  timeout=self.__server_conf.get('timeoutInMillis', 4000) / 1000) as client:
            self.__client = client
            self.__connected = True

            await self.__validate_nodes()

            while not self.__stopped:
                if time() - self.__last_poll >= self.__server_conf.get('scanPeriodInMillis', 5000) / 1000:
                    await self.__poll_nodes()
                    self.__last_poll = time()

                await asyncio.sleep(.2)

        self.__connected = False

    @staticmethod
    def is_regex_pattern(pattern):
        return not re.fullmatch(pattern, pattern)

    async def find_nodes(self, node_pattern, current_parent_node=None, level=0, nodes=[], final=[]):
        if level <= len(node_pattern.split('.')):
            if level == 0:
                current_parent_node = self.__client.nodes.root

            # TODO: change read_display_name().Text to read_browse_name().Name for matching
            for node in await current_parent_node.get_children():
                child_node = await node.read_display_name()
                try:
                    if re.match(node_pattern.split('.')[level], child_node.Text):
                        if level == len(node_pattern.split('.')) - 1:
                            browser_name = await node.read_browse_name()
                            final.append(f'{browser_name.NamespaceIndex}:{browser_name.Name}')
                        else:
                            browser_name = await node.read_browse_name()
                            nodes.append(f'{browser_name.NamespaceIndex}:{browser_name.Name}')
                            await self.find_nodes(node_pattern, current_parent_node=node, level=level + 1, nodes=nodes)
                except IndexError:
                    continue

        return [[i, x] for x in final for i in nodes]

    async def __validate_nodes(self):
        for device in self.__server_conf.get('mapping', []):
            self.__validated_nodes[device['deviceNamePattern']] = {**device}

            nodes = []
            if self.is_regex_pattern(device['deviceNodePattern']):
                nodes = await self.find_nodes(device['deviceNodePattern'])

            for section in ('attributes', 'timeseries'):
                for node_config in device.get(section, []):
                    try:
                        if child_nodes := re.search(r"(ns=\d*;[isgb]=.*\d)", node_config['path']):
                            for child in child_nodes.groups():
                                self.__validated_nodes[device['deviceNamePattern']][section][
                                    device[section].index(node_config)]['node_paths'] = [child]
                        elif child_nodes := re.search(r"\${([A-Za-z.:\d]*)}", node_config['path']):
                            for child in child_nodes.groups():
                                if nodes:
                                    self.__validated_nodes[device['deviceNamePattern']][section][
                                        device[section].index(node_config)]['node_paths'] = [[*item, *child.split('.')]
                                                                                             for item in nodes]
                                else:
                                    device_node_path = device["deviceNodePattern"].replace("\\.", ".")
                                    self.__validated_nodes[device['deviceNamePattern']][section][
                                        device[section].index(node_config)]['node_paths'] = [
                                        [f'{device_node_path}.{item}' for item in child.split('.')]]

                    except KeyError as e:
                        self.__log.error('Invalid config for %s (key %s not found)', device, e)

        print(self.__validated_nodes)

    async def __poll_nodes(self):
        data_types = {
            'attributes': 'attributes',
            'timeseries': 'telemetry'
        }
        for (_, device) in self.__validated_nodes.items():
            converter_data = {
                'deviceName': device['deviceNamePattern'],
                'deviceType': 'default',
                'attributes': [],
                'telemetry': [],
            }
            for section in ('attributes', 'timeseries'):
                for node in device.get(section, []):
                    for path in node.get('node_paths', []):
                        try:
                            var = await self.__client.nodes.root.get_child(path)
                            value = await var.get_value()
                            converter_data[data_types[section]].append({node['key']: value})
                        except Exception as e:
                            self.__log.exception(e)
                            # TODO: remove invalid path

            OpcUaConnectorAsyncIO.DATA_TO_SEND.put(converter_data)

    def __send_data(self):
        while not self.__stopped:
            if not OpcUaConnectorAsyncIO.DATA_TO_SEND.empty():
                data = OpcUaConnectorAsyncIO.DATA_TO_SEND.get()
                self.statistics['MessagesReceived'] = self.statistics['MessagesReceived'] + 1
                log.debug(data)
                self.__gateway.send_to_storage(self.get_name(), data)
                self.statistics['MessagesSent'] = self.statistics['MessagesSent'] + 1
                log.info('Data to ThingsBoard %s', data)

            sleep(.2)

    def on_attributes_update(self, content):
        pass

    def server_side_rpc_handler(self, content):
        pass
