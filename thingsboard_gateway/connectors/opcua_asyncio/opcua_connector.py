import asyncio
import re
from random import choice
from string import ascii_lowercase
from threading import Thread
from time import sleep, time
from queue import Queue

from thingsboard_gateway.connectors.connector import Connector, log
from thingsboard_gateway.tb_utility.tb_loader import TBModuleLoader
from thingsboard_gateway.tb_utility.tb_utility import TBUtility

try:
    import asyncua
except ImportError:
    print("OPC-UA library not found")
    TBUtility.install_package("asyncua")
    import asyncua

from asyncua.crypto.security_policies import SecurityPolicyBasic256Sha256, SecurityPolicyBasic256, \
    SecurityPolicyBasic128Rsa15

DEFAULT_UPLINK_CONVERTER = 'OpcUaUplinkConverter'

SECURITY_POLICIES = {
    "Basic128Rsa15": SecurityPolicyBasic128Rsa15,
    "Basic256": SecurityPolicyBasic256,
    "Basic256Sha256": SecurityPolicyBasic256Sha256,
}


class OpcUaConnectorAsyncIO(Connector, Thread):
    DATA_TO_SEND = Queue(-1)
    SUB_DATA_TO_CONVERT = Queue(-1)

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
        self.__subscriptions = []
        self.__last_poll = 0

    def open(self):
        self.__stopped = False
        self.start()
        log.info("Starting OPC-UA Connector (Async IO)")

    def close(self):
        task = self.__loop.create_task(self.__close_subscriptions())

        while not task.done():
            sleep(.2)

        self.__stopped = True
        self.__connected = False
        self.__log.info('%s has been stopped.', self.get_name())

    async def __close_subscriptions(self):
        for subscription in self.__subscriptions:
            sub, handle = subscription
            await sub.unsubscribe(handle)
            await sub.delete()

    def get_name(self):
        return self.name

    def is_connected(self):
        return self.__connected

    def run(self):
        data_send_thread = Thread(name='Send Data Thread', target=self.__send_data, daemon=True)
        data_send_thread.start()

        if not self.__server_conf.get('disableSubscriptions', False):
            sub_data_convert_thread = Thread(name='Sub Data Convert Thread', target=self.__convert_sub_data,
                                             daemon=True)
            sub_data_convert_thread.start()

        self.__loop.run_until_complete(self.start_client())

    async def start_client(self):
        self.__client = asyncua.Client(url=self.__opcua_url,
                                       timeout=self.__server_conf.get('timeoutInMillis', 4000) / 1000)

        if self.__server_conf.get("type") == "cert.PEM":
            await self.__set_auth_settings_by_cert()
        if self.__server_conf["identity"].get("username"):
            self.__set_auth_settings_by_username()

        async with self.__client:
            self.__connected = True

            await self.__validate_nodes()

            while not self.__stopped:
                if time() - self.__last_poll >= self.__server_conf.get('scanPeriodInMillis', 5000) / 1000:
                    await self.__poll_nodes()
                    self.__last_poll = time()

                await asyncio.sleep(.2)

        self.__connected = False

    async def __set_auth_settings_by_cert(self):
        try:
            ca_cert = self.__server_conf["identity"].get("caCert")
            private_key = self.__server_conf["identity"].get("privateKey")
            cert = self.__server_conf["identity"].get("cert")
            policy = self.__server_conf["security"]

            if cert is None or private_key is None:
                log.exception("Error in ssl configuration - cert or privateKey parameter not found")
                raise RuntimeError("Error in ssl configuration - cert or privateKey parameter not found")

            await self.__client.set_security(
                SECURITY_POLICIES[policy],
                certificate=cert,
                private_key=private_key,
                server_certificate=ca_cert
            )
        except Exception as e:
            self.__log.exception(e)

    def __set_auth_settings_by_username(self):
        self.__client.set_user(self.__server_conf["identity"].get("username"))
        if self.__server_conf["identity"].get("password"):
            self.__client.set_password(self.__server_conf["identity"].get("password"))

    def __load_converter(self, device):
        converter_class_name = device.get('converter', DEFAULT_UPLINK_CONVERTER)
        module = TBModuleLoader.import_module(self._connector_type, converter_class_name)

        if module:
            log.debug('Converter %s for device %s - found!', converter_class_name, self.name)
            return module

        log.error("Cannot find converter for %s device", self.name)
        return None

    @staticmethod
    def is_regex_pattern(pattern):
        return not re.fullmatch(pattern, pattern)

    async def find_nodes(self, node_pattern, current_parent_node=None, level=0, nodes=None, final=None):
        node_list = node_pattern.split('.')

        if level <= len(node_list) - 1:
            if level == 0:
                current_parent_node = self.__client.nodes.root

            children = await current_parent_node.get_children()
            for node in children:
                child_node = await node.read_browse_name()

                if re.match(node_list[level], child_node.Name):
                    try:
                        nodes[level] = f'{child_node.NamespaceIndex}:{child_node.Name}'
                    except IndexError:
                        nodes.append(f'{child_node.NamespaceIndex}:{child_node.Name}')

                    await self.find_nodes(node_pattern, current_parent_node=node, level=level + 1, nodes=nodes,
                                          final=final)
        else:
            final.append(nodes[:])

        return final

    async def __validate_nodes(self):
        for device in self.__server_conf.get('mapping', []):
            self.__validated_nodes[device['deviceNamePattern']] = {**device}

            nodes = []
            if self.is_regex_pattern(device['deviceNodePattern']):
                nodes = await self.find_nodes(device['deviceNodePattern'], nodes=[], final=[])
                device['device_nodes'] = nodes

            # check if device name is regexp
            device_names = []
            if self.is_regex_pattern(device['deviceNamePattern']):
                device_name_nodes = await self.find_nodes(device['deviceNamePattern'], nodes=[], final=[])

                for node in device_name_nodes:
                    var = await self.__client.nodes.root.get_child(node)
                    value = await var.read_value()
                    device_names.append(value)

            for section in ('attributes', 'timeseries'):
                for node_config in device.get(section, []):
                    try:
                        if child_nodes := re.search(r"(ns=\d*;[isgb]=.*\d)", node_config['path']):
                            for child in child_nodes.groups():
                                self.__validated_nodes[device['deviceNamePattern']][section][
                                    device[section].index(node_config)]['node_paths'] = [{'path': child}]
                        elif child_nodes := re.search(r"\${([A-Za-z.:\d]*)}", node_config['path']):
                            for child in child_nodes.groups():
                                if nodes:
                                    self.__validated_nodes[device['deviceNamePattern']][section][
                                        device[section].index(node_config)]['node_paths'] = [
                                        {'path': [*device_node, *child.split('.')]} for device_node in nodes]
                                    device['device_names'] = device_names
                                else:
                                    device_node_path = device["deviceNodePattern"].replace("\\.", ".")
                                    self.__validated_nodes[device['deviceNamePattern']][section][
                                        device[section].index(node_config)]['node_paths'] = [
                                        {'path': [f'{device_node_path}.{item}' for item in child.split('.')]}]

                    except KeyError as e:
                        self.__log.error('Invalid config for %s (key %s not found)', device, e)

            converter = self.__load_converter(device)
            self.__validated_nodes[device['deviceNamePattern']]['converter'] = converter(device)
            self.__validated_nodes[device['deviceNamePattern']]['converter_for_sub'] = converter(device)

    def __convert_sub_data(self):
        while not self.__stopped:
            if not OpcUaConnectorAsyncIO.SUB_DATA_TO_CONVERT.empty():
                sub_node, data = OpcUaConnectorAsyncIO.SUB_DATA_TO_CONVERT.get()

                for (_, device) in self.__validated_nodes.items():
                    for section in ('attributes', 'timeseries'):
                        for node in device.get(section, []):
                            if node.get('id') == sub_node.__str__():
                                device['converter_for_sub'].convert(config={'section': section, 'key': node['key']},
                                                                    val=data.monitored_item.Value)
                                converter_data = device['converter_for_sub'].get_data()

                                if converter_data:
                                    OpcUaConnectorAsyncIO.DATA_TO_SEND.put(*converter_data)
                                    device['converter'].clear_data()
            sleep(.2)

    async def __poll_nodes(self):
        for (_, device) in self.__validated_nodes.items():
            for section in ('attributes', 'timeseries'):
                for node in device.get(section, []):
                    for (index, path) in enumerate(node.get('node_paths', [])):
                        if not path.get('invalid', False):
                            try:
                                var = await self.__client.nodes.root.get_child(path['path'])
                                value = await var.read_data_value()

                                device['converter'].convert(config={'section': section, 'key': node['key']}, val=value)
                            except Exception as e:
                                self.__log.exception(e)
                                path['invalid'] = True
                            else:
                                if not self.__server_conf.get('disableSubscriptions', False) and not node.get('sub_on',
                                                                                                              False):
                                    handler = SubHandler()
                                    sub = await self.__client.create_subscription(10, handler)
                                    handle = await sub.subscribe_data_change(var)
                                    node['sub_on'] = True
                                    node['id'] = f'ns={var.nodeid.NamespaceIndex};i={var.nodeid.Identifier}'
                                    self.__subscriptions.append((sub, handle))
                                    path['invalid'] = False

            converter_data = device['converter'].get_data()
            if converter_data:
                OpcUaConnectorAsyncIO.DATA_TO_SEND.put(*converter_data)

                device['converter'].clear_data()

    def __send_data(self):
        while not self.__stopped:
            if not OpcUaConnectorAsyncIO.DATA_TO_SEND.empty():
                data = OpcUaConnectorAsyncIO.DATA_TO_SEND.get()
                self.statistics['MessagesReceived'] = self.statistics['MessagesReceived'] + 1
                self.__log.debug(data)
                self.__gateway.send_to_storage(self.get_name(), data)
                self.statistics['MessagesSent'] = self.statistics['MessagesSent'] + 1
                self.__log.info('Data to ThingsBoard %s', data)

            sleep(.2)

    def on_attributes_update(self, content):
        self.__log.debug(content)
        try:
            for device in self.__server_conf['mapping']:
                if re.fullmatch(device['deviceNamePattern'], content['device']):
                    for (key, value) in content['data'].items():
                        for attr_update in device['attributes_updates']:
                            if attr_update['attributeOnThingsBoard'] == key:
                                for section in ('attributes', 'timeseries'):
                                    for node in device[section]:
                                        for path in node.get('node_paths', []):
                                            if not path['invalid'] and path.get('id') and re.fullmatch(
                                                    attr_update['attributeOnDevice'], path['path']):
                                                self.__loop.create_task(self.__write_value(path['id'], value))

        except Exception as e:
            self.__log.exception(e)

    def server_side_rpc_handler(self, content):
        try:
            rpc_method = content["data"].get("method")

            # firstly check if a method is not service
            if rpc_method == 'set' or rpc_method == 'get':
                full_path = ''
                args_list = []

                try:
                    args_list = content['data']['params'].split(';')

                    if 'ns' in content['data']['params']:
                        full_path = ';'.join([item for item in (args_list[0:-1] if rpc_method == 'set' else args_list)])
                    else:
                        full_path = args_list[0].split('=')[-1]
                except IndexError:
                    log.error('Not enough arguments. Expected min 2.')
                    self.__gateway.send_rpc_reply(content['device'],
                                                  content['data']['id'],
                                                  {content['data']['method']: 'Not enough arguments. Expected min 2.',
                                                   'code': 400})

                result = {}
                if rpc_method == 'get':
                    task = self.__loop.create_task(self.__read_value(full_path, result))

                    while not task.done():
                        sleep(.2)
                elif rpc_method == 'set':
                    value = args_list[2].split('=')[-1]
                    task = self.__loop.create_task(self.__write_value(full_path, value, result))

                    while not task.done():
                        sleep(.2)

                self.__gateway.send_rpc_reply(content['device'],
                                              content['data']['id'],
                                              {content['data']['method']: result})
            else:
                for device in self.__validated_nodes:
                    if content['device'] in device.get('device_names', []) or device['deviceNamePattern'] == content[
                            'device']:
                        for rpc in device['rpc_methods']:
                            if rpc['method'] == content["data"]['method']:
                                arguments_from_config = rpc["arguments"]
                                arguments = content["data"].get("params") if content["data"].get(
                                    "params") is not None else arguments_from_config

                                try:
                                    result = {}
                                    for node_path in device['device_nodes']:
                                        task = self.__loop.create_task(self.__call_method(node_path, arguments, result))

                                        while not task.done():
                                            sleep(.2)

                                    self.__gateway.send_rpc_reply(content["device"],
                                                                  content["data"]["id"],
                                                                  {content["data"]["method"]: result, "code": 200})
                                    log.debug("method %s result is: %s", rpc['method'], result)
                                except Exception as e:
                                    log.exception(e)
                                    self.__gateway.send_rpc_reply(content["device"], content["data"]["id"],
                                                                  {"error": str(e), "code": 500})
                            else:
                                log.error("Method %s not found for device %s", rpc_method, content["device"])
                                self.__gateway.send_rpc_reply(content["device"], content["data"]["id"],
                                                              {"error": "%s - Method not found" % rpc_method,
                                                               "code": 404})
                    else:
                        pass

        except Exception as e:
            self.__log.exception(e)

    async def __write_value(self, path, value, result={}):
        try:
            var = self.__client.get_node(path.replace('\\.', '.'))
            await var.write_value(value)
        except Exception as e:
            result['error'] = e.__str__()

    async def __read_value(self, path, result={}):
        try:
            var = self.__client.get_node(path)
            result['value'] = await var.read_value()
        except Exception as e:
            result['error'] = e.__str__()

    async def __call_method(self, path, arguments, result={}):
        try:
            var = self.__client.get_node(path)
            result['result'] = await var.call_method(*arguments)
        except Exception as e:
            result['error'] = e.__str__()


class SubHandler:
    @staticmethod
    def datachange_notification(node, _, data):
        log.info("New data change event %s %s", node, data)
        OpcUaConnectorAsyncIO.SUB_DATA_TO_CONVERT.put((node, data))

    @staticmethod
    def event_notification(event):
        try:
            log.debug("Python: New event %s", event)
        except Exception as e:
            log.exception(e)
