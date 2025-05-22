#     Copyright 2025. ThingsBoard
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

import random
import socket
import ssl
import string
from queue import Queue, Empty
from re import fullmatch, match, search
from threading import Thread, Event
from time import sleep, time

import orjson

from thingsboard_gateway.connectors.mqtt.backward_compatibility_adapter import BackwardCompatibilityAdapter
from thingsboard_gateway.gateway.constant_enums import Status
from thingsboard_gateway.connectors.connector import Connector
from thingsboard_gateway.connectors.mqtt.mqtt_decorators import CustomCollectStatistics
from thingsboard_gateway.gateway.constants import DATA_RETRIEVING_STARTED, CONVERTED_TS_PARAMETER
from thingsboard_gateway.gateway.entities.converted_data import ConvertedData
from thingsboard_gateway.gateway.statistics.decorators import CollectAllReceivedBytesStatistics
from thingsboard_gateway.tb_utility.tb_loader import TBModuleLoader
from thingsboard_gateway.tb_utility.tb_utility import TBUtility
from thingsboard_gateway.gateway.statistics.statistics_service import StatisticsService
from thingsboard_gateway.tb_utility.tb_logger import init_logger

try:
    from paho.mqtt.client import Client, Properties
    from paho.mqtt.packettypes import PacketTypes
except ImportError:
    print("paho-mqtt library not found")
    TBUtility.install_package("paho-mqtt")
    from paho.mqtt.client import Client, Properties
    from paho.mqtt.packettypes import PacketTypes

from paho.mqtt.client import MQTTv31, MQTTv311, MQTTv5


MQTT_VERSIONS = {
    3: MQTTv31,
    4: MQTTv311,
    5: MQTTv5
}

RESULT_CODES_V3 = {
    1: "Connection rejected for unsupported protocol version",
    2: "Connection rejected for rejected client ID",
    3: "Connection rejected for unavailable server",
    4: "Connection rejected for damaged username or password",
    5: "Connection rejected for unauthorized",
}

RESULT_CODES_V5 = {
    4:   "Disconnect with Will Message",
    16:  "No matching subscribers",
    17:  "No subscription existed",
    128: "Unspecified error",
    129: "Malformed Packet",
    130: "Protocol Error",
    131: "Implementation specific error",
    132: "Unsupported Protocol Version",
    133: "Client Identifier not valid",
    134: "Bad User Name or Password",
    135: "Not authorized",
    136: "Server unavailable",
    137: "Server busy",
    138: "Banned",
    139: "Server shutting down",
    140: "Bad authentication method",
    141: "Keep Alive timeout",
    142: "Session taken over",
    143: "Topic Filter invalid",
    144: "Topic Name invalid",
    145: "Packet Identifier in use",
    146: "Packet Identifier not found",
    147: "Receive Maximum exceeded",
    148: "Topic Alias invalid",
    149: "Packet too large",
    150: "Message rate too high",
    151: "Quota exceeded",
    152: "Administrative action",
    153: "Payload format invalid",
    154: "Retain not supported",
    155: "QoS not supported",
    156: "Use another server",
    157: "Server moved",
    158: "Shared Subscription not supported",
    159: "Connection rate exceeded",
    160: "Maximum connect time",
    161: "Subscription Identifiers not supported",
    162: "Wildcard Subscription not supported"
}


class MqttConnector(Connector, Thread):
    CONFIGURATION_KEY_SHARED_GLOBAL = "sharedGlobal"
    CONFIGURATION_KEY_SHARED_ID = "sharedId"

    def __init__(self, gateway, config, connector_type):
        super().__init__()

        self.__gateway = gateway  # Reference to TB Gateway
        self._connector_type = connector_type  # Should be "mqtt"

        # Set up lifecycle flags ---------------------------------------------------------------------------------------
        self._connected = False
        self.__stopped = False
        self.__stop_event = Event()
        self.daemon = True

        self.__log = init_logger(self.__gateway, config['name'],
                                 config.get('logLevel', 'INFO'),
                                 enable_remote_logging=config.get('enableRemoteLogging', False),
                                 is_connector_logger=True)
        self.__converter_log = init_logger(self.__gateway, config['name'] + '_converter',
                                           config.get('logLevel', 'INFO'),
                                           enable_remote_logging=config.get('enableRemoteLogging', False),
                                           is_converter_logger=True, attr_name=config['name'])

        # check if the configuration is in the old format
        using_old_config_format_detected = BackwardCompatibilityAdapter.is_old_config_format(config)
        if using_old_config_format_detected:
            self.config = BackwardCompatibilityAdapter(config).convert()
            self.__id = self.config.get('id')
        else:
            self.config = config
            self.__id = self.config.get('id')

        self.statistics = {'MessagesReceived': 0, 'MessagesSent': 0}
        self.__subscribes_sent = {}

        if using_old_config_format_detected:
            self.__log.info("Old MQTT connector configuration format detected. Automatic conversion is applied.")

        # Extract main sections from configuration ---------------------------------------------------------------------
        self.__broker = config.get('broker')
        if not self.__broker:
            self.__log.error('Broker configuration is missing!')
            return

        self.__mapping = []
        self.__server_side_rpc = []
        self.__connect_requests = []
        self.__disconnect_requests = []
        self.__attribute_requests = []
        self.__attribute_updates = []

        self.__shared_custom_converters = {}

        mapping_key = 'mapping' if self.config.get('mapping') else 'dataMapping'

        mandatory_keys = {
            mapping_key: ['topicFilter', 'converter'],
            "serverSideRpc": ['deviceNameFilter', 'methodFilter', 'requestTopicExpression', 'valueExpression'],
            "connectRequests": ['topicFilter'],
            "disconnectRequests": ['topicFilter'],
            "attributeRequests": ['topicFilter', 'topicExpression', 'valueExpression'],
            "attributeUpdates": ['deviceNameFilter', 'attributeFilter', 'topicExpression', 'valueExpression']
        }

        # Mappings, i.e., telemetry/attributes-push handlers provided by user via configuration file
        self.load_handlers(mapping_key, mandatory_keys[mapping_key], self.__mapping)

        # RPCs, i.e., remote procedure calls (ThingsBoard towards devices) handlers
        self.load_handlers('serverSideRpc', mandatory_keys['serverSideRpc'], self.__server_side_rpc)

        # Connect requests, i.e., telling ThingsBoard that a device is online even if it does not post telemetry
        self.load_handlers('connectRequests', mandatory_keys['connectRequests'], self.__connect_requests)

        # Disconnect requests, i.e., telling ThingsBoard that a device is offline even if keep-alive has not expired yet
        self.load_handlers('disconnectRequests', mandatory_keys['disconnectRequests'], self.__disconnect_requests)

        # Shared attributes direct requests, i.e., asking ThingsBoard for some shared attribute value
        self.load_handlers('attributeRequests', mandatory_keys['attributeRequests'], self.__attribute_requests)

        # Attributes updates requests, i.e., asking ThingsBoard to send updates about an attribute
        self.load_handlers('attributeUpdates', mandatory_keys['attributeUpdates'], self.__attribute_updates)

        # Setup topic substitution lists for each class of handlers ----------------------------------------------------
        self.__mapping_sub_topics = {}
        self.__connect_requests_sub_topics = {}
        self.__disconnect_requests_sub_topics = {}
        self.__attribute_requests_sub_topics = {}

        # Set up external MQTT broker connection -----------------------------------------------------------------------
        client_id = self.__broker.get("clientId", ''.join(random.choice(string.ascii_lowercase) for _ in range(23)))

        self._cleanSession = self.__broker.get("cleanSession", True)
        self._cleanStart = self.__broker.get("cleanStart", True)
        self._sessionExpiryInterval = self.__broker.get("sessionExpiryInterval", 0)

        self._mqtt_version = self.__broker.get('version', 5)
        try:
            if self._mqtt_version != 5:
                self._client = Client(client_id=client_id, clean_session=self._cleanSession,
                                      protocol=MQTT_VERSIONS[self._mqtt_version])
            else:
                self._client = Client(client_id=client_id, protocol=MQTT_VERSIONS[self._mqtt_version])
        except KeyError:
            self.__log.error('Unknown MQTT version. Starting up on version 5...')
            self._client = Client(client_id=client_id, protocol=MQTTv5)
            self._mqtt_version = 5

        self.name = config.get("name", self.__broker.get(
            "name",
            'Mqtt Broker ' + ''.join(random.choice(string.ascii_lowercase) for _ in range(5))))

        if "username" in self.__broker["security"]:
            self._client.username_pw_set(self.__broker["security"].get("username"),
                                         self.__broker["security"].get("password"))

        if "caCert" in self.__broker["security"] \
                or self.__broker["security"].get("type", "none").lower() == "certificates":
            
            self.__log.debug("Connector connecting with certificates")
            ca_cert = self.__broker["security"].get("pathToCACert")
            private_key = self.__broker["security"].get("pathToPrivateKey")
            cert = self.__broker["security"].get("pathToClientCert")
            if ca_cert is None:
                self._client.tls_set_context(ssl.SSLContext(ssl.PROTOCOL_TLSv1_2))
            else:
                try:
                    self._client.tls_set(ca_certs=ca_cert,
                                         certfile=cert,
                                         keyfile=private_key,
                                         cert_reqs=ssl.CERT_REQUIRED,
                                         tls_version=ssl.PROTOCOL_TLSv1_2,
                                         ciphers=None)
                except Exception as e:
                    self.__log.error("Cannot setup connection to broker %s using SSL. "
                                     "Please check your configuration.\nError: %s",
                                     self.get_name(), e)
                if self.__broker["security"].get("insecure", False):
                    self._client.tls_insecure_set(True)
                    self.__log.debug("Connector tls_insecure_set: True")
                else:
                    self._client.tls_insecure_set(False)
                    self.__log.debug("Connector tls_insecure_set: False")
        else:
            self.__log.debug("Connector connecting anonymously")
        # Set up external MQTT broker callbacks ------------------------------------------------------------------------
        self._client.on_connect = self._on_connect
        self._client.on_message = self._on_message
        self._client.on_subscribe = self._on_subscribe
        self._client.on_disconnect = self._on_disconnect
        # self._client.on_log = self._on_log

        self.__msg_queue = Queue(self.__broker.get('maxMessageQueue', 1000000000))
        self.__workers_thread_pool = []
        self.__max_msg_number_for_worker = self.__broker.get('maxMessageNumberPerWorker', 10)
        self.__max_number_of_workers = self.__broker.get('maxNumberOfWorkers', 100)

        self._on_message_queue = Queue(self.__broker.get('maxProcessingMessageQueue', 1000000000))
        self._on_message_thread = Thread(name='On Message', target=self._process_on_message, daemon=True)
        self._on_message_thread.start()

    def get_config(self):
        return self.config

    def get_type(self):
        return self._connector_type

    @staticmethod
    def __add_ts_to_test_message(msg, ts_name):
        msg = orjson.loads(msg.decode('utf-8').replace("'", '"'))
        msg[ts_name] = int(time() * 1000)
        return orjson.dumps(msg).encode('utf-8')

    def load_handlers(self, handler_flavor, mandatory_keys, accepted_handlers_list):
        handler_configuration = self.config.get(handler_flavor)
        if handler_configuration is None:
            request_mapping_config = self.config.get("requestsMapping", {})
            if isinstance(request_mapping_config, dict):
                handler_configuration = None
                for request_mapping in request_mapping_config:
                    if request_mapping == handler_flavor:
                        handler_configuration = request_mapping_config[request_mapping]

        if not handler_configuration:
            self.__log.debug("'%s' section missing from configuration", handler_flavor)
        else:
            for handler in handler_configuration:
                discard = False

                for key in mandatory_keys:
                    if key not in handler:
                        # Will report all missing fields to user before discarding the entry => no break here
                        discard = True
                        self.__log.error("Mandatory key '%s' missing from %s handler: %r",
                                         key, handler_flavor, handler)
                    else:
                        self.__log.debug("Mandatory key '%s' found in %s handler: %r",
                                         key, handler_flavor, handler)

                if discard:
                    self.__log.warning("%s handler is missing some mandatory keys => rejected: %r",
                                       handler_flavor, handler)
                else:
                    accepted_handlers_list.append(handler)
                    self.__log.debug("%s handler has all mandatory keys => accepted: %r",
                                     handler_flavor, handler)

            self.__log.info("Number of accepted %s handlers: %d",
                            handler_flavor,
                            len(accepted_handlers_list))

            self.__log.debug("Number of rejected %s handlers: %d",
                             handler_flavor,
                             len(handler_configuration) - len(accepted_handlers_list))

    def is_connected(self):
        return self._connected

    def is_stopped(self):
        return self.__stopped

    def open(self):
        self.__stopped = False
        self.start()

    def run(self):
        try:
            self.__connect()
        except Exception as e:
            self.__log.exception("Error in connector main loop: %s", e)

        while not self.__stopped:
            try:
                if not self._connected:
                    self.__connect()

                self.__threads_manager()

                self.__stop_event.wait(timeout=0.2)
            except TimeoutError:
                pass
            except Exception as e:
                self.__log.exception("Error in connector main loop: %s", e)

    def __connect(self):
        while not self._connected and not self.__stopped:
            try:
                if self._mqtt_version != 5:
                    self._client.connect(self.__broker['host'], self.__broker.get('port', 1883))
                else:
                    properties = Properties(PacketTypes.CONNECT)
                    properties.SessionExpiryInterval = self._sessionExpiryInterval
                    self._client.connect(self.__broker['host'],
                                         self.__broker.get('port', 1883),
                                         clean_start=self._cleanStart,
                                         properties=properties)
                self._client.loop_start()
                if not self._connected:
                    sleep(1)
            except (ConnectionRefusedError, ConnectionResetError, ssl.SSLEOFError, socket.timeout) as e:
                self.__log.error("Error while connecting to broker %s: %s", self.get_name(), e)
                sleep(10)
            except Exception as e:
                self.__log.exception("Error while connecting to broker %s: %s", self.get_name(), e)
                sleep(10)

    def close(self):
        self.__stopped = True
        self.__stop_event.set()
        try:
            self._client.disconnect()
        except Exception as e:
            self.__log.exception(e)
        self._client.loop_stop()
        for worker in self.__workers_thread_pool:
            worker.stop()
        self.__log.info('%s has been stopped.', self.get_name())
        self.__log.stop()

    def get_name(self):
        return self.name

    def get_id(self):
        return self.__id

    def __subscribe(self, topic, qos):
        message = self._client.subscribe(topic, qos)
        try:
            self.__subscribes_sent[message[1]] = topic
        except Exception as e:
            self.__log.exception(e)

    def _on_connect(self, client, userdata, flags, result_code, *extra_params):
        if result_code == 0:
            self._connected = True
            self.__log.info('%s connected to %s:%s - successfully.',
                            self.get_name(),
                            self.__broker["host"],
                            self.__broker.get("port", "1883"))

            self.__log.debug("Client %s, userdata %s, flags %s, extra_params %s",
                             str(client),
                             str(userdata),
                             str(flags),
                             extra_params)

            self.__mapping_sub_topics = {}

            # Setup data upload requests handling ----------------------------------------------------------------------
            for mapping in self.__mapping:
                try:
                    # Load converter for this mapping entry ------------------------------------------------------------
                    # mappings are guaranteed to have topicFilter and converter fields. See __init__
                    default_converters = {
                        "json": "JsonMqttUplinkConverter",
                        "bytes": "BytesMqttUplinkConverter"
                    }

                    # Get converter class from "extension" parameter or default converter
                    converter_class_name = mapping["converter"].get("extension",
                                                                    default_converters.get(
                                                                        mapping['converter'].get('type')))
                    if not converter_class_name:
                        self.__log.error('Converter type or extension class should be configured!')
                        continue

                    converter = None
                    sharing_id = None
                    if mapping["converter"].get("type") == "custom" or mapping["converter"].get("extension"):
                        if mapping["converter"].get(self.CONFIGURATION_KEY_SHARED_GLOBAL, False):
                            sharing_id = converter_class_name
                        elif mapping["converter"].get(self.CONFIGURATION_KEY_SHARED_ID):
                            sharing_id = mapping["converter"][self.CONFIGURATION_KEY_SHARED_ID]

                        if sharing_id:
                            converter = self.__shared_custom_converters.get(sharing_id)
                            self.__log.debug('Converter %s for topic %s will use in sharing mode!', sharing_id,
                                             mapping["topicFilter"])

                    if not converter:
                        # Find and load required class
                        module = TBModuleLoader.import_module(self._connector_type, converter_class_name)
                        if module:
                            self.__log.debug('Converter %s for topic %s - found!', converter_class_name,
                                             mapping["topicFilter"])
                            converter = module(mapping, self.__converter_log)
                            if sharing_id:
                                self.__shared_custom_converters[sharing_id] = converter
                        else:
                            self.__log.error("Cannot find converter for %s topic", mapping["topicFilter"])
                            continue
                    else:
                        self.__log.debug('Converter %s for topic %s - found in cache!', converter_class_name,
                                         mapping["topicFilter"])

                    # Setup regexp topic acceptance list ---------------------------------------------------------------
                    # Check if topic is shared subscription type
                    # (an exception is aws topics that do not support shared subscription)
                    regex_topic = mapping["topicFilter"]
                    if not regex_topic.startswith('$aws') and regex_topic.startswith('$'):
                        regex_topic = '/'.join(regex_topic.split('/')[2:])
                    else:
                        regex_topic = TBUtility.topic_to_regex(regex_topic)

                    # There may be more than one converter per topic, so I'm using vectors
                    if not self.__mapping_sub_topics.get(regex_topic):
                        self.__mapping_sub_topics[regex_topic] = []

                    self.__mapping_sub_topics[regex_topic].append(converter)

                    # Subscribe to appropriate topic -------------------------------------------------------------------
                    self.__subscribe(mapping["topicFilter"], mapping.get("subscriptionQos", 1))

                    self.__log.info('Connector "%s" subscribe to %s',
                                    self.get_name(),
                                    TBUtility.regex_to_topic(regex_topic))

                except Exception as e:
                    self.__log.exception(e)

            # Setup connection requests handling -----------------------------------------------------------------------
            for request in [entry for entry in self.__connect_requests if entry is not None]:
                # requests are guaranteed to have topicFilter field. See __init__
                self.__subscribe(request["topicFilter"], request.get("subscriptionQos", 1))
                topic_filter = TBUtility.topic_to_regex(request.get("topicFilter"))
                self.__connect_requests_sub_topics[topic_filter] = request

            # Setup disconnection requests handling --------------------------------------------------------------------
            for request in [entry for entry in self.__disconnect_requests if entry is not None]:
                # requests are guaranteed to have topicFilter field. See __init__
                self.__subscribe(request["topicFilter"], request.get("subscriptionQos", 1))
                topic_filter = TBUtility.topic_to_regex(request.get("topicFilter"))
                self.__disconnect_requests_sub_topics[topic_filter] = request

            # Setup attributes requests handling -----------------------------------------------------------------------
            for request in [entry for entry in self.__attribute_requests if entry is not None]:
                # requests are guaranteed to have topicFilter field. See __init__
                self.__subscribe(request["topicFilter"], request.get("subscriptionQos", 1))
                topic_filter = TBUtility.topic_to_regex(request.get("topicFilter"))
                self.__attribute_requests_sub_topics[topic_filter] = request
        else:
            result_codes = RESULT_CODES_V5 if self._mqtt_version == 5 else RESULT_CODES_V3
            rc = result_code.value if self._mqtt_version == 5 else result_code
            if rc in result_codes:
                self.__log.error("%s connection FAIL with error %s %s!", self.get_name(), rc, result_codes[rc])
            else:
                self.__log.error("%s connection FAIL with unknown error!", self.get_name())

    def _on_disconnect(self, *args):
        self._connected = False
        self.__log.debug('"%s" was disconnected. %s', self.get_name(), str(args))

    def _on_log(self, *args):
        self.__log.debug(args)

    def _on_subscribe(self, _, __, mid, granted_qos, *args):
        self.__log.info(args)
        try:
            if granted_qos[0] == 128:
                self.__log.error('"%s" subscription failed to topic %s subscription message id = %i',
                                 self.get_name(),
                                 self.__subscribes_sent.get(mid), mid)
            else:
                self.__log.info('"%s" subscription success to topic %s, subscription message id = %i',
                                self.get_name(),
                                self.__subscribes_sent.get(mid), mid)
        except Exception as e:
            self.__log.exception(e)

        # Success or not, remove this topic from the list of pending subscription requests
        if self.__subscribes_sent.get(mid) is not None:
            del self.__subscribes_sent[mid]

    def put_data_to_convert(self, converter, message, content) -> bool:
        if not self.__msg_queue.full():
            self.__msg_queue.put((converter.convert, message.topic, content), True, 100)
            return True
        return False

    def _save_converted_msg(self, topic, data):
        data.add_to_metadata({DATA_RETRIEVING_STARTED: int(time() * 1000)})
        if self.__gateway.send_to_storage(self.name, self.get_id(), data) == Status.SUCCESS:
            StatisticsService.count_connector_message(self.name, stat_parameter_name='storageMsgPushed')
            self.statistics['MessagesSent'] += 1
            self.__log.debug("Successfully converted message from topic %s", topic)

    def __threads_manager(self):
        if len(self.__workers_thread_pool) == 0:
            worker = MqttConnector.ConverterWorker("Main Worker", self.__msg_queue, self._save_converted_msg)
            self.__workers_thread_pool.append(worker)
            worker.start()

        number_of_needed_threads = round(self.__msg_queue.qsize() / self.__max_msg_number_for_worker, 0)
        threads_count = len(self.__workers_thread_pool)
        if number_of_needed_threads > threads_count < self.__max_number_of_workers:
            thread = MqttConnector.ConverterWorker(
                "Worker " + ''.join(random.choice(string.ascii_lowercase) for _ in range(5)), self.__msg_queue,
                self._save_converted_msg)
            self.__workers_thread_pool.append(thread)
            thread.start()
        elif number_of_needed_threads < threads_count and threads_count > 1:
            worker: MqttConnector.ConverterWorker = self.__workers_thread_pool[-1]
            worker.stopped = True
            self.__workers_thread_pool.remove(worker)

    def _on_message(self, client, userdata, message):
        StatisticsService.count_connector_message(self.name, stat_parameter_name='connectorMsgsReceived')
        StatisticsService.count_connector_bytes(self.name, message.payload,
                                                stat_parameter_name='connectorBytesReceived')
        self._on_message_queue.put((client, userdata, message))

    @staticmethod
    def _parse_device_info(device_info, topic, content):
        found_device_name = None
        found_device_type = 'default'

        # Get device name, either from topic or from content
        if device_info.get('deviceNameExpressionSource') == 'topic':
            device_name_match = search(device_info["deviceNameExpression"], topic)
            if device_name_match is not None:
                found_device_name = device_name_match.group(0)
        elif device_info.get('deviceNameExpressionSource') == 'message':
            found_device_name = TBUtility.get_value(device_info["deviceNameExpression"], content,
                                                    expression_instead_none=True)
        elif device_info.get('deviceNameExpressionSource') == 'constant':
            found_device_name = device_info["deviceNameExpression"]

        # Get device type (if any), either from topic or from content
        if device_info.get("deviceProfileExpressionSource") == 'topic':
            device_type_match = search(device_info["deviceProfileExpression"], topic)
            found_device_type = device_type_match.group(0) if device_type_match is not None else device_info[
                "deviceProfileExpression"]
        elif device_info.get("deviceProfileExpressionSource") == 'message':
            found_device_type = TBUtility.get_value(device_info["deviceProfileExpression"], content,
                                                    expression_instead_none=True)
        elif device_info.get("deviceProfileExpressionSource") == 'constant':
            found_device_type = TBUtility.get_value(device_info["deviceProfileExpression"], content,)

        return found_device_name, found_device_type

    def _process_on_message(self):
        while not self.__stopped:
            if not self._on_message_queue.empty():
                client, userdata, message = self._on_message_queue.get_nowait()

                self.statistics['MessagesReceived'] += 1
                content = TBUtility.decode(message)

                # Check if message topic exists in mappings "i.e., I'm posting telemetry/attributes" -------------------
                topic_handlers = [regex for regex in self.__mapping_sub_topics if fullmatch(regex, message.topic)]

                if topic_handlers:
                    # Note: every topic may be associated to one or more converter.
                    # This means that a single MQTT message
                    # may produce more than one message towards ThingsBoard. This also means that I cannot return after
                    # the first successful conversion: I got to use all the available ones.
                    # I will use a flag to understand whether at least one converter succeeded
                    request_handled = False

                    for topic in topic_handlers:
                        available_converters = self.__mapping_sub_topics[topic]
                        for converter in available_converters:
                            try:
                                request_handled = self.put_data_to_convert(converter, message, content)
                            except Exception as e:
                                self.__log.exception(e)

                    if not request_handled:
                        self.__log.error('Cannot find converter for the topic:"%s"! Client: %s, User data: %s',
                                         message.topic,
                                         str(client),
                                         str(userdata))

                    # Note: if I'm in this branch, this was for sure a telemetry/attribute push message
                    # => Execution must end here both in case of failure and success
                    continue

                # Check if message topic exists in connection handlers "i.e., I'm connecting a device" -----------------
                topic_handlers = [regex for regex in self.__connect_requests_sub_topics if
                                  fullmatch(regex, message.topic)]

                if topic_handlers:
                    for topic in topic_handlers:
                        handler = self.__connect_requests_sub_topics[topic]

                        # Get device name, either from topic or from content
                        device_info = handler.get("deviceInfo", {})

                        found_device_name, found_device_type = MqttConnector._parse_device_info(device_info,
                                                                                                message.topic, content)

                        if found_device_name is None:
                            self.__log.error("Device name missing from connection request")
                            continue

                        # Note: device must be added even if it is already known locally: else ThingsBoard
                        # will not send RPCs and attribute updates
                        self.__log.info("Connecting device %s of type %s", found_device_name, found_device_type)
                        self.__gateway.add_device(found_device_name, {"connector": self}, device_type=found_device_type)

                    # Note: if I'm in this branch, this was for sure a connection message
                    # => Execution must end here both in case of failure and success
                    continue

                # Check if message topic exists in disconnection handlers "i.e., I'm disconnecting a device" -----------
                topic_handlers = [regex for regex in self.__disconnect_requests_sub_topics if
                                  fullmatch(regex, message.topic)]
                if topic_handlers:
                    for topic in topic_handlers:
                        handler = self.__disconnect_requests_sub_topics[topic]

                        # Get device name, either from topic or from content
                        device_info = handler.get("deviceInfo", {})
                        found_device_name, found_device_type = MqttConnector._parse_device_info(device_info,
                                                                                                message.topic, content)

                        if found_device_name is None:
                            self.__log.error("Device name missing from disconnection request")
                            continue

                        if found_device_name in self.__gateway.get_devices():
                            self.__log.info("Disconnecting device %s of type %s", found_device_name, found_device_type)
                            self.__gateway.del_device(found_device_name)
                        else:
                            self.__log.info("Device %s was not connected", found_device_name)

                        break

                    # Note: if I'm in this branch, this was for sure a disconnection message
                    # => Execution must end here both in case of failure and success
                    continue

                # Check if message topic exists in attribute request handlers "i.e., I'm asking for a shared attribute"
                topic_handlers = [regex for regex in self.__attribute_requests_sub_topics if
                                  fullmatch(regex, message.topic)]
                if topic_handlers:
                    try:
                        for topic in topic_handlers:
                            handler = self.__attribute_requests_sub_topics[topic]

                            found_attribute_names = None

                            # Get device name, either from topic or from content
                            device_info = handler.get("deviceInfo", {})
                            found_device_name, _ = MqttConnector._parse_device_info(device_info, message.topic, content)

                            # Get attribute name, either from topic or from content
                            if handler.get("attributeNameExpressionSource") == "topic":
                                attribute_name_match = search(handler["attributeNameExpression"], message.topic)
                                if attribute_name_match is not None:
                                    found_attribute_names = attribute_name_match.group(0)
                            elif handler.get("attributeNameExpressionSource") == "message" or handler.get(
                                    "attributeNameExpressionSource") == "constant":
                                found_attribute_names = list(filter(lambda x: x is not None,
                                                                    TBUtility.get_values(
                                                                        handler["attributeNameExpression"],
                                                                        content)))

                            if found_device_name is None:
                                self.__log.error("Device name missing from attribute request")
                                continue

                            if found_attribute_names is None:
                                self.__log.error("Attribute name missing from attribute request")
                                continue

                            self.__log.info("Will retrieve attribute %s of %s", found_attribute_names,
                                            found_device_name)
                            scope = 'shared'
                            if handler.get('scope') is not None:
                                scope = handler.get('scope')
                            if content and TBUtility.get_value(f'${scope}', content, get_tag=True) is not None:
                                scope = TBUtility.get_value(f'${scope}', content, get_tag=True)

                            request_arguments = (
                                    found_device_name,
                                    found_attribute_names,
                                    lambda data, *args: self.notify_attribute(
                                        data,
                                        found_attribute_names,
                                        handler.get("topicExpression"),
                                        handler.get("valueExpression"),
                                        handler.get('retain', False),
                                        handler.get('qos', 0)))

                            if scope == 'client':
                                self.__gateway.tb_client.client.gw_request_client_attributes(*request_arguments)
                            else:
                                self.__gateway.tb_client.client.gw_request_shared_attributes(*request_arguments)
                            break

                    except Exception as e:
                        self.__log.exception(e)

                    # Note: if I'm in this branch, this was for sure an attribute request message
                    # => Execution must end here both in case of failure and success
                    continue

                # Check if message topic exists in RPC handlers --------------------------------------------------------
                # The gateway is expecting for this message => no wildcards here, the topic must be evaluated as is

                if self.__gateway.is_rpc_in_progress(message.topic):
                    self.__log.info("RPC response arrived. Forwarding it to thingsboard.")
                    self.__gateway.rpc_with_reply_processing(message.topic, content)
                    continue

                self.__log.debug("Received message to topic \"%s\" with unknown interpreter data: \n\n\"%s\"",
                                 message.topic,
                                 content)
            else:
                sleep(.2)

    def notify_attribute(self, incoming_data, attribute_name, topic_expression, value_expression, retain, qos):
        if incoming_data.get("device") is None or incoming_data.get("value", incoming_data.get('values')) is None:
            return

        device_name = incoming_data.get("device")
        attribute_values = incoming_data.get("value", incoming_data.get("values"))

        topic = topic_expression \
            .replace("${deviceName}", str(device_name)) \
            .replace("${attributeKey}", str(attribute_name))

        if len(attribute_name) <= 1:
            data = value_expression.replace("${attributeKey}", str(attribute_name[0])) \
                .replace("${attributeValue}", str(attribute_values))
        else:
            data = orjson.dumps(attribute_values)

        self._client.publish(topic, data, qos=qos, retain=retain).wait_for_publish()

    @CollectAllReceivedBytesStatistics(start_stat_type='allReceivedBytesFromTB')
    def on_attributes_update(self, content):
        if self.__attribute_updates:
            for attribute_update in self.__attribute_updates:
                if match(attribute_update["deviceNameFilter"], content["device"]):
                    for attribute_key in content["data"]:
                        if match(attribute_update["attributeFilter"], attribute_key):
                            received_value = content["data"][attribute_key]
                            if isinstance(received_value, dict) or isinstance(received_value, list):
                                received_value = orjson.dumps(received_value)
                            elif isinstance(received_value, bool):
                                received_value = str(received_value).lower()
                            elif isinstance(received_value, float) or isinstance(received_value, int):
                                received_value = str(received_value)
                            elif received_value is None:
                                received_value = "null"
                            try:
                                topic = attribute_update["topicExpression"] \
                                    .replace("${deviceName}", str(content["device"])) \
                                    .replace("${attributeKey}", str(attribute_key)) \
                                    .replace("${attributeValue}", received_value)
                            except KeyError as e:
                                self.__log.exception("Cannot form topic, key %s - not found", e)
                                raise e
                            try:
                                data = attribute_update["valueExpression"] \
                                    .replace("${attributeKey}", str(attribute_key)) \
                                    .replace("${attributeValue}", received_value) \
                                    .replace("${deviceName}", str(content["device"]))

                            except KeyError as e:
                                self.__log.exception("Cannot form topic, key %s - not found", e)
                                raise e

                            self._publish(topic, data, attribute_update.get('retain', False), attribute_update.get('qos', 0))
                            self.__log.debug("Attribute Update data: %s for device %s to topic: %s", data,
                                             content["device"], topic)
                        else:
                            self.__log.error("Cannot find attributeName by filter in message with data: %s", content)
                else:
                    self.__log.error("Cannot find deviceName by filter in message with data: %s", content)
        else:
            self.__log.error("Attribute updates config not found.")

    def __process_rpc_request(self, content, rpc_config):
        try:
            # This handler seems able to handle the request
            self.__log.info("Candidate RPC handler found")

            expects_response = rpc_config.get("responseTopicExpression")
            defines_timeout = rpc_config.get("responseTimeout")

            # 2-way RPC setup
            if expects_response and defines_timeout:
                expected_response_topic = rpc_config["responseTopicExpression"] \
                    .replace("${methodName}", str(content['data']['method'])) \
                    .replace("${requestId}", str(content["data"]["id"]))

                if content.get('device'):
                    expected_response_topic = expected_response_topic.replace("${deviceName}", str(content["device"]))

                expected_response_topic = TBUtility.replace_params_tags(expected_response_topic, content)

                timeout = time() * 1000 + rpc_config.get("responseTimeout")

                # Start listening on the response topic
                self.__log.info("Subscribing to: %s", expected_response_topic)
                self.__subscribe(expected_response_topic, rpc_config.get("responseTopicQoS", 1))

                # Wait for subscription to be carried out
                sub_response_timeout = 10

                while expected_response_topic in self.__subscribes_sent.values():
                    sub_response_timeout -= 1
                    sleep(0.1)
                    if sub_response_timeout == 0:
                        break

                # Ask the gateway to enqueue this as an RPC response
                self.__gateway.register_rpc_request_timeout(content,
                                                            timeout,
                                                            expected_response_topic,
                                                            self.rpc_cancel_processing)

                # Wait for RPC to be successfully enqueued, which never fails.
                while self.__gateway.is_rpc_in_progress(expected_response_topic):
                    sleep(0.1)

            elif expects_response and not defines_timeout:
                self.__log.info("2-way RPC without timeout: treating as 1-way")

            # Actually reach out for the device
            request_topic: str = rpc_config.get("requestTopicExpression") \
                .replace("${methodName}", str(content['data']['method'])) \
                .replace("${requestId}", str(content["data"]["id"]))

            if content.get('device'):
                request_topic = request_topic.replace("${deviceName}", str(content["device"]))

            request_topic = TBUtility.replace_params_tags(request_topic, content)

            data_to_send_tags = TBUtility.get_values(rpc_config.get('valueExpression'), content['data'],
                                                     'params',
                                                     get_tag=True)
            data_to_send_values = TBUtility.get_values(rpc_config.get('valueExpression'), content['data'],
                                                       'params',
                                                       expression_instead_none=True)

            data_to_send = rpc_config.get('valueExpression')

            if content.get('device'):
                data_to_send = data_to_send.replace("${deviceName}", str(content["device"]))

            for (tag, value) in zip(data_to_send_tags, data_to_send_values):
                data_to_send = data_to_send.replace('${' + tag + '}', orjson.dumps(value).decode('utf-8'))

            try:
                self.__log.info("Publishing to: %s with data %s", request_topic, data_to_send)
                result = None
                try:
                    result = self._publish(request_topic, data_to_send, rpc_config.get('retain', False), rpc_config.get('qos', 0))
                except Exception as e:
                    self.__log.exception("Error during publishing to target broker: %r", e)
                    self.__gateway.send_rpc_reply(device=content.get("device"),
                                                  req_id=content["data"]["id"],
                                                  content={
                                                      "error": str.format("Error on publish to target broker: %r",
                                                                          str(e))},
                                                  success_sent=False, to_connector_rpc=True if content.get('device') is None else False) # noqa
                    return
                if not expects_response or not defines_timeout:
                    self.__log.info("One-way RPC: sending ack to ThingsBoard immediately")
                    self.__gateway.send_rpc_reply(device=content.get('device'), req_id=content["data"]["id"],
                                                  success_sent=result is not None, to_connector_rpc=True if content.get('device') is None else False) # noqa

                # Everything went out smoothly: RPC is served
                return
            except Exception as e:
                self.__log.exception("Error during publishing RPC response: ", exc_info=e)
        except Exception as e:
            self.__log.exception("Error during processing RPC request: ", exc_info=e)

    @CollectAllReceivedBytesStatistics(start_stat_type='allReceivedBytesFromTB')
    def server_side_rpc_handler(self, content):
        try:
            self.__log.info("Incoming server-side RPC: %s", content)

            if content.get('data') is None:
                content['data'] = {'params': content['params'], 'method': content['method'], 'id': content['id']}

            rpc_method = content['data']['method']

            # check if RPC type is connector RPC (can be only 'get' or 'set')
            try:
                (connector_type, rpc_method_name) = rpc_method.split('_')
                if connector_type == self._connector_type:
                    rpc_method = rpc_method_name
            except ValueError:
                pass

            if content.get('device'):
                # check if RPC method is reserved get/set
                if rpc_method == 'get' or rpc_method == 'set':
                    params = {}
                    for param in content['data']['params'].split(';'):
                        try:
                            (key, value) = param.split('=')
                        except ValueError:
                            continue

                        if key and value:
                            params[key] = value

                    params['valueExpression'] = params.pop('value', None)

                    return self.__process_rpc_request(content, params)
                else:
                    # Check whether one of my RPC handlers can handle this request
                    for rpc_config in self.__server_side_rpc:
                        if search(rpc_config["deviceNameFilter"], content["device"]) \
                                and search(rpc_config["methodFilter"], rpc_method) is not None:

                            return self.__process_rpc_request(content, rpc_config)

                    self.__log.error("RPC not handled: %s", content)
            else:
                return self.__process_rpc_request(content, content['data']['params'])
        except Exception as e:
            self.__log.exception("Error during handling RPC request", exc_info=e)

    @CustomCollectStatistics(start_stat_type='allBytesSentToDevices')
    def _publish(self, request_topic, data_to_send, retain, qos):
        result = False
        try:
            if self._connected and self._client is not None and self._client.is_connected():
                self._client.publish(request_topic, data_to_send, qos=qos, retain=retain).wait_for_publish()
                result = True
        except Exception as e:
            self.__log.error("Error during publishing to target broker: %r", e)
        return result

    def rpc_cancel_processing(self, topic):
        self.__log.info("RPC canceled or terminated. Unsubscribing from %s", topic)
        self._client.unsubscribe(topic)

    def get_converters(self):
        return [item[0] for _, item in self.__mapping_sub_topics.items()]


    def _send_current_converter_config(self, name, config):
        self.__gateway.send_attributes({name: config})

    class ConverterWorker(Thread):
        def __init__(self, name, incoming_queue, send_result, batch_size=100):
            super().__init__()
            self.stopped = False
            self.name = name
            self.daemon = True
            self.__msg_queue = incoming_queue
            self.__send_result = send_result
            self.__batch_size = batch_size
            self.__stop_event = Event()

        def run(self):
            while not self.stopped:
                try:
                    batch = []
                    for _ in range(self.__batch_size):
                        try:
                            batch.append(self.__msg_queue.get_nowait())
                        except Empty:
                            break

                    if not batch:
                        self.__stop_event.wait(0.01)
                        continue

                    for convert_function, config, incoming_data in batch:
                        converted_data: ConvertedData = convert_function(config, incoming_data)
                        converted_data.add_to_metadata({CONVERTED_TS_PARAMETER: int(time() * 1000)})
                        if converted_data and (converted_data.telemetry_datapoints_count > 0 or
                                               converted_data.attributes_datapoints_count > 0):
                            self.__send_result(config, converted_data)
                except Exception as e:
                    # Log the exception if needed
                    print("Error in worker: ", e)
                    pass

        def stop(self):
            self.stopped = True
            self.__stop_event.set()
