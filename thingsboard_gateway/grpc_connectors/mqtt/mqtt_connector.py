#     Copyright 2024. ThingsBoard
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

from random import choice
from string import ascii_lowercase
import ssl
from queue import Queue
from threading import Thread
from time import sleep, time
from re import fullmatch, match, search

from simplejson import dumps

from thingsboard_gateway.connectors.mqtt.mqtt_connector import MqttConnector, MQTT_VERSIONS, RESULT_CODES_V5, \
    RESULT_CODES_V3
from thingsboard_gateway.grpc_connectors.gw_grpc_connector import GwGrpcConnector, log
from thingsboard_gateway.grpc_connectors.gw_grpc_msg_creator import GrpcMsgCreator
from thingsboard_gateway.tb_utility.tb_loader import TBModuleLoader
from thingsboard_gateway.tb_utility.tb_utility import TBUtility

try:
    from paho.mqtt.client import Client, MQTTv5
except ImportError:
    print("paho-mqtt library not found")
    TBUtility.install_package("paho-mqtt", version=">=1.6")
    from paho.mqtt.client import Client


class GrpcMqttConnector(GwGrpcConnector):
    def __init__(self, connector_config: str, config_dir_path: str):
        super().__init__(connector_config, config_dir_path)
        self.__config = self.connection_config['config'][list(self.connection_config['config'].keys())[0]]
        self._connector_key = self.connection_config['grpc_key']
        self._connector_type = 'mqtt'
        self.name = self.__config.get("name", 'MQTT Connector ' + ''.join(choice(ascii_lowercase) for _ in range(5)))

        self.statistics = {'MessagesReceived': 0, 'MessagesSent': 0}
        self.__subscribes_sent = {}

        self.__broker = self.__config.get('broker')

        self.__mapping = []
        self.__server_side_rpc = []
        self.__connect_requests = []
        self.__disconnect_requests = []
        self.__attribute_requests = []
        self.__attribute_updates = []

        # temporary attributes
        self.__rpc_register_queue = Queue()
        self.__rpc_requests_in_progress = {}

        self._attribute_requests_queue = Queue()

        mandatory_keys = {
            "mapping": ['topicFilter', 'converter'],
            "serverSideRpc": ['deviceNameFilter', 'methodFilter', 'requestTopicExpression', 'valueExpression'],
            "connectRequests": ['topicFilter'],
            "disconnectRequests": ['topicFilter'],
            "attributeRequests": ['topicFilter', 'topicExpression', 'valueExpression'],
            "attributeUpdates": ['deviceNameFilter', 'attributeFilter', 'topicExpression', 'valueExpression']
        }

        # Mappings, i.e., telemetry/attributes-push handlers provided by user via configuration file
        self.load_handlers('mapping', mandatory_keys['mapping'], self.__mapping)

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
        client_id = self.__broker.get("clientId", ''.join(choice(ascii_lowercase) for _ in range(23)))

        self._mqtt_version = self.__broker.get('version', 5)
        try:
            self._client = Client(client_id, protocol=MQTT_VERSIONS[self._mqtt_version])
        except KeyError:
            log.error('Unknown MQTT version. Starting up on version 5...')
            self._client = Client(client_id, protocol=MQTTv5)
            self._mqtt_version = 5

        if "username" in self.__broker["security"]:
            self._client.username_pw_set(self.__broker["security"]["username"],
                                         self.__broker["security"]["password"])

        if "caCert" in self.__broker["security"] \
                or self.__broker["security"].get("type", "none").lower() == "tls":
            ca_cert = self.__broker["security"].get("caCert")
            private_key = self.__broker["security"].get("privateKey")
            cert = self.__broker["security"].get("cert")

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
                    log.error("Cannot setup connection to broker %s using SSL. "
                              "Please check your configuration.\nError: %s",
                              self.get_name(), e)
                if self.__broker["security"].get("insecure", False):
                    self._client.tls_insecure_set(True)
                else:
                    self._client.tls_insecure_set(False)

        # Set up external MQTT broker callbacks ------------------------------------------------------------------------
        self._client.on_connect = self._on_connect
        self._client.on_message = self._on_message
        self._client.on_subscribe = self._on_subscribe
        self._client.on_disconnect = self._on_disconnect
        # self._client.on_log = self._on_log

        # Set up lifecycle flags ---------------------------------------------------------------------------------------
        self._connected = False
        self.__stopped = False

        self.__msg_queue = Queue()
        self.__workers_thread_pool = []
        self.__max_msg_number_for_worker = self.__config.get('maxMessageNumberPerWorker', 10)
        self.__max_number_of_workers = self.__config.get('maxNumberOfWorkers', 100)

        self._on_message_queue = Queue()
        self._on_message_thread = Thread(name='On Message', target=self._process_on_message, daemon=True)
        self._on_message_thread.start()

    def load_handlers(self, handler_flavor, mandatory_keys, accepted_handlers_list):
        config = self.__config.get(handler_flavor, [])
        if self.__config.get("requestsMapping") is not None:
            config = self.__config["requestsMapping"].get(handler_flavor, [])
        if handler_flavor not in config:
            log.error("'%s' section missing from configuration", handler_flavor)
        else:
            for handler in config:
                discard = False

                for key in mandatory_keys:
                    if key not in handler:
                        # Will report all missing fields to user before discarding the entry => no break here
                        discard = True
                        log.error("Mandatory key '%s' missing from %s handler: %s",
                                  key, handler_flavor, dumps(handler))
                    else:
                        log.debug("Mandatory key '%s' found in %s handler: %s",
                                  key, handler_flavor, dumps(handler))

                if discard:
                    log.error("%s handler is missing some mandatory keys => rejected: %s",
                              handler_flavor, dumps(handler))
                else:
                    accepted_handlers_list.append(handler)
                    log.debug("%s handler has all mandatory keys => accepted: %s",
                              handler_flavor, dumps(handler))

            log.info("Number of accepted %s handlers: %d",
                     handler_flavor,
                     len(accepted_handlers_list))

            log.info("Number of rejected %s handlers: %d",
                     handler_flavor,
                     len(self.__config.get(handler_flavor)) - len(accepted_handlers_list))

    def is_connected(self):
        return self._connected

    def run(self):
        while not self._grpc_client.connected:
            if self.registered:
                if not self._grpc_client.connected:
                    self.registered = False
                    continue

            sleep(.2)

        try:
            self.__connect()
        except Exception as e:
            log.exception(e)
            try:
                self.close()
            except Exception as e:
                log.exception(e)

        while True:
            if self.__stopped:
                break
            elif not self._connected:
                self.__connect()
            self.__threads_manager()
            sleep(.2)

    def __connect(self):
        while not self._connected and not self.__stopped:
            try:
                self._client.connect(self.__broker['host'],
                                     self.__broker.get('port', 1883))
                self._client.loop_start()
                if not self._connected:
                    sleep(1)
            except ConnectionRefusedError as e:
                log.error(e)
                sleep(10)

    def close(self):
        self.__stopped = True
        try:
            self._client.disconnect()
        except Exception as e:
            log.exception(e)
        self._client.loop_stop()
        log.info('%s has been stopped.', self.get_name())

    def stop(self):
        super(GrpcMqttConnector, self).stop()

    def get_name(self):
        return self.name

    def __subscribe(self, topic, qos):
        message = self._client.subscribe(topic, qos)
        try:
            self.__subscribes_sent[message[1]] = topic
        except Exception as e:
            log.exception(e)

    def _on_connect(self, client, userdata, flags, result_code, *extra_params):
        if result_code == 0:
            self._connected = True
            log.info('%s connected to %s:%s - successfully.',
                     self.get_name(),
                     self.__broker["host"],
                     self.__broker.get("port", "1883"))

            log.debug("Client %s, userdata %s, flags %s, extra_params %s",
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
                        "json": "JsonGrpcMqttUplinkConverter",
                        "bytes": "BytesGrpcMqttUplinkConverter"
                    }

                    # Get converter class from "extension" parameter or default converter
                    converter_class_name = mapping["converter"].get("extension",
                                                                    default_converters.get(
                                                                        mapping['converter'].get('type')))
                    if not converter_class_name:
                        log.error('Converter type or extension class should be configured!')
                        continue

                    # Find and load required class
                    module = TBModuleLoader.import_module(self._connector_type, converter_class_name)
                    if module:
                        log.debug('Converter %s for topic %s - found!', converter_class_name,
                                  mapping["topicFilter"])
                        converter = module(mapping)
                    else:
                        log.error("Cannot find converter for %s topic", mapping["topicFilter"])
                        continue

                    # Setup regexp topic acceptance list ---------------------------------------------------------------
                    regex_topic = TBUtility.topic_to_regex(mapping["topicFilter"])

                    # There may be more than one converter per topic, so I'm using vectors
                    if not self.__mapping_sub_topics.get(regex_topic):
                        self.__mapping_sub_topics[regex_topic] = []

                    self.__mapping_sub_topics[regex_topic].append(converter)

                    # Subscribe to appropriate topic -------------------------------------------------------------------
                    self.__subscribe(mapping["topicFilter"], mapping.get("subscriptionQos", 1))

                    log.info('Connector "%s" subscribe to %s',
                             self.get_name(),
                             TBUtility.regex_to_topic(regex_topic))

                except Exception as e:
                    log.exception(e)

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
            if result_code in result_codes:
                log.error("%s connection FAIL with error %s %s!", self.get_name(), result_code,
                          result_codes[result_code])
            else:
                log.error("%s connection FAIL with unknown error!", self.get_name())

    def _on_disconnect(self, *args):
        self._connected = False
        log.debug('"%s" was disconnected. %s', self.get_name(), str(args))

    @staticmethod
    def _on_log(*args):
        log.debug(args)

    def _on_subscribe(self, _, __, mid, granted_qos, *args):
        log.info(args)
        try:
            if granted_qos[0] == 128:
                log.error('"%s" subscription failed to topic %s subscription message id = %i',
                          self.get_name(),
                          self.__subscribes_sent.get(mid), mid)
            else:
                log.info('"%s" subscription success to topic %s, subscription message id = %i',
                         self.get_name(),
                         self.__subscribes_sent.get(mid), mid)
        except Exception as e:
            log.exception(e)

        # Success or not, remove this topic from the list of pending subscription requests
        if self.__subscribes_sent.get(mid) is not None:
            del self.__subscribes_sent[mid]

    def put_data_to_convert(self, converter, message, content) -> bool:
        if not self.__msg_queue.full():
            self.__msg_queue.put((converter.convert, message.topic, content), True, 100)
            return True
        return False

    def _save_converted_msg(self, topic, data):
        basic_msg = GrpcMsgCreator.get_basic_message(None)
        telemetry = data['telemetry']
        attributes = data['attributes']
        GrpcMsgCreator.create_telemetry_connector_msg(telemetry, device_name=data['deviceName'],
                                                      basic_message=basic_msg)
        GrpcMsgCreator.create_attributes_connector_msg(attributes, device_name=data['deviceName'],
                                                       basic_message=basic_msg)
        self._grpc_client.send(basic_msg)
        self.statistics['MessagesSent'] += 1
        log.debug("Successfully converted message from topic %s", topic)

    def __threads_manager(self):
        if len(self.__workers_thread_pool) == 0:
            worker = MqttConnector.ConverterWorker("Main", self.__msg_queue, self._save_converted_msg)
            self.__workers_thread_pool.append(worker)
            worker.start()

        number_of_needed_threads = round(self.__msg_queue.qsize() / self.__max_msg_number_for_worker, 0)
        threads_count = len(self.__workers_thread_pool)
        if number_of_needed_threads > threads_count < self.__max_number_of_workers:
            thread = MqttConnector.ConverterWorker(
                "Worker " + ''.join(choice(ascii_lowercase) for _ in range(5)), self.__msg_queue,
                self._save_converted_msg)
            self.__workers_thread_pool.append(thread)
            thread.start()
        elif number_of_needed_threads < threads_count and threads_count > 1:
            worker: MqttConnector.ConverterWorker = self.__workers_thread_pool[-1]
            if not worker.in_progress:
                worker.stopped = True
                self.__workers_thread_pool.remove(worker)

    def _on_message(self, client, userdata, message):
        self._on_message_queue.put((client, userdata, message))

    def _process_on_message(self):
        while not self.__stopped:
            if not self._on_message_queue.empty():
                client, userdata, message = self._on_message_queue.get()

                self.statistics['MessagesReceived'] += 1
                content = TBUtility.decode(message)

                # Check if message topic exists in mappings "i.e., I'm posting telemetry/attributes" -------------------
                topic_handlers = [regex for regex in self.__mapping_sub_topics if fullmatch(regex, message.topic)]

                if topic_handlers:
                    # Note: every topic may be associated to one or more converter.
                    # This means that a single MQTT message may produce more than one message towards ThingsBoard.
                    # This also means that I cannot return after the first successful conversion:
                    # I got to use all the available ones.
                    # I will use a flag to understand whether at least one converter succeeded
                    request_handled = False

                    for topic in topic_handlers:
                        available_converters = self.__mapping_sub_topics[topic]
                        for converter in available_converters:
                            try:
                                if isinstance(content, list):
                                    for item in content:
                                        request_handled = self.put_data_to_convert(converter, message, item)
                                        if not request_handled:
                                            log.error(
                                                'Cannot find converter for the topic:"%s"! Client: %s, User data: %s',
                                                message.topic,
                                                str(client),
                                                str(userdata))
                                else:
                                    request_handled = self.put_data_to_convert(converter, message, content)

                            except Exception as e:
                                log.exception(e)

                    if not request_handled:
                        log.error('Cannot find converter for the topic:"%s"! Client: %s, User data: %s',
                                  message.topic,
                                  str(client),
                                  str(userdata))

                    # Note: if I'm in this branch, this was for sure a telemetry/attribute push message
                    # => Execution must end here both in case of failure and success
                    return None

                # Check if message topic exists in connection handlers "i.e., I'm connecting a device" -----------------
                topic_handlers = [regex for regex in self.__connect_requests_sub_topics if
                                  fullmatch(regex, message.topic)]

                if topic_handlers:
                    for topic in topic_handlers:
                        handler = self.__connect_requests_sub_topics[topic]

                        found_device_name = None
                        found_device_type = 'default'

                        # Get device name, either from topic or from content
                        if handler.get("deviceNameTopicExpression"):
                            device_name_match = search(handler["deviceNameTopicExpression"], message.topic)
                            if device_name_match is not None:
                                found_device_name = device_name_match.group(0)
                        elif handler.get("deviceNameJsonExpression"):
                            found_device_name = TBUtility.get_value(handler["deviceNameJsonExpression"], content)

                        # Get device type (if any), either from topic or from content
                        if handler.get("deviceTypeTopicExpression"):
                            device_type_match = search(handler["deviceTypeTopicExpression"], message.topic)
                            found_device_type = device_type_match.group(0) if device_type_match is not None else \
                                handler["deviceTypeTopicExpression"]
                        elif handler.get("deviceTypeJsonExpression"):
                            found_device_type = TBUtility.get_value(handler["deviceTypeJsonExpression"], content)

                        if found_device_name is None:
                            log.error("Device name missing from connection request")
                            continue

                        # Note: device must be added even if it is already known locally: else ThingsBoard
                        # will not send RPCs and attribute updates
                        log.info("Connecting device %s of type %s", found_device_name, found_device_type)
                        GrpcMsgCreator.create_device_connected_msg(found_device_name)

                    # Note: if I'm in this branch, this was for sure a connection message
                    # => Execution must end here both in case of failure and success
                    return None

                # Check if message topic exists in disconnection handlers "i.e., I'm disconnecting a device" -----------
                topic_handlers = [regex for regex in self.__disconnect_requests_sub_topics if
                                  fullmatch(regex, message.topic)]
                if topic_handlers:
                    for topic in topic_handlers:
                        handler = self.__disconnect_requests_sub_topics[topic]

                        found_device_name = None
                        found_device_type = 'default'

                        # Get device name, either from topic or from content
                        if handler.get("deviceNameTopicExpression"):
                            device_name_match = search(handler["deviceNameTopicExpression"], message.topic)
                            if device_name_match is not None:
                                found_device_name = device_name_match.group(0)
                        elif handler.get("deviceNameJsonExpression"):
                            found_device_name = TBUtility.get_value(handler["deviceNameJsonExpression"], content)

                        # Get device type (if any), either from topic or from content
                        if handler.get("deviceTypeTopicExpression"):
                            device_type_match = search(handler["deviceTypeTopicExpression"], message.topic)
                            if device_type_match is not None:
                                found_device_type = device_type_match.group(0)
                        elif handler.get("deviceTypeJsonExpression"):
                            found_device_type = TBUtility.get_value(handler["deviceTypeJsonExpression"], content)

                        if found_device_name is None:
                            log.error("Device name missing from disconnection request")
                            continue

                        if found_device_name in GrpcMsgCreator.create_get_connected_devices_msg(self._connector_key):
                            log.info("Disconnecting device %s of type %s", found_device_name, found_device_type)
                            GrpcMsgCreator.create_device_disconnected_msg(found_device_name)
                        else:
                            log.info("Device %s was not connected", found_device_name)

                        break

                    # Note: if I'm in this branch, this was for sure a disconnection message
                    # => Execution must end here both in case of failure and success
                    return None

                # Check if message topic exists in attribute request handlers "i.e., I'm asking for a shared attribute"-
                topic_handlers = [regex for regex in self.__attribute_requests_sub_topics if
                                  fullmatch(regex, message.topic)]
                if topic_handlers:
                    try:
                        for topic in topic_handlers:
                            handler = self.__attribute_requests_sub_topics[topic]

                            found_device_name = None
                            found_attribute_names = None

                            # Get device name, either from topic or from content
                            if handler.get("deviceNameTopicExpression"):
                                device_name_match = search(handler["deviceNameTopicExpression"], message.topic)
                                if device_name_match is not None:
                                    found_device_name = device_name_match.group(0)
                            elif handler.get("deviceNameJsonExpression"):
                                found_device_name = TBUtility.get_value(handler["deviceNameJsonExpression"], content)

                            # Get attribute name, either from topic or from content
                            if handler.get("attributeNameTopicExpression"):
                                attribute_name_match = search(handler["attributeNameTopicExpression"], message.topic)
                                if attribute_name_match is not None:
                                    found_attribute_names = attribute_name_match.group(0)
                            elif handler.get("attributeNameJsonExpression"):
                                found_attribute_names = list(filter(lambda x: x is not None,
                                                                    TBUtility.get_values(
                                                                        handler["attributeNameJsonExpression"],
                                                                        content)))

                            if found_device_name is None:
                                log.error("Device name missing from attribute request")
                                continue

                            if found_attribute_names is None:
                                log.error("Attribute name missing from attribute request")
                                continue

                            log.info("Will retrieve attribute %s of %s", found_attribute_names, found_device_name)
                            self.request_device_attributes(found_device_name, found_attribute_names, client_scope=False)
                            self._attribute_requests_queue.put(
                                (found_attribute_names, handler.get("topicExpression"), handler.get("valueExpression"),
                                 handler.get('retain', False)))

                            break

                    except Exception as e:
                        log.exception(e)

                    # Note: if I'm in this branch, this was for sure an attribute request message
                    # => Execution must end here both in case of failure and success
                    return None

                log.debug("Received message to topic \"%s\" with unknown interpreter data: \n\n\"%s\"",
                          message.topic,
                          content)

    def notify_attribute(self, content):
        try:
            incoming_data = {
                'device': content.deviceName,
                'values': content.responseMsg.sharedAttributeList
            }
            attribute_name, topic_expression, value_expression, retain = self._attribute_requests_queue.get()

            device_name = incoming_data.get("device")
            attribute_values = [item.string_v for item in incoming_data.get('values')]

            topic = topic_expression \
                .replace("${deviceName}", str(device_name)) \
                .replace("${attributeKey}", str(attribute_name))

            if len(attribute_name) <= 1:
                data = value_expression.replace("${attributeKey}", str(attribute_name[0])) \
                    .replace("${attributeValue}", str(attribute_values[0]))
            else:
                data = dumps(attribute_values)

            self._client.publish(topic, data, retain=retain).wait_for_publish()
            return
        except (AttributeError, IndexError) as e:
            log.error('Error when processing attribute response\n %s', e)
            return

    def on_attributes_update(self, content):
        # check if msg is attribute response
        if hasattr(content, 'responseMsg') and not self._attribute_requests_queue.empty():
            return self.notify_attribute(content)

        converted_content = {
            'device': content.deviceName,
            'data': {
                content.notificationMsg.sharedUpdated[0].kv.key: content.notificationMsg.sharedUpdated[0].kv.string_v
            }
        }

        if self.__attribute_updates:
            for attribute_update in self.__attribute_updates:
                if match(attribute_update["deviceNameFilter"], converted_content["device"]):
                    for attribute_key in converted_content["data"]:
                        if match(attribute_update["attributeFilter"], attribute_key):
                            try:
                                topic = attribute_update["topicExpression"] \
                                    .replace("${deviceName}", str(converted_content["device"])) \
                                    .replace("${attributeKey}", str(attribute_key)) \
                                    .replace("${attributeValue}", str(converted_content["data"][attribute_key]))
                            except KeyError as e:
                                log.exception("Cannot form topic, key %s - not found", e)
                                raise e
                            try:
                                data = attribute_update["valueExpression"] \
                                    .replace("${attributeKey}", str(attribute_key)) \
                                    .replace("${attributeValue}", str(converted_content["data"][attribute_key]))
                            except KeyError as e:
                                log.exception("Cannot form topic, key %s - not found", e)
                                raise e
                            self._client.publish(topic, data,
                                                 retain=attribute_update.get('retain', False)).wait_for_publish()
                            log.debug("Attribute Update data: %s for device %s to topic: %s", data,
                                      converted_content["device"], topic)
                        else:
                            log.error("Cannot find attributeName by filter in message with data: %s", converted_content)
                else:
                    log.error("Cannot find deviceName by filter in message with data: %s", converted_content)
        else:
            log.error("Attribute updates config not found.")

    def __process_rpc_request(self, content, rpc_config):
        # This handler seems able to handle the request
        log.info("Candidate RPC handler found")

        expects_response = rpc_config.get("responseTopicExpression")
        defines_timeout = rpc_config.get("responseTimeout")

        # 2-way RPC setup
        if expects_response and defines_timeout:
            expected_response_topic = rpc_config["responseTopicExpression"] \
                .replace("${deviceName}", str(content["device"])) \
                .replace("${methodName}", str(content['data']['method'])) \
                .replace("${requestId}", str(content["data"]["id"]))

            expected_response_topic = TBUtility.replace_params_tags(expected_response_topic, content)

            timeout = time() * 1000 + rpc_config.get("responseTimeout")

            # Start listenting on the response topic
            log.info("Subscribing to: %s", expected_response_topic)
            self.__subscribe(expected_response_topic, rpc_config.get("responseTopicQoS", 1))

            # Wait for subscription to be carried out
            sub_response_timeout = 10

            while expected_response_topic in self.__subscribes_sent.values():
                sub_response_timeout -= 1
                sleep(0.1)
                if sub_response_timeout == 0:
                    break

        elif expects_response and not defines_timeout:
            log.info("2-way RPC without timeout: treating as 1-way")

        # Actually reach out for the device
        request_topic: str = rpc_config.get("requestTopicExpression") \
            .replace("${deviceName}", str(content["device"])) \
            .replace("${methodName}", str(content['data']['method'])) \
            .replace("${requestId}", str(content["data"]["id"]))

        request_topic = TBUtility.replace_params_tags(request_topic, content)

        data_to_send_tags = TBUtility.get_values(rpc_config.get('valueExpression'), content['data'],
                                                 'params',
                                                 get_tag=True)
        data_to_send_values = TBUtility.get_values(rpc_config.get('valueExpression'), content['data'],
                                                   'params',
                                                   expression_instead_none=True)

        data_to_send = rpc_config.get('valueExpression')
        for (tag, value) in zip(data_to_send_tags, data_to_send_values):
            data_to_send = data_to_send.replace('${' + tag + '}', dumps(value))

        try:
            log.info("Publishing to: %s with data %s", request_topic, data_to_send)
            self._client.publish(request_topic, data_to_send, rpc_config.get('retain', False))

            if not expects_response or not defines_timeout:
                log.info("One-way RPC: sending ack to ThingsBoard immediately")
                self.send_rpc_reply(device=content["device"], req_id=content["data"]["id"],
                                    success=True)

            # Everything went out smoothly: RPC is served
            return
        except Exception as e:
            log.exception(e)

    def server_side_rpc_handler(self, content):
        log.info("Incoming server-side RPC: %s", content)

        converted_content = {
            'device': content.deviceName,
            'data': {
                'method': content.rpcRequestMsg.methodName,
                'params': content.rpcRequestMsg.params,
                'id': content.rpcRequestMsg.requestId
            }
        }

        rpc_method = converted_content['data']['method']

        # check if RPC method is reserved get/set
        if rpc_method == 'get' or rpc_method == 'set':
            params = {}
            for param in converted_content['data']['params'].split(';'):
                try:
                    (key, value) = param.split('=')
                except ValueError:
                    continue

                if key and value:
                    params[key] = value

            return self.__process_rpc_request(converted_content, params)
        else:
            # Check whether one of my RPC handlers can handle this request
            for rpc_config in self.__server_side_rpc:
                if search(rpc_config["deviceNameFilter"], converted_content["device"]) \
                        and search(rpc_config["methodFilter"], rpc_method) is not None:
                    return self.__process_rpc_request(converted_content, rpc_config)

            log.error("RPC not handled: %s", converted_content)

    def rpc_cancel_processing(self, topic):
        log.info("RPC canceled or terminated. Unsubscribing from %s", topic)
        self._client.unsubscribe(topic)

    def unregister_connector_callback(self):
        self.registered = False


if __name__ == '__main__':
    import sys

    connector_config = sys.argv[1]
    config_path = sys.argv[2]

    connector = GrpcMqttConnector(connector_config=connector_config, config_dir_path=config_path)
    connector.start()
