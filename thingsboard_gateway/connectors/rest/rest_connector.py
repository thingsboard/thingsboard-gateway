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

import asyncio
import json
from queue import Queue
from random import choice
from re import fullmatch
from string import ascii_lowercase
from threading import Thread
from time import time, sleep
import ssl
import os

from simplejson import JSONDecodeError, dumps
import requests
from requests.auth import HTTPBasicAuth as HTTPBasicAuthRequest
from requests.exceptions import RequestException

from thingsboard_gateway.tb_utility.tb_loader import TBModuleLoader
from thingsboard_gateway.tb_utility.tb_utility import TBUtility
from thingsboard_gateway.connectors.connector import Connector
from thingsboard_gateway.tb_utility.tb_logger import init_logger

try:
    from requests import Timeout, request as regular_request
except ImportError:
    print("Requests library not found - installing...")
    TBUtility.install_package("requests")
    from requests import Timeout, request as regular_request

try:
    from aiohttp import web, BasicAuth
except ImportError:
    print('AIOHTTP library not found - installing...')
    TBUtility.install_package('aiohttp')
    from aiohttp import web, BasicAuth

requests.packages.urllib3.util.ssl_.DEFAULT_CIPHERS += ':ADH-AES128-SHA256'


class RESTConnector(Connector, Thread):
    def __init__(self, gateway, config, connector_type):
        super().__init__()
        self._default_converters = {
            "uplink": "JsonRESTUplinkConverter",
            "downlink": "JsonRESTDownlinkConverter"
        }
        self._events_type = {
            'STATISTICS_MESSAGE_RECEIVED': self.statistic_message_received,
            'STATISTICS_MESSAGE_SEND': self.statistic_message_send
        }
        self.__config = config
        self.__id = self.__config.get('id')
        self._connector_type = connector_type
        self.statistics = {'MessagesReceived': 0,
                           'MessagesSent': 0}
        self.name = config.get("name", 'REST Connector ' + ''.join(choice(ascii_lowercase) for _ in range(5)))
        self.__gateway = gateway
        self.__log = init_logger(self.__gateway, self.name, self.__config.get('logLevel', 'INFO'),
                                 enable_remote_logging=self.__config.get('enableRemoteLogging', False))
        self._default_downlink_converter = TBModuleLoader.import_module(self._connector_type,
                                                                        self._default_converters['downlink'])
        self._default_uplink_converter = TBModuleLoader.import_module(self._connector_type,
                                                                      self._default_converters['uplink'])
        self.__USER_DATA = {}
        self._loop = None
        self._app = None
        self._runner = None
        self._connected = False
        self.__stopped = False
        self.daemon = True
        self.__attribute_type = {}
        self.__rpc_requests = []
        self.__attribute_updates = []
        self.__fill_requests_from_TB()

    def load_endpoints(self):
        endpoints = {}
        for mapping in self.__config.get("mapping"):
            converter = TBModuleLoader.import_module(self._connector_type,
                                                     mapping['converter'].get("extension",
                                                                              self._default_converters["uplink"]))
            endpoints.update({mapping['endpoint']: {"config": mapping, "converter": converter}})

        # configuring Attribute Request endpoints
        if len(self.__config.get('attributeRequests', [])):
            self.__attribute_type = {
                'client': self.__gateway.tb_client.client.gw_request_client_attributes,
                'shared': self.__gateway.tb_client.client.gw_request_shared_attributes
            }

            for attr in self.__config['attributeRequests']:
                config = {
                    'type': 'attributeRequest',
                    'function': self.__attribute_type[attr['type']],
                    'config': attr,
                }
                endpoints.update({attr['endpoint']: config})

        return endpoints

    def load_handlers(self):
        data_handlers = {
            "basic": BasicDataHandler,
            "anonymous": AnonymousDataHandler,
        }
        handlers = []
        mappings = self.__config.get("mapping", []) + self.__config.get('attributeRequests', [])
        for mapping in mappings:
            try:
                security_type = "anonymous" if mapping.get("security") is None else mapping["security"]["type"].lower()
                if security_type != "anonymous":
                    Users.add_user(mapping['endpoint'],
                                   mapping['security']['username'],
                                   mapping['security']['password'])
                for http_method in mapping['HTTPMethods']:
                    handler = data_handlers[security_type](self.collect_statistic_and_send, self.get_name(),
                                                           self.get_id(), self.endpoints[mapping["endpoint"]],
                                                           self.__log, provider=self.__event_provider)
                    handlers.append(web.route(http_method, mapping['endpoint'], handler))
            except Exception as e:
                self.__log.error("Error on creating handlers - %s", str(e))
        self._app.add_routes(handlers)

    def open(self):
        self.__stopped = False
        self.start()

    async def __run_server(self):
        self.endpoints = self.load_endpoints()

        self._app = web.Application(debug=self.__config.get('debugMode', False), logger=self.__log)

        ssl_context = None
        cert = None
        key = None
        if self.__config.get('SSL', False):
            if not self.__config.get('security'):
                if not os.path.exists('domain_srv.crt'):
                    from thingsboard_gateway.connectors.rest.ssl_generator import SSLGenerator
                    n = SSLGenerator(self.__config['host'])
                    n.generate_certificate()

                cert = 'domain_srv.crt'
                key = 'domain_srv.key'
            else:
                try:
                    cert = self.__config['security']['cert']
                    key = self.__config['security']['key']
                except KeyError as e:
                    self.__log.error('Provide certificate and key path!\n %s', e)

            ssl_context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
            ssl_context.load_cert_chain(cert, key)

        self.load_handlers()
        self._runner = web.AppRunner(self._app)
        await self._runner.setup()
        site = web.TCPSite(self._runner, host=self.__config['host'], port=int(self.__config.get('port', 5000)),
                           ssl_context=ssl_context, reuse_port=True, reuse_address=True)
        await site.start()
        self.__log.info('REST connector started at %s',
                        self.__config['host'] + ':' + str(self.__config.get('port', 5000)))

    def run(self):
        self._connected = True

        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)
        self._loop.run_until_complete(self.__run_server())
        try:
            self._loop.run_forever()
        finally:
            self._loop.run_until_complete(self.stop_server())
            self._loop.close()

    async def stop_server(self):
        await self._runner.cleanup()

        tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

    def close(self):
        self.__stopped = True
        self._connected = False
        if not self._loop.is_closed():
            self._loop.call_soon_threadsafe(self._loop.stop)
        self.__log.info('REST connector stopped.')
        self.__log.stop()
        self.join()

    def get_id(self):
        return self.__id

    def get_name(self):
        return self.name

    def get_type(self):
        return self._connector_type

    def is_connected(self):
        return self._connected

    def is_stopped(self):
        return self.__stopped

    def get_config(self):
        return self.__config

    def on_attributes_update(self, content):
        try:
            for attribute_request in self.__attribute_updates:
                if fullmatch(attribute_request["deviceNameFilter"], content["device"]) and \
                        fullmatch(attribute_request["attributeFilter"], list(content["data"].keys())[0]):
                    converted_data = attribute_request["downlink_converter"].convert(attribute_request, content)
                    response_queue = Queue(1)
                    request_dict = {"config": {**attribute_request,
                                               **converted_data},
                                    "request": regular_request}
                    attribute_update_request_thread = Thread(target=self.__send_request,
                                                             args=(request_dict, response_queue, self.__log),
                                                             daemon=True,
                                                             name="Attribute request to %s" % (
                                                                 converted_data["url"]))
                    attribute_update_request_thread.start()
                    if not response_queue.empty():
                        response = response_queue.get_nowait()
                        self.__log.debug(response)
                    del response_queue

            # ONLY if initialized "response" section for endpoint
            # check if attribute update relates to some responseAttribute
            for endpoint in self.__config['mapping']:
                response_attribute = endpoint.get('response', {}).get('responseAttribute')
                if list(content['data'].keys())[0] == response_attribute:
                    BaseDataHandler.responses_queue.put(content['data'][response_attribute])
        except Exception as e:
            self.__log.exception(e)

    def server_side_rpc_handler(self, content):
        try:
            if content.get('data') is None:
                content['data'] = {'params': content['params'], 'method': content['method']}

            rpc_method = content['data']['method']

            # check if RPC type is connector RPC (can be only 'get' or 'set')
            try:
                (connector_type, rpc_method_name) = rpc_method.split('_')
                if connector_type == self._connector_type:
                    rpc_method = rpc_method_name
                    content['device'] = content['params'].split(' ')[0].split('=')[-1]
            except (IndexError, ValueError):
                pass

            # check if RPC method is reserved get/set
            if rpc_method == 'get' or rpc_method == 'set':
                device = content.get('device')
                params = {}
                for param in content['data']['params'].split(';'):
                    try:
                        (key, value) = param.split('=')
                    except ValueError:
                        continue

                    if key and value:
                        params[key] = value

                uplink_converter = self._default_uplink_converter
                downlink_converter = self._default_downlink_converter
                converted_data = downlink_converter.convert(params, content)

                request_dict = {'config': {**params, **converted_data}, 'request': regular_request,
                                'converter': uplink_converter}
                response = self.__send_request(request_dict, Queue(1), self.__log, with_queue=False)

                self.__log.debug('Response from RPC request: %s', response)
                self.__gateway.send_rpc_reply(device=device,
                                              req_id=content["data"].get('id'),
                                              content=response[2] if response and len(response) >= 3 else response)
            else:
                for rpc_request in self.__rpc_requests:
                    if fullmatch(rpc_request["deviceNameFilter"], content["device"]) and \
                            fullmatch(rpc_request["methodFilter"], rpc_method):
                        converted_data = rpc_request["downlink_converter"].convert(rpc_request, content)

                        request_dict = {"config": {**rpc_request,
                                                   **converted_data},
                                        "request": regular_request}
                        request_dict["converter"] = request_dict["config"].get("uplink_converter")

                        response = self.__send_request(request_dict, Queue(1), self.__log, with_queue=False)

                        self.__log.debug('Response from RPC request: %s', response)
                        if (content['data'].get('id') is not None) and (response is not None):
                            self.__gateway.send_rpc_reply(device=content["device"],
                                                          req_id=content["data"]["id"],
                                                          content=response[2] if response and len(response) >= 3 else response)
        except Exception as e:
            self.__log.exception(e)

    def __event_provider(self, event_type):
        try:
            self._events_type[event_type]()
        except KeyError:
            self.__log.error('Unresolved event type')

    def statistic_message_received(self):
        self.statistics["MessagesReceived"] = self.statistics["MessagesReceived"] + 1

    def statistic_message_send(self):
        self.statistics["MessagesSent"] = self.statistics["MessagesSent"] + 1

    def collect_statistic_and_send(self, connector_name, connector_id, data):
        self.statistics["MessagesReceived"] = self.statistics["MessagesReceived"] + 1
        self.__gateway.send_to_storage(connector_name, connector_id, data)
        self.statistics["MessagesSent"] = self.statistics["MessagesSent"] + 1

    def __fill_requests_from_TB(self):
        requests_from_tb = {
            "attributeUpdates": self.__attribute_updates,
            "serverSideRpc": self.__rpc_requests,
        }
        for request_section in requests_from_tb:
            for request_config_object in self.__config.get(request_section, []):

                uplink_imported_class = TBModuleLoader.import_module(self._connector_type, request_config_object.get("extension", self._default_converters["uplink"]))
                uplink_converter = uplink_imported_class(request_config_object, self.__log)

                downlink_imported_class = TBModuleLoader.import_module(self._connector_type, request_config_object.get("extension", self._default_converters["downlink"]))
                downlink_converter = downlink_imported_class(request_config_object, self.__log)

                request_dict = {**request_config_object,
                                "uplink_converter": uplink_converter,
                                "downlink_converter": downlink_converter,
                                }
                requests_from_tb[request_section].append(request_dict)
        self.__log.debug("Requests from TB: %s", requests_from_tb)
        self.__rpc_requests = requests_from_tb["serverSideRpc"]
        self.__attribute_updates = requests_from_tb["attributeUpdates"]

    def __send_request(self, request_dict, converter_queue, logger, with_queue=True):
        url = ""
        try:
            request_dict["next_time"] = time() + request_dict["config"].get("scanPeriod", 10)

            if str(request_dict["config"]["url"]).lower().startswith("http"):
                url = request_dict["config"]["url"]
            else:
                url = "http://" + request_dict["config"]["url"]

            logger.debug(url)
            security = None

            if request_dict["config"]["security"]["type"].lower() == "basic":
                security = HTTPBasicAuthRequest(request_dict["config"]["security"]["username"],
                                                request_dict["config"]["security"]["password"])

            request_timeout = request_dict["config"].get("timeout")

            data = {"data": request_dict["config"]["data"]}
            params = {
                "method": request_dict["config"].get("HTTPMethod", "GET"),
                "url": url,
                "timeout": request_timeout,
                "allow_redirects": request_dict["config"].get("allowRedirects", False),
                "verify": request_dict["config"].get("SSLVerify"),
                "auth": security,
                **data,
            }
            logger.debug(url)

            if request_dict["config"].get("httpHeaders") is not None:
                params["headers"] = request_dict["config"]["httpHeaders"]

            logger.debug("Request to %s will be sent", url)
            response = None
            data_to_storage = []
            try:
                response = request_dict["request"](**params)

            except Timeout:
                logger.error("Timeout error on request %s.", url)
                data_to_storage.append({"error": "Timeout", "code": 408})
            except RequestException as e:
                logger.error("Cannot connect to %s. Request exception.", url)
                data_to_storage.append({"error": str(e)})
                logger.debug(e)
            except ConnectionError:
                logger.error("Cannot connect to %s. Connection error.", url)
                data_to_storage.append({"error": f"Cannot connect to target url: {url}"})

            if response and response.ok:
                try:
                    data_to_storage.append(response.json())
                except UnicodeDecodeError:
                    data_to_storage.append(response.content)
                except JSONDecodeError:
                    data_to_storage.append(response.content)

                if len(data_to_storage) == 3 and with_queue and not converter_queue.full():
                    converter_queue.put(data_to_storage)
                    self.statistics["MessagesReceived"] = self.statistics["MessagesReceived"] + 1
            else:
                if response is not None:
                    logger.error("Request to URL: %s finished with code: %i. Cat information: http://http.cat/%i",
                                 url,
                                 response.status_code,
                                 response.status_code)
                    logger.debug("Response: %r", response.text)
                    data_to_storage.append({"error": response.reason, "code": response.status_code})

                if with_queue:
                    converter_queue.put(data_to_storage)

                self.statistics["MessagesReceived"] = self.statistics["MessagesReceived"] + 1

            if not with_queue:
                return data_to_storage
        except Exception as e:
            logger.exception(e)


class BaseDataHandler:
    responses_queue = Queue()
    response_attribute_request = Queue()

    def __init__(self, send_to_storage, name, id, endpoint, logger, provider=None):
        self.log = logger
        self.send_to_storage = send_to_storage
        self.connector_id = id
        self.__name = name
        self.__endpoint = endpoint
        self.__provider = provider

        self.success_response = self.__endpoint['config'].get('response', {}).get('successResponse')
        self.unsuccessful_response = self.__endpoint['config'].get('response', {}).get('unsuccessfulResponse')
        self.response_expected = self.__endpoint['config'].get('response', {}).get('responseExpected', False)

    @property
    def name(self):
        return self.__name

    @property
    def endpoint(self):
        return self.__endpoint

    @staticmethod
    async def _convert_data_from_request(request):
        if request.method == 'GET':
            params = request.query

            return dict(params)
        else:
            try:
                json_data = await request.json()
            except json.decoder.JSONDecodeError:
                data = await request.post()
                if len(data):
                    json_data = dict(data)
                else:
                    json_data = await request.text()

            return json_data

    @staticmethod
    def modify_data_for_remote_response(data, modify):
        if modify:
            data['attributes'].append({'responseExpected': True})

    def get_response(self):
        if self.response_expected:
            time_point = time()
            while not time() - time_point >= self.endpoint['config'].get('response', {}).get('timeout', 120):
                if not BaseDataHandler.responses_queue.empty():
                    response = BaseDataHandler.responses_queue.get()
                    return web.Response(body=str(response), status=200)

                sleep(.2)

            return web.Response(body=str(self.unsuccessful_response) if self.unsuccessful_response else None,
                                status=408)

        return web.Response(body=str(self.success_response) if self.success_response else None, status=200)

    def process_attribute_request(self, data):
        if self.processed_attribute_request(data):
            time_point = time()
            while not time() - time_point >= self.endpoint['config']['timeout']:
                if not BaseDataHandler.response_attribute_request.empty():
                    self.__provider('STATISTICS_MESSAGE_SEND')
                    return web.Response(body=BaseDataHandler.response_attribute_request.get())

                sleep(.2)

            return web.Response(status=408)

    @staticmethod
    def attribute_request_callback(content, _):
        BaseDataHandler.response_attribute_request.put(dumps(content))

    def processed_attribute_request(self, data):
        if self.__endpoint.get('type') == 'attributeRequest':
            device_name_tags = TBUtility.get_values(self.__endpoint['config'].get("deviceNameExpression"), data,
                                                    get_tag=True)
            device_name_values = TBUtility.get_values(self.__endpoint['config'].get("deviceNameExpression"), data,
                                                      expression_instead_none=True)

            device_name = self.__endpoint['config'].get("deviceNameExpression")
            for (device_name_tag, device_name_value) in zip(device_name_tags, device_name_values):
                is_valid_key = "${" in self.__endpoint['config'].get("deviceNameExpression") and "}" in \
                               self.__endpoint['config'].get("deviceNameExpression")
                device_name = device_name.replace('${' + str(device_name_tag) + '}',
                                                  str(device_name_value)) \
                    if is_valid_key else device_name_tag

            if not device_name or device_name == '':
                return False

            found_attribute_names = list(filter(lambda x: x is not None,
                                                TBUtility.get_values(
                                                    self.__endpoint['config'].get("attributeNameExpression"),
                                                    data)))

            if found_attribute_names is None:
                return False

            self.__endpoint['function'](device_name, found_attribute_names, self.attribute_request_callback)
            self.__provider('STATISTICS_MESSAGE_RECEIVED')
            return True

        return False


class AnonymousDataHandler(BaseDataHandler):
    async def __call__(self, request: web.Request):
        json_data = await self._convert_data_from_request(request)

        if not json_data and not len(request.query):
            return web.Response(body=str(self.unsuccessful_response) if self.unsuccessful_response else None,
                                status=415)

        endpoint_config = self.endpoint['config']
        if request.method.upper() not in [method.upper() for method in endpoint_config['HTTPMethods']]:
            return web.Response(body=str(self.unsuccessful_response) if self.unsuccessful_response else None,
                                status=405)

        json_data.update(dict(request.query))
        data = json_data

        # check if request is Attribute Request type
        result = self.process_attribute_request(data)
        if isinstance(result, web.Response):
            return result

        try:
            self.log.info("CONVERTER CONFIG: %r", endpoint_config['converter'])
            converter = self.endpoint['converter'](endpoint_config['converter'], self.log)
            converted_data = converter.convert(config=endpoint_config['converter'], data=data)

            self.modify_data_for_remote_response(converted_data, self.response_expected)

            self.send_to_storage(self.name, self.connector_id, converted_data)
            self.log.info("CONVERTED_DATA: %r", converted_data)

            return self.get_response()
        except Exception as e:
            self.log.exception("Error while post to anonymous handler: %s", e)
            return web.Response(body=str(self.success_response) if self.success_response else None, status=500)


class BasicDataHandler(BaseDataHandler):
    def verify(self, username, password):
        if not username and password:
            return False
        return Users.validate_user_credentials(self.endpoint['config']['endpoint'], username, password)

    async def __call__(self, request: web.Request):
        if request.headers.get('Authorization') is None:
            return web.Response(body=str(self.unsuccessful_response) if self.unsuccessful_response else None,
                                status=401)

        auth = BasicAuth.decode(request.headers['Authorization'])
        if self.verify(auth.login, auth.password):
            json_data = await self._convert_data_from_request(request)

            if not json_data:
                return web.Response(body=str(self.unsuccessful_response) if self.unsuccessful_response else None,
                                    status=415)

            endpoint_config = self.endpoint['config']

            if request.method.upper() not in [method.upper() for method in endpoint_config['HTTPMethods']]:
                return web.Response(body=str(self.unsuccessful_response) if self.unsuccessful_response else None,
                                    status=405)

            json_data.update(dict(request.query))
            data = json_data

            # check if request is Attribute Request type
            result = self.process_attribute_request(data)
            if isinstance(result, web.Response):
                return result

            try:
                self.log.info("CONVERTER CONFIG: %r", endpoint_config['converter'])
                converter = self.endpoint['converter'](endpoint_config['converter'], self.log)
                converted_data = converter.convert(config=endpoint_config['converter'], data=data)

                self.modify_data_for_remote_response(converted_data, self.response_expected)

                self.send_to_storage(self.name, self.connector_id, converted_data)
                self.log.info("CONVERTED_DATA: %r", converted_data)

                return self.get_response()
            except Exception as e:
                self.log.exception("Error while post to basic handler: %s", e)
                return web.Response(body=str(self.unsuccessful_response) if self.unsuccessful_response else None,
                                    status=500)

        return web.Response(body=str(self.unsuccessful_response) if self.unsuccessful_response else None, status=401)


class Users:
    USER_DATA = {}

    @classmethod
    def add_user(cls, endpoint, username, password):
        cls.USER_DATA.update({endpoint: {username: password}})

    @classmethod
    def validate_user_credentials(cls, endpoint, username, password):
        result = False
        if cls.USER_DATA.get(endpoint) is not None and cls.USER_DATA[endpoint].get(username) == password:
            result = True
        return result
