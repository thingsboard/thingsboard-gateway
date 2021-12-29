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

from queue import Queue
from random import choice
from re import fullmatch
from string import ascii_lowercase
from threading import Thread
from time import time
import ssl
import os

from simplejson import JSONDecodeError
import requests
from requests.auth import HTTPBasicAuth as HTTPBasicAuthRequest
from requests.exceptions import RequestException

from thingsboard_gateway.tb_utility.tb_loader import TBModuleLoader
from thingsboard_gateway.tb_utility.tb_utility import TBUtility
from thingsboard_gateway.connectors.connector import Connector, log

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
        self.__log = log
        self._default_converters = {
            "uplink": "JsonRESTUplinkConverter",
            "downlink": "JsonRESTDownlinkConverter"
        }
        self.__config = config
        self._connector_type = connector_type
        self.statistics = {'MessagesReceived': 0,
                           'MessagesSent': 0}
        self.__gateway = gateway
        self.__USER_DATA = {}
        self.setName(config.get("name", 'REST Connector ' + ''.join(choice(ascii_lowercase) for _ in range(5))))
        self._app = None
        self._connected = False
        self.__stopped = False
        self.daemon = True
        self.__rpc_requests = []
        self.__attribute_updates = []
        self.__fill_requests_from_TB()

    def load_endpoints(self):
        endpoints = {}
        for mapping in self.__config.get("mapping"):
            converter = TBModuleLoader.import_module(self._connector_type,
                                                     mapping.get("extension", self._default_converters["uplink"]))
            endpoints.update({mapping['endpoint']: {"config": mapping, "converter": converter}})
        return endpoints

    def load_handlers(self):
        data_handlers = {
            "basic": BasicDataHandler,
            "anonymous": AnonymousDataHandler,
        }
        handlers = []
        for mapping in self.__config.get("mapping"):
            try:
                security_type = "anonymous" if mapping.get("security") is None else mapping["security"]["type"].lower()
                if security_type != "anonymous":
                    Users.add_user(mapping['endpoint'],
                                   mapping['security']['username'],
                                   mapping['security']['password'])
                for http_method in mapping['HTTPMethods']:
                    handler = data_handlers[security_type](self.collect_statistic_and_send, self.get_name(),
                                                           self.endpoints[mapping["endpoint"]])
                    handlers.append(web.route(http_method, mapping['endpoint'], handler))
            except Exception as e:
                log.error("Error on creating handlers - %s", str(e))
        self._app.add_routes(handlers)

    def open(self):
        self.__stopped = False
        self.start()

    def __run_server(self):
        self.endpoints = self.load_endpoints()

        self._app = web.Application(debug=self.__config.get('debugMode', False))

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
                    log.exception(e)
                    log.error('Provide certificate and key path!')

            ssl_context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
            ssl_context.load_cert_chain(cert, key)

        self.load_handlers()
        web.run_app(self._app, host=self.__config['host'], port=self.__config['port'], handle_signals=False,
                    ssl_context=ssl_context, reuse_port=self.__config['port'], reuse_address=self.__config['host'])

    def run(self):
        self._connected = True

        try:
            self.__run_server()
        except Exception as e:
            log.exception(e)

    def close(self):
        self.__stopped = True
        self._connected = False

    def get_name(self):
        return self.name

    def is_connected(self):
        return self._connected

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
                                                             args=(request_dict, response_queue, log),
                                                             daemon=True,
                                                             name="Attribute request to %s" % (
                                                                 converted_data["url"]))
                    attribute_update_request_thread.start()
                    if not response_queue.empty():
                        response = response_queue.get_nowait()
                        log.debug(response)
                    del response_queue
        except Exception as e:
            log.exception(e)

    def server_side_rpc_handler(self, content):
        try:
            for rpc_request in self.__rpc_requests:
                if fullmatch(rpc_request["deviceNameFilter"], content["device"]) and \
                        fullmatch(rpc_request["methodFilter"], content["data"]["method"]):
                    converted_data = rpc_request["downlink_converter"].convert(rpc_request, content)

                    request_dict = {"config": {**rpc_request,
                                               **converted_data},
                                    "request": regular_request}
                    request_dict["converter"] = request_dict["config"].get("uplink_converter")

                    response = self.__send_request(request_dict, Queue(1), log, with_queue=False)

                    log.debug('Response from RPC request: %s', response)
                    self.__gateway.send_rpc_reply(device=content["device"],
                                                  req_id=content["data"]["id"],
                                                  content=response[2] if response and len(response) >= 3 else response)
        except Exception as e:
            log.exception(e)

    def collect_statistic_and_send(self, connector_name, data):
        self.statistics["MessagesReceived"] = self.statistics["MessagesReceived"] + 1
        self.__gateway.send_to_storage(connector_name, data)
        self.statistics["MessagesSent"] = self.statistics["MessagesSent"] + 1

    def __fill_requests_from_TB(self):
        requests_from_tb = {
            "attributeUpdates": self.__attribute_updates,
            "serverSideRpc": self.__rpc_requests,
        }
        for request_section in requests_from_tb:
            for request_config_object in self.__config.get(request_section, []):
                uplink_converter = TBModuleLoader.import_module(self._connector_type,
                                                                request_config_object.get("extension",
                                                                                          self._default_converters[
                                                                                              "uplink"]))(
                    request_config_object)
                downlink_converter = TBModuleLoader.import_module(self._connector_type,
                                                                  request_config_object.get("extension",
                                                                                            self._default_converters[
                                                                                                "downlink"]))(
                    request_config_object)
                request_dict = {**request_config_object,
                                "uplink_converter": uplink_converter,
                                "downlink_converter": downlink_converter,
                                }
                requests_from_tb[request_section].append(request_dict)

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
            response = request_dict["request"](**params)
            data_to_storage = [url, request_dict["config"]["uplink_converter"]]

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

        except Timeout:
            logger.error("Timeout error on request %s.", url)
        except RequestException as e:
            logger.error("Cannot connect to %s. Connection error.", url)
            logger.debug(e)
        except ConnectionError:
            logger.error("Cannot connect to %s. Connection error.", url)
        except Exception as e:
            logger.exception(e)


class AnonymousDataHandler:
    def __init__(self, send_to_storage, name, endpoint):
        self.send_to_storage = send_to_storage
        self.__name = name
        self.__endpoint = endpoint

    async def __call__(self, request: web.Request):
        json_data = await request.json()
        if not json_data and not len(request.query):
            return web.Response(status=415)
        endpoint_config = self.__endpoint['config']
        if request.method.upper() not in [method.upper() for method in endpoint_config['HTTPMethods']]:
            return web.Response(status=405)
        try:
            log.info("CONVERTER CONFIG: %r", endpoint_config['converter'])
            converter = self.__endpoint['converter'](endpoint_config['converter'])
            data = json_data if json_data else dict(request.query)
            converted_data = converter.convert(config=endpoint_config['converter'], data=data)
            self.send_to_storage(self.__name, converted_data)
            log.info("CONVERTED_DATA: %r", converted_data)
            return web.Response(status=200)
        except Exception as e:
            log.exception("Error while post to anonymous handler: %s", e)
            return web.Response(status=500)


class BasicDataHandler:
    def __init__(self, send_to_storage, name, endpoint):
        self.send_to_storage = send_to_storage
        self.__name = name
        self.__endpoint = endpoint

    def verify(self, username, password):
        if not username and password:
            return False
        return Users.validate_user_credentials(self.__endpoint['config']['endpoint'], username, password)

    async def __call__(self, request: web.Request):
        auth = BasicAuth.decode(request.headers.get('Authorization'))
        if self.verify(auth.login, auth.password):
            json_data = await request.json()
            if not json_data:
                return web.Response(status=415)
            endpoint_config = self.__endpoint['config']
            if request.method.upper() not in [method.upper() for method in endpoint_config['HTTPMethods']]:
                return web.Response(status=405)
            try:
                log.info("CONVERTER CONFIG: %r", endpoint_config['converter'])
                converter = self.__endpoint['converter'](endpoint_config['converter'])
                converted_data = converter.convert(config=endpoint_config['converter'], data=json_data)
                self.send_to_storage(self.__name, converted_data)
                log.info("CONVERTED_DATA: %r", converted_data)
                return web.Response(status=200)
            except Exception as e:
                log.exception("Error while post to basic handler: %s", e)
                return web.Response(status=500)

        return web.Response(status=401)


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
