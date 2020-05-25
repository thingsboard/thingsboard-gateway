from time import sleep
from threading import Thread
from string import ascii_lowercase
from random import choice
from time import time
from re import fullmatch
from queue import Queue

from thingsboard_gateway.tb_utility.tb_utility import TBUtility


try:
    from requests import Timeout, request
except ImportError:
    print("Requests library not found - installing...")
    TBUtility.install_package("requests")
    from requests import Timeout, request
import requests
from requests.auth import HTTPBasicAuth
from requests.exceptions import RequestException
requests.packages.urllib3.util.ssl_.DEFAULT_CIPHERS += ':ADH-AES128-SHA256'

try:
    from flask import Flask, jsonify, request
except ImportError:
    print("Flask library not found - installing...")
    TBUtility.install_package("flask")
    from flask import Flask, jsonify, request
try:
    from flask_restful import reqparse, abort, Api, Resource
except ImportError:
    print("RESTFUL flask library not found - installing...")
    TBUtility.install_package("Flask-restful")
    from flask_restful import reqparse, abort, Api, Resource
try:
    from flask_httpauth import HTTPBasicAuth
except ImportError:
    print("HTTPAuth flask library not found - installing...")
    TBUtility.install_package("Flask-httpauth")
    from flask_httpauth import HTTPBasicAuth
try:
    from werkzeug.security import generate_password_hash, check_password_hash
except ImportError:
    print("Werkzeug flask library not found - installing...")
    TBUtility.install_package("werkzeug")
    from werkzeug.security import generate_password_hash, check_password_hash

from thingsboard_gateway.connectors.connector import Connector, log


class RESTConnector(Connector, Thread):
    _app = None

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

        self._connected = False
        self.__stopped = False
        self.daemon = True
        self._app = Flask(self.get_name())
        self._api = Api(self._app)
        self.__rpc_requests = []
        self.__fill_rpc_requests()
        self.__attribute_updates = []
        self.__fill_attribute_updates()
        self.endpoints = self.load_endpoints()
        self.load_handlers()

    def load_endpoints(self):
        endpoints = {}
        for mapping in self.__config.get("mapping"):
            converter = TBUtility.check_and_import(self._connector_type,
                                                   mapping.get("class", self._default_converters["uplink"]))
            endpoints.update({mapping['endpoint']: {"config": mapping, "converter": converter}})
        return endpoints

    def load_handlers(self):
        data_handlers = {
            "basic": BasicDataHandler,
            "anonymous": AnonymousDataHandler,
        }
        for mapping in self.__config.get("mapping"):
            try:
                security_type = "anonymous" if mapping.get("security") is None else mapping["security"]["type"].lower()
                if security_type != "anonymous":
                    Users.add_user(mapping['endpoint'],
                                   mapping['security']['username'],
                                   mapping['security']['password'])
                self._api.add_resource(data_handlers[security_type],
                                       mapping['endpoint'],
                                       endpoint=mapping['endpoint'],
                                       resource_class_args=(self.collect_statistic_and_send,
                                                            self.get_name(),
                                                            self.endpoints[mapping["endpoint"]]))
            except Exception as e:
                log.error("Error on creating handlers - %s", str(e))

    def open(self):
        self.__stopped = False
        self.start()

    def run(self):
        self._connected = True
        try:
            self._app.run(host=self.__config["host"], port=self.__config["port"])

            while not self.__stopped:
                if self.__stopped:
                    break
                else:
                    sleep(.1)
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
                if fullmatch(attribute_request["deviceNameFilter"], content["device"]) and fullmatch(attribute_request["attributeFilter"], list(content["data"].keys())[0]):
                    converted_data = attribute_request["converter"].convert(attribute_request, content)
                    response_queue = Queue(1)
                    request_dict = {"config": {**attribute_request,
                                               **converted_data},
                                    "request": request}
                    attribute_update_request_thread = Thread(target=self.__send_request,
                                                             args=(request_dict, response_queue, log),
                                                             daemon=True,
                                                             name="Attribute request to %s" % (converted_data["url"]))
                    attribute_update_request_thread.start()
                    attribute_update_request_thread.join()
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
                    converted_data = rpc_request["converter"].convert(rpc_request, content)
                    response_queue = Queue(1)
                    request_dict = {"config": {**rpc_request,
                                               **converted_data},
                                    "request": request}
                    request_dict["config"].get("uplink_converter")
                    rpc_request_thread = Thread(target=self.__send_request,
                                                args=(request_dict, response_queue, log),
                                                daemon=True,
                                                name="RPC request to %s" % (converted_data["url"]))
                    rpc_request_thread.start()
                    rpc_request_thread.join()
                    if not response_queue.empty():
                        response = response_queue.get_nowait()
                        log.debug(response)
                        self.__gateway.send_rpc_reply(device=content["device"], req_id=content["data"]["id"], content=response[2])
                    self.__gateway.send_rpc_reply(success_sent=True)

                    del response_queue
        except Exception as e:
            log.exception(e)

    def collect_statistic_and_send(self, connector_name, data):
        self.statistics["MessagesReceived"] = self.statistics["MessagesReceived"] + 1
        self.__gateway.send_to_storage(connector_name, data)
        self.statistics["MessagesSent"] = self.statistics["MessagesSent"] + 1

    def __fill_attribute_updates(self):
        for attribute_request in self.__config.get("attributeUpdates", []):
            converter = TBUtility.check_and_import(self._connector_type,
                                                   attribute_request.get("class", self._default_converters["downlink"]))(attribute_request)
            attribute_request_dict = {**attribute_request, "converter": converter}
            self.__attribute_updates.append(attribute_request_dict)

    def __fill_rpc_requests(self):
        for rpc_request in self.__config.get("serverSideRpc", []):
            converter = TBUtility.check_and_import(self._connector_type,
                                                   rpc_request.get("class", self._default_converters["downlink"]))
            rpc_request_dict = {**rpc_request, "converter": converter}
            self.__rpc_requests.append(rpc_request_dict)

    def __send_request(self, request, converter_queue, logger):
        url = ""
        try:
            request["next_time"] = time() + request["config"].get("scanPeriod", 10)
            if str(request["config"]["url"]).lower().startswith("http"):
                url = request["config"]["url"]
            else:
                url = "http://" + request["config"]["url"]
            logger.debug(url)
            security = None
            if request["config"]["security"]["type"].lower() == "basic":
                security = HTTPBasicAuth(request["config"]["security"]["username"],
                                         request["config"]["security"]["password"])
            request_timeout = request["config"].get("timeout", 1)
            params = {
                "method": request["config"].get("httpMethod", "GET"),
                "url": url,
                "timeout": request_timeout,
                "allow_redirects": request["config"].get("allowRedirects", False),
                "verify": request["config"].get("SSLVerify"),
                "auth": security
            }
            logger.debug(url)
            if request["config"].get("httpHeaders") is not None:
                params["headers"] = request["config"]["httpHeaders"]
            logger.debug("Request to %s will be sent", url)
            response = request["request"](**params)
            if response and response.ok:
                if not converter_queue.full():
                    data_to_storage = [url, request["converter"]]
                    try:
                        data_to_storage.append(response.json())
                    except UnicodeDecodeError:
                        data_to_storage.append(response.content())
                    if len(data_to_storage) == 3:
                        converter_queue.put(data_to_storage)
                        self.statistics["MessagesReceived"] = self.statistics["MessagesReceived"] + 1
            else:
                logger.error("Request to URL: %s finished with code: %i", url, response.status_code)
        except Timeout:
            logger.error("Timeout error on request %s.", url)
        except RequestException as e:
            logger.error("Cannot connect to %s. Connection error.", url)
            logger.debug(e)
        except ConnectionError:
            logger.error("Cannot connect to %s. Connection error.", url)
        except Exception as e:
            logger.exception(e)


class AnonymousDataHandler(Resource):
    def __init__(self, send_to_storage, name, endpoint):
        super().__init__()
        self.send_to_storage = send_to_storage
        self.__name = name
        self.__endpoint = endpoint

    def process_data(self, request):
        if not request.json:
            abort(415)
        endpoint_config = self.__endpoint['config']
        if request.method.upper() not in [method.upper() for method in endpoint_config['HTTPMethods']]:
            abort(405)
        try:
            log.info("CONVERTER CONFIG: %r", endpoint_config['converter'])
            converter = self.__endpoint['converter'](endpoint_config['converter'])
            converted_data = converter.convert(config=endpoint_config['converter'], data=request.get_json())
            self.send_to_storage(self.__name, converted_data)
            log.info("CONVERTED_DATA: %r", converted_data)
            return "OK", 200
        except Exception as e:
            log.exception("Error while post to anonymous handler: %s", e)
            return "", 500

    def get(self):
        return self.process_data(request)

    def post(self):
        return self.process_data(request)

    def put(self):
        return self.process_data(request)

    def update(self):
        return self.process_data(request)

    def delete(self):
        return self.process_data(request)

class BasicDataHandler(Resource):

    auth = HTTPBasicAuth()

    def __init__(self, send_to_storage, name, endpoint):
        super().__init__()
        self.send_to_storage = send_to_storage
        self.__name = name
        self.__endpoint = endpoint

    @staticmethod
    @auth.verify_password
    def verify(username, password):
        if not username and password:
            return False
        return Users.validate_user_credentials(request.endpoint, username, password)

    def process_data(self, request):
        if not request.json:
            abort(415)
        endpoint_config = self.__endpoint['config']
        if request.method.upper() not in [method.upper() for method in endpoint_config['HTTPMethods']]:
            abort(405)
        try:
            log.info("CONVERTER CONFIG: %r", endpoint_config['converter'])
            converter = self.__endpoint['converter'](endpoint_config['converter'])
            converted_data = converter.convert(config=endpoint_config['converter'], data=request.get_json())
            self.send_to_storage(self.__name, converted_data)
            log.info("CONVERTED_DATA: %r", converted_data)
            return "OK", 200
        except Exception as e:
            log.exception("Error while post to basic handler: %s", e)
            return "", 500

    @auth.login_required
    def get(self):
        return self.process_data(request)

    @auth.login_required
    def post(self):
        return self.process_data(request)

    @auth.login_required
    def put(self):
        return self.process_data(request)

    @auth.login_required
    def update(self):
        return self.process_data(request)

    @auth.login_required
    def delete(self):
        return self.process_data(request)


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
