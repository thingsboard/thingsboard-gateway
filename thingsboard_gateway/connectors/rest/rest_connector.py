from time import sleep
from threading import Thread
from string import ascii_lowercase
from random import choice

from thingsboard_gateway.tb_utility.tb_utility import TBUtility

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


'''

url: http://127.0.0.1/test_device

method: POST

HEADER: 

Authorization: Basic dXNlcjpwYXNzd2Q=

BODY:

body = {
            "name": "number 2",
            "sensorModel": "0AF0CE",
            "temp": 25.8,
        }

mapping_dict = {
                "/test_device":
                    {
                        "converter":JsonRestUplinkConverter(config["converter"]),
                        "rpc":"...",
                        "attributeUpdates":"..."
                    }
                }

mapping_dict["/test_device"]["converter"].convert("/test_device", body)

#TODO: 
1 Create endpoints (with/without authorization and methods from the config)
1.1 Initialize converters for endpoints
1.2 Create a dictionary for data processing 
2 Run application on open() function on host and port from the config.
3 On receiving message: convert data and send_to_storage 


'''


class RESTConnector(Connector, Thread):
    _app = None

    def __init__(self, gateway, config, connector_type):
        super().__init__()
        self.__log = log
        self.config = config
        self._connector_type = connector_type
        self.statistics = {'MessagesReceived': 0,
                           'MessagesSent': 0}
        self.__gateway = gateway
        self.__USER_DATA = {}
        self.setName(config.get("name", 'REST Connector ' + ''.join(choice(ascii_lowercase) for _ in range(5))))

        self._connected = False
        self.__stopped = False
        self.daemon = True
        # self.endpoints = {'/api/v1/telemetry': {'function': self.telemetry_handler, 'methods': ['GET', 'POST']},
        #                   '/api/v1/attributes': {'function': self.attributes_handler, 'methods': ['GET']}}
        self._app = Flask(self.get_name())
        self._api = Api(self._app)
        self.endpoints = self.load_endpoints()
        self.load_handlers()
        # TODO create converters dict
        # TODO Implement check allowed method

    def load_endpoints(self):
        endpoints = {}
        for mapping in self.config.get("mapping"):
            converter = TBUtility.check_and_import(self._connector_type,
                                                   mapping.get("class", "JsonRESTUplinkConverter"))
            endpoints.update({mapping['endpoint']: {"config": mapping, "converter": converter}})
        return endpoints

    def load_handlers(self):
        data_handlers = {
            "basic": BasicDataHandler,
            "anonymous": AnonymousDataHandler,
        }
        for mapping in self.config.get("mapping"):
            try:
                security_type = "anonymous" if mapping.get("security") is None else mapping["security"]["type"].lower()
                if security_type != "anonymous":
                    Users.add_user(mapping['endpoint'],
                                   mapping['security']['username'],
                                   mapping['security']['password'])
                self._api.add_resource(data_handlers[security_type],
                                       mapping['endpoint'],
                                       endpoint=mapping['endpoint'],
                                       resource_class_args=(self.__gateway.send_to_storage,
                                                            self.get_name(),
                                                            self.endpoints[mapping["endpoint"]]))
            except Exception as e:
                log.error("Error on creating handlers - %s", str(e))

    def open(self):
        self.__stopped = False
        self.start()

    # TODO implement data type check
    def run(self):
        try:
            self._app.run(host=self.config["host"], port=self.config["port"])

            while not self.__stopped:
                if self.__stopped:
                    break
                else:
                    sleep(.1)
        except Exception as e:
            log.exception(e)

    def close(self):
        self.__stopped = True

    def get_name(self):
        return self.name

    def is_connected(self):
        pass

    def on_attributes_update(self, content):
        pass

    def server_side_rpc_handler(self, content):
        pass

    def load_converters(self):
        converters = {}
        for mapping in self.config.get("mapping"):
            converter = TBUtility.check_and_import(self._connector_type, mapping.get("class", "JsonRestUplinkConverter"))
            converters.update({mapping['endpoint']: converter})
        return converters


class AnonymousDataHandler(Resource):
    def __init__(self, send_to_storage, name, endpoint):
        super().__init__()
        self.send_to_storage = send_to_storage
        self.__name = name
        self.__endpoint = endpoint

    def process_data(self, request):
        if not request.json:
            abort(415)
        endpoint_config = self.__endpoint[request.endpoint]['config']
        if request.method.upper() not in [method.upper() for method in endpoint_config['HTTPMethods']]:
            abort(405)
        try:
            log.info("CONVERTER CONFIG: %r", endpoint_config['converter'])
            converter = self.__endpoint['converter'](endpoint_config['converter'])
            converted_data = converter.convert(config=endpoint_config['converter'], data=request.get_json())
            self.send_to_storage(self.__name, converted_data)
            log.info("CONVERTED_DATA: %r", converted_data)
            return "", 200
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
        x = request

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
            return "", 200
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
