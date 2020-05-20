from thingsboard_gateway.connectors.connector import Connector, log
from thingsboard_gateway.connectors.mqtt.json_mqtt_uplink_converter import JsonMqttUplinkConverter
from threading import Thread
from random import choice
from string import ascii_lowercase
from time import sleep


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
    TBUtility.install_package("flask_restful")
    from flask_restful import reqparse, abort, Api, Resource

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

    class BasicDataHandler(Resource):
        def __init__(self, send_to_storage, name, endpoints):
            super().__init__()
            self.send_to_storage = send_to_storage
            self.__name = name
            self.__endpoints = endpoints

        def get(self):
            log.debug('attrs get works')
            return {'test': 'get'}

        def post(self):
            print(request.get_json())
            print(request.endpoint)
            # try:
            #     converter = RestConverter(config=self.__config)
            #     converted_data = converter.convert(config=self.__config, data=request.get_json())
            #     self.send_to_storage(self.__name, converted_data)
            #     return {'you sent this': 'data'}
            # except Exception as e:
            #     log.debug(e)

    class AnonymousDataHandler(Resource):
        def __init__(self, send_to_storage, name, endpoints):
            super().__init__()
            self.send_to_storage = send_to_storage
            self.__name = name
            self.__endpoints = endpoints

        def get(self):
            log.debug('attrs get works')
            return {'test': 'get'}

        def post(self):
            try:
                log.info("CONVERTER CONFIG: %r", self.__endpoints[request.endpoint]['config']['converter'])
                converter = self.__endpoints[request.endpoint]['converter'](self.__endpoints[request.endpoint]['config']['converter'])
                converted_data = converter.convert(config=self.__endpoints[request.endpoint]['config']['converter'], data=request.get_json())
                log.info("CONVERTED_DATA: %r", converted_data)
            except Exception as e:
                log.error("Error while post to anonymous handler: %s", e)

    def load_endpoints(self):
        endpoints = {}
        for mapping in self.config.get("mappings"):
            converter = TBUtility.check_and_import(self._connector_type,
                                                   mapping.get("class", "JsonRESTUplinkConverter"))
            endpoints.update({mapping['endpoint']: {"config": mapping, "converter": converter}})
        return endpoints

    def load_handlers(self):
        for mapping in self.config.get("mappings"):
            if mapping.get("security")["type"] == "basic":
                self._api.add_resource(self.BasicDataHandler, mapping['endpoint'], endpoint=mapping['endpoint'],
                                       resource_class_args=(
                                       self.__gateway.send_to_storage, self.get_name(), self.endpoints))
            elif mapping.get("security")["type"] == "anonymous":
                self._api.add_resource(self.AnonymousDataHandler, mapping['endpoint'], endpoint=mapping['endpoint'],
                                       resource_class_args=(self.__gateway.send_to_storage, self.get_name(), self.endpoints))

    def open(self):
        self.__stopped = False
        self.start()

    # TODO implement data type check
    def run(self):
        #
        try:
            self._app.run(host=self.config["host"], port=self.config["port"])

            while True:
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
        for mapping in self.config.get("mappings"):
            converter = TBUtility.check_and_import(self._connector_type, mapping.get("class", "JsonRestUplinkConverter"))
            converters.update({mapping['endpoint']: converter})
        #print(converters)
        return converters
