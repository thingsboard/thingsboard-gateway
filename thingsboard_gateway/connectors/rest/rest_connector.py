from thingsboard_gateway.connectors.connector import Connector, log
from thingsboard_gateway.connectors.mqtt.json_mqtt_uplink_converter import JsonMqttUplinkConverter
from threading import Thread
from random import choice
from string import ascii_lowercase
from time import sleep
from flask import Flask, jsonify, request
from flask_restful import reqparse, abort, Api, Resource
from thingsboard_gateway.tb_utility.tb_utility import TBUtility
from thingsboard_gateway.connectors.rest.rest_converter import RestConverter

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


class HttpConnector(Connector, Thread):
    _app = None

    def __init__(self, gateway, config, connector_type):
        super().__init__()
        self.__log = log
        self.config = config
        self.__connector_type = connector_type
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
        self.add_endpoints()

    class TelemetryHandler(Resource):
        def __init__(self, send_to_storage, name, config):
            super().__init__()
            self.send_to_storage = send_to_storage
            self.__name = name
            self.__config = config

        def get(self):
            log.debug('attrs get works')
            return {'test': 'get'}

        def post(self):
            try:
                converter = RestConverter(config=self.__config)
                converted_data = converter.convert(config=self.__config, data=request.get_json())
                self.send_to_storage(self.__name, converted_data)
                return {'you sent this': 'data'}
            except Exception as e:
                log.debug(e)

    # class AttributesHandler(Resource):
    #     def __init__(self, send_to_storage, name, config):
    #         super().__init__()
    #         self.send_to_storage = send_to_storage
    #         self.__name = name
    #         self.__config = config
    #
    #     def get(self):
    #         log.debug('attrs get works')
    #         return {'test': 'get'}
    #
    #     def post(self):
    #         try:
    #             converter = RestConverter(config=self.__config)
    #             converted_data = converter.convert(config=self.__config, data=request.get_json())
    #             self.send_to_storage(self.__name, converted_data)
    #             return {'you sent this': 'data'}
    #         except Exception as e:
    #             log.debug(e)

    def add_endpoints(self):
        for mapping in self.config.get("mappings"):
            print(mapping)
            self._api.add_resource(self.TelemetryHandler, mapping['endpoint'],
                                   resource_class_args=(self.__gateway.send_to_storage, self.get_name(), mapping))
        # self._api.add_resource(self.TelemetryHandler, '/api/v1/telemetry',
        #                        resource_class_args=(self.__gateway.send_to_storage, self.get_name(), self.config))
        # self._api.add_resource(self.AttributesHandler, '/api/v1/attributes',
        #                        resource_class_args=(self.__gateway.send_to_storage, self.get_name(), self.config))
        # try:
        #     for endpoint in self.endpoints.keys():
        #         self._app.add_url_rule(rule=endpoint, view_func=self.endpoints[endpoint]['function'],
        #                                methods=self.endpoints[endpoint]['methods'])
        # except Exception as e:
        #     log.exception(e)

    # def telemetry_handler(self):
    #     log.debug('Telemetry handler WORKS')
    #     return '<h1> Telemetry handler WORKS</h1>'
    #
    # def attributes_handler(self):
    #     log.debug('Attributes handler WORKS')
    #     return '<h1> Attributes handler WORKS</h1>'

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
