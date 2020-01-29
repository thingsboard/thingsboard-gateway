from thingsboard_gateway.connectors.connector import Connector, log
from thingsboard_gateway.connectors.mqtt.json_mqtt_uplink_converter import JsonMqttUplinkConverter
from threading import Thread
from random import choice
from string import ascii_lowercase
from time import sleep
from flask import Flask
from thingsboard_gateway.tb_utility.tb_utility import TBUtility


class RESTConnector(Connector, Thread):
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
        self.endpoints = {'/telemetry': {'function': self.telemetry_handler, 'methods': ['GET']},
                          '/attributes': {'function': self.attributes_handler, 'methods': ['GET']}}
        self._app = Flask(self.get_name())
        self.add_endpoints()

    def add_endpoints(self):
        try:
            for endpoint in self.endpoints.keys():
                self._app.add_url_rule(rule=endpoint, view_func=self.endpoints[endpoint]['function'],
                                       methods=self.endpoints[endpoint]['methods'])
        except Exception as e:
            log.exception(e)

    def telemetry_handler(self):
        log.debug('Telemetry handler WORKS')
        return '<h1> Telemetry handler WORKS</h1>'

    def attributes_handler(self):
        log.debug('Test1 handler WORKS')
        return '<h1> Test1 handler WORKS</h1>'

    def open(self):
        self.__stopped = False
        self.start()

    def run(self):
        #
        try:
            self._app.run(host=self.config["host"], port="8082")

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


