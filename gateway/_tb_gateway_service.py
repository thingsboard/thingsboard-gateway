import logging
import time
import yaml
from json import load, loads, dumps

from gateway.tb_client import TBClient
from tb_utility.tb_utility import TBUtility
from threading import Thread
from connectors.mqtt.mqtt_connector import MqttConnector
from storage.memory_event_storage import MemoryEventStorage


log = logging.getLogger(__name__)


class TBGatewayService:
    def __init__(self, config_file):
        with open(config_file) as config:
            config = yaml.safe_load(config)
            self.available_connectors = {}
            self.__connected_devices = {}
            self.__connector_incoming_messages = {}
            self.__Send_Thread = Thread(target=self.__send_data_from_storage, daemon=True)
            if config["storage"]["type"] == "memory":
                self.__event_storage = MemoryEventStorage(config["storage"])
            else:
                pass
            self.tb_client = TBClient(config["thingsboard-client"])
            self.tb_client.connect()

            self.__load_connectors(config)
            self.__connect_with_connectors()
            self.__TEST_PUT_FOR__Send_Thread.start()
            self.__Send_Thread.start()

            while True:
                time.sleep(.1)

    def __load_connectors(self, config):
        self._connectors_configs = {}
        for connector in config['connectors']:
            try:
                with open('config/'+connector['configuration'],'r') as conf_file:
                    connector_conf = load(conf_file)
                    if not self._connectors_configs.get(connector['type']):
                        self._connectors_configs[connector['type']] = []
                    self._connectors_configs[connector['type']].append({connector['configuration']: connector_conf})
            except Exception as e:
                log.error(e)

    def __connect_with_connectors(self):
        for type in self._connectors_configs:
            if type == "mqtt":
                for connector_config in self._connectors_configs[type]:
                    log.debug(connector_config)
                    for config_file in connector_config:
                        try:
                            connector = MqttConnector(self, connector_config[config_file])
                            self.available_connectors[connector.getName()] = connector
                            connector.open()
                        except Exception as e:
                            log.error(e)

    def _send_to_storage(self, connector_name, data):
        if not TBUtility.validate_converted_data(data):
            log.error("Data from %s connector is invalid.", connector_name)
            return
        if not self.__connector_incoming_messages.get(connector_name):
            self.__connector_incoming_messages[connector_name] = 0
        else:
            self.__connector_incoming_messages[connector_name] += 1
        json_data = dumps(data)
        save_result = self.__event_storage.put(json_data)
        if save_result:
            log.debug('Connector "%s" - Saved information - %s', connector_name, json_data)
        else:
            log.error('Data from connector "%s" cannot be saved.')

    def __send_data_from_storage(self):
        while True:
            events = self.__event_storage.get_event_pack()
            if events:
                for event in events:
                    current_event = loads(event)
                    if current_event["deviceName"] not in self.__connected_devices:
                        self.tb_client._client.gw_connect_device(current_event["deviceName"]).wait_for_publish()
                    self.__connected_devices[current_event["deviceName"]] = current_event["deviceName"]
                    if current_event.get("telemetry"):
                        data_to_send = loads('{"ts": %i,"values": %s}'%(time.time(), ','.join(dumps(param) for param in current_event["telemetry"])))
                        self.tb_client._client.gw_send_telemetry(current_event["deviceName"], data_to_send)
                    if current_event.get("attributes"):
                        data_to_send = loads('%s'%( ','.join(dumps(param) for param in current_event["attributes"])))
                        self.tb_client._client.gw_send_attributes(current_event["deviceName"], data_to_send)
            self.__event_storage.event_pack_processing_done()
            time.sleep(5)
