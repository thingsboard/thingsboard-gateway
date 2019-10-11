import logging.config
import time
import yaml
from json import load, loads, dumps
from os import listdir, path
from gateway.tb_client import TBClient
from tb_utility.tb_utility import TBUtility
from threading import Thread
from connectors.mqtt.mqtt_connector import MqttConnector
from connectors.opcua.opcua_connector import OpcUaConnector
from connectors.modbus.modbus_connector import ModbusConnector
from storage.memory_event_storage import MemoryEventStorage
from storage.file_event_storage import FileEventStorage

logging.config.fileConfig('config/logs.conf')
log = logging.getLogger('service')


class TBGatewayService:
    def __init__(self, config_file):
        with open(config_file) as config:
            config = yaml.safe_load(config)
            self.available_connectors = {}
            self.__connector_incoming_messages = {}
            self.__connected_devices = {}
            self.__events = []
            self.__rpc_requests_in_progress = {}
            self.__connected_devices_file = "connected_devices.json"
            self.tb_client = TBClient(config["thingsboard-client"])
            self.tb_client.client.gw_set_server_side_rpc_request_handler(self.__rpc_request_handler)
            self.__implemented_connectors = {
                "mqtt": MqttConnector,
                "modbus": ModbusConnector,
                "opcua": OpcUaConnector,
            }
            self.__load_connectors(config)
            self.__connect_with_connectors()
            self.__load_persistent_devices()
            self.__send_thread = Thread(target=self.__read_data_from_storage, daemon=True)
            if config["storage"]["type"] == "memory":
                self.__event_storage = MemoryEventStorage(config["storage"])
            else:
                self.__event_storage = FileEventStorage(config["storage"])
            self.tb_client.connect()
            self.tb_client.client.gw_subscribe_to_all_attributes(self.__attribute_update_callback)
            self.__send_thread.start()

            while True:
                for rpc_in_progress in self.__rpc_requests_in_progress:
                    if time.time() >= self.__rpc_requests_in_progress[rpc_in_progress][1]:
                        self.__rpc_requests_in_progress[rpc_in_progress][2](rpc_in_progress)
                        self.cancel_rpc_request(rpc_in_progress)
                time.sleep(.1)

    def __load_connectors(self, config):
        self._connectors_configs = {}
        for connector in config['connectors']:
            try:
                with open('config/'+connector['configuration'], 'r') as conf_file:
                    connector_conf = load(conf_file)
                    if not self._connectors_configs.get(connector['type']):
                        self._connectors_configs[connector['type']] = []
                    self._connectors_configs[connector['type']].append({connector['configuration']: connector_conf})
            except Exception as e:
                log.error(e)

    def __connect_with_connectors(self):
        for connector_type in self._connectors_configs:
            for connector_config in self._connectors_configs[connector_type]:
                for config_file in connector_config:
                    try:
                        connector = self.__implemented_connectors[connector_type](self, connector_config[config_file])
                        self.available_connectors[connector.getName()] = connector
                        connector.open()
                    except Exception as e:
                        log.error(e)

    def _send_to_storage(self, connector_name, data):
        if not TBUtility.validate_converted_data(data):
            log.error("Data from %s connector is invalid.", connector_name)
            return
        if data["deviceName"] not in self.get_devices():
            self.add_device(data["deviceName"],
                            {"connector": self.available_connectors[connector_name]}, wait_for_publish=True)
        if not self.__connector_incoming_messages.get(connector_name):
            self.__connector_incoming_messages[connector_name] = 0
        else:
            self.__connector_incoming_messages[connector_name] += 1
        json_data = dumps(data)
        save_result = self.__event_storage.put(json_data)
        if save_result:
            log.debug('Connector "%s" - Saved information - %s', connector_name, json_data)
        else:
            log.error('Data from device "%s" cannot be saved, connector name is %s.',
                      data["deviceName"],
                      connector_name)

    def __read_data_from_storage(self):
        while True:
            try:
                self.__published_events = []
                events = self.__event_storage.get_event_pack()
                if events:
                    for event in events:
                        current_event = loads(event)
                        if current_event["deviceName"] not in self.get_devices():
                            self.add_device(current_event["deviceName"],
                                            {"current_event": current_event["deviceName"]}, wait_for_publish=True)
                        else:
                            self.update_device(current_event["deviceName"],
                                               "current_event",
                                               current_event["deviceName"])
                        if current_event.get("telemetry"):
                            data_to_send = loads('{"ts": %f,"values": %s}' % (int(time.time()*1000),
                                                                              ','.join(dumps(param) for param in current_event["telemetry"])))
                            self.__published_events.append(self.tb_client.client.gw_send_telemetry(current_event["deviceName"],
                                                                                                   data_to_send))
                        if current_event.get("attributes"):
                            data_to_send = loads('%s' % (','.join(dumps(param) for param in current_event["attributes"])))
                            self.__published_events.append(self.tb_client.client.gw_send_attributes(current_event["deviceName"],
                                                                                                    data_to_send))
                    success = True
                    for event in range(len(self.__published_events)):
                        result = self.__published_events[event].get()
                        success = result == self.__published_events[event].TB_ERR_SUCCESS
                    if success:
                        self.__event_storage.event_pack_processing_done()
                else:
                    time.sleep(1)
            except Exception as e:
                log.exception(e)
                time.sleep(10)

    def __rpc_request_handler(self, _, content):
        try:
            device = content.get("device")
            if device is not None:
                connector = self.get_devices()[device].get("connector")
                if connector is not None:
                    connector.server_side_rpc_handler(content)
                else:
                    log.error("Received RPC request but connector for device %s not found. Request data: \n %s",
                              content["device"],
                              dumps(content))
            else:
                log.debug("RPC request with no device param.")
        except Exception as e:
            log.exception(e)

    def rpc_with_reply_processing(self, topic, content):
        req_id = self.__rpc_requests_in_progress[topic][0]["data"]["id"]
        device = self.__rpc_requests_in_progress[topic][0]["device"]
        self.send_rpc_reply(device, req_id, content)
        self.cancel_rpc_request(topic)

    def send_rpc_reply(self, device, req_id, content):
        self.tb_client.client.gw_send_rpc_reply(device, req_id, content)

    def register_rpc_request_timeout(self, content, timeout, topic, cancel_method):
        self.__rpc_requests_in_progress[topic] = (content, timeout, cancel_method)

    def cancel_rpc_request(self, rpc_request):
        del self.__rpc_requests_in_progress[rpc_request]

    def __attribute_update_callback(self, content):
        self.__connected_devices[content["device"]]["connector"].on_attributes_update(content)

    def add_device(self, device_name, content, wait_for_publish=False):
        self.__connected_devices[device_name] = content
        if wait_for_publish:
            self.tb_client.client.gw_connect_device(device_name).wait_for_publish()
        else:
            self.tb_client.client.gw_connect_device(device_name)
        self.__save_persistent_devices()

    def update_device(self, device_name, event, content):
        self.__connected_devices[device_name][event] = content
        if event == 'connector':
            self.__save_persistent_devices()

    def del_device(self, device_name):
        del self.__connected_devices[device_name]
        self.tb_client.client.gw_disconnect_device(device_name)
        self.__save_persistent_devices()

    def get_devices(self):
        return self.__connected_devices

    def __load_persistent_devices(self):
        devices = {}
        config_dir = './config/'
        if self.__connected_devices_file in listdir(config_dir) and \
                path.getsize(config_dir+self.__connected_devices_file) > 0:
            try:
                with open(config_dir+self.__connected_devices_file) as devices_file:
                    devices = load(devices_file)
            except Exception as e:
                log.exception(e)
        else:
            connected_devices_file = open(config_dir + self.__connected_devices_file, 'w')
            connected_devices_file.close()

        if devices is not None:
            log.debug("Loaded devices:\n %s", devices)
            for device_name in devices:
                self.__connected_devices[device_name] = {"connector": self.available_connectors[devices[device_name]]}
        else:
            log.debug("No device found in connected device file.")
            self.__connected_devices = {} if self.__connected_devices is None else self.__connected_devices

    def __save_persistent_devices(self):
        config_dir = './config/'
        with open(config_dir+self.__connected_devices_file, 'w') as config_file:
            try:
                data_to_save = {}
                for device in self.__connected_devices:
                    if self.__connected_devices[device]["connector"] is not None:
                        data_to_save[device] = self.__connected_devices[device]["connector"].get_name()
                config_file.write(dumps(data_to_save, indent=2, sort_keys=True))
            except Exception as e:
                log.exception(e)
        log.debug("Saved connected devices.")


