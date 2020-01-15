#     Copyright 2020. ThingsBoard
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

import logging.config
import logging.handlers
import time
# import yaml
from yaml import safe_dump, safe_load
from simplejson import load, loads, dumps
from os import listdir, path
from sys import getsizeof
from threading import Thread
from queue import Queue
from thingsboard_gateway.gateway.tb_client import TBClient
from thingsboard_gateway.gateway.tb_logger import TBLoggerHandler
from thingsboard_gateway.tb_utility.tb_utility import TBUtility
from thingsboard_gateway.storage.memory_event_storage import MemoryEventStorage
from thingsboard_gateway.storage.file_event_storage import FileEventStorage
from thingsboard_gateway.gateway.tb_gateway_remote_configurator import RemoteConfigurator

log = logging.getLogger('service')


class TBGatewayService:
    def __init__(self, config_file=None):
        if config_file is None:
            config_file = path.dirname(path.dirname(path.abspath(__file__))) + '/config/tb_gateway.yaml'
        with open(config_file) as config:
            config = safe_load(config)
            self._config_dir = path.dirname(path.abspath(config_file)) + '/'
            logging.config.fileConfig(self._config_dir + "logs.conf")
            global log
            log = logging.getLogger('service')
            self.available_connectors = {}
            self.__connector_incoming_messages = {}
            self.__connected_devices = {}
            self.__saved_devices = {}
            self.__events = []
            self.__rpc_requests_in_progress = {}
            self.__connected_devices_file = "connected_devices.json"
            self.tb_client = TBClient(config["thingsboard"])
            self.tb_client.connect()
            self.tb_client.client.gw_set_server_side_rpc_request_handler(self._rpc_request_handler)
            self.tb_client.client.set_server_side_rpc_request_handler(self._rpc_request_handler)
            self.tb_client.client.subscribe_to_all_attributes(self._attribute_update_callback)
            self.tb_client.client.gw_subscribe_to_all_attributes(self._attribute_update_callback)
            self.main_handler = logging.handlers.MemoryHandler(1000)
            self.remote_handler = TBLoggerHandler(self)
            self.main_handler.setTarget(self.remote_handler)
            self._default_connectors = {
                "mqtt": "MqttConnector",
                "modbus": "ModbusConnector",
                "opcua": "OpcUaConnector",
                "ble": "BLEConnector",
            }
            self._implemented_connectors = {}
            self._event_storage_types = {
                "memory": MemoryEventStorage,
                "file": FileEventStorage,
            }
            self._event_storage = self._event_storage_types[config["storage"]["type"]](config["storage"])
            self.__load_connectors(config)
            self._connect_with_connectors()
            self.__remote_configurator = None
            if config["thingsboard"].get("remoteConfiguration"):
                try:
                    self.__remote_configurator = RemoteConfigurator(self, config)
                    self.__check_shared_attributes()
                except Exception as e:
                    self.__load_connectors(config)
                    self._connect_with_connectors()
                    log.exception(e)
            if self.__remote_configurator is not None:
                self.__remote_configurator.send_current_configuration()
            self.__load_persistent_devices()
            self.__published_events = Queue(0)
            self.__send_thread = Thread(target=self.__read_data_from_storage, daemon=True,
                                        name="Send data to Thingsboard Thread")
            self.__send_thread.start()

            try:
                gateway_statistic_send = 0
                while True:
                    cur_time = time.time()
                    if self.__rpc_requests_in_progress and self.tb_client.is_connected():
                        for rpc_in_progress in self.__rpc_requests_in_progress:
                            if cur_time >= self.__rpc_requests_in_progress[rpc_in_progress][1]:
                                self.__rpc_requests_in_progress[rpc_in_progress][2](rpc_in_progress)
                                self.cancel_rpc_request(rpc_in_progress)
                        time.sleep(0.1)
                    else:
                        time.sleep(1)

                    if cur_time - gateway_statistic_send > 60.0 and self.tb_client.is_connected():
                        summary_messages = {"eventsProduced": 0, "eventsSent": 0}
                        telemetry = {}
                        for connector in self.available_connectors:
                            if self.available_connectors[connector].is_connected():
                                connector_camel_case = connector[0].lower() + connector[1:].replace(' ', '')
                                telemetry[(connector_camel_case + ' EventsProduced').replace(' ', '')] = \
                                    self.available_connectors[connector].statistics['MessagesReceived']
                                telemetry[(connector_camel_case + ' EventsSent').replace(' ', '')] = \
                                    self.available_connectors[connector].statistics['MessagesSent']
                                self.tb_client.client.send_telemetry(telemetry)
                                summary_messages['eventsProduced'] += telemetry[
                                    str(connector_camel_case + ' EventsProduced').replace(' ', '')]
                                summary_messages['eventsSent'] += telemetry[
                                    str(connector_camel_case + ' EventsSent').replace(' ', '')]
                        self.tb_client.client.send_telemetry(summary_messages)
                        gateway_statistic_send = time.time()
            except Exception as e:
                log.exception(e)
                for device in self.__connected_devices:
                    log.debug("Close connection for device %s", device)
                    try:
                        current_connector = self.__connected_devices[device].get("connector")
                        if current_connector is not None:
                            current_connector.close()
                            log.debug("Connector %s closed connection.", current_connector.get_name())
                    except Exception as e:
                        log.error(e)

    def __attributes_parse(self, content, *args):
        try:
            shared_attributes = content.get("shared")
            client_attributes = content.get("client")
            if shared_attributes is not None:
                if self.__remote_configurator is not None and shared_attributes.get("configuration"):
                    try:
                        self.__remote_configurator.process_configuration(shared_attributes.get("configuration"))
                    except Exception as e:
                        log.exception(e)
            elif client_attributes is not None:
                log.debug("Client attributes received")
            if self.__remote_configurator is not None and content.get("configuration"):
                try:
                    self.__remote_configurator.process_configuration(content.get("configuration"))
                except Exception as e:
                    log.exception(e)
        except Exception as e:
            log.exception(e)

    def get_config_path(self):
        return self._config_dir

    def __check_shared_attributes(self):
        self.tb_client.client.request_attributes(callback=self.__attributes_parse)

    def __load_connectors(self, config):
        self._connectors_configs = {}
        if not config.get("connectors"):
            raise Exception("Configuration for connectors not found, check your config file.")
        for connector in config['connectors']:
            try:
                if connector.get('class') is not None:
                    try:
                        connector_class = TBUtility.check_and_import(connector['type'], connector['class'])
                        self._implemented_connectors[connector['type']] = connector_class
                    except Exception as e:
                        log.error("Exception when loading the custom connector:")
                        log.exception(e)
                elif connector.get("type") is not None and connector["type"] in self._default_connectors:
                    try:
                        connector_class = TBUtility.check_and_import(connector["type"], self._default_connectors[connector["type"]], default=True)
                        self._implemented_connectors[connector["type"]] = connector_class
                    except Exception as e:
                        log.error("Error on loading default connector:")
                        log.exception(e)
                else:
                    log.error("Connector with config %s - not found", safe_dump(connector))

                with open(self._config_dir + connector['configuration'], 'r') as conf_file:
                    connector_conf = load(conf_file)
                    if not self._connectors_configs.get(connector['type']):
                        self._connectors_configs[connector['type']] = []
                    self._connectors_configs[connector['type']].append({connector['configuration']: connector_conf})
            except Exception as e:
                log.error(e)

    def _connect_with_connectors(self):
        for connector_type in self._connectors_configs:
            for connector_config in self._connectors_configs[connector_type]:
                for config_file in connector_config:
                    connector = None
                    try:
                        connector = self._implemented_connectors[connector_type](self, connector_config[config_file],
                                                                                 connector_type)
                        self.available_connectors[connector.get_name()] = connector
                        connector.open()
                    except Exception as e:
                        log.exception(e)
                        if connector is not None:
                            connector.close()

    def __send_statistic(self):
        self.tb_client.client.gw_send_telemetry()

    def send_to_storage(self, connector_name, data):
        self._send_to_storage(connector_name, data)

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

        telemetry = {}
        for item in data["telemetry"]:
            telemetry = {**telemetry, **item}
        data["telemetry"] = {"ts": int(time.time() * 1000), "values": telemetry}

        json_data = dumps(data)
        save_result = self._event_storage.put(json_data)
        if not save_result:
            log.error('Data from device "%s" cannot be saved, connector name is %s.',
                      data["deviceName"],
                      connector_name)

    def __read_data_from_storage(self):
        devices_data_in_event_pack = {}
        while True:
            try:
                if self.tb_client.is_connected():
                    size = getsizeof(devices_data_in_event_pack)
                    events = self._event_storage.get_event_pack()
                    if events:
                        for event in events:
                            try:
                                current_event = loads(event)
                            except Exception as e:
                                log.exception(e)
                                continue
                            if not devices_data_in_event_pack.get(current_event["deviceName"]):
                                devices_data_in_event_pack[current_event["deviceName"]] = {"telemetry": [],
                                                                                           "attributes": {}}
                            if current_event.get("telemetry"):
                                if type(current_event["telemetry"]) == list:
                                    for item in current_event["telemetry"]:
                                        size += getsizeof(item)
                                        if size >= 48000:
                                            if not self.tb_client.is_connected(): break
                                            self.__send_data(devices_data_in_event_pack)
                                            size = 0
                                        devices_data_in_event_pack[current_event["deviceName"]]["telemetry"].append(item)
                                else:
                                    if not self.tb_client.is_connected(): break
                                    size += getsizeof(current_event["telemetry"])
                                    if size >= 48000:
                                        self.__send_data(devices_data_in_event_pack)
                                        size = 0
                                    devices_data_in_event_pack[current_event["deviceName"]]["telemetry"].append(current_event["telemetry"])
                            if current_event.get("attributes"):
                                if type(current_event["attributes"]) == list:
                                    for item in current_event["attributes"]:
                                        if not self.tb_client.is_connected(): break
                                        size += getsizeof(item)
                                        if size >= 48000:
                                            self.__send_data(devices_data_in_event_pack)
                                            size = 0
                                        devices_data_in_event_pack[current_event["deviceName"]][
                                            "attributes"].update(
                                            item.items())
                                else:
                                    if not self.tb_client.is_connected(): break
                                    size += getsizeof(current_event["attributes"].items())
                                    if size >= 48000:
                                        self.__send_data(devices_data_in_event_pack)
                                        size = 0
                                    devices_data_in_event_pack[current_event["deviceName"]]["attributes"].update(
                                        current_event["attributes"].items())
                        if devices_data_in_event_pack:
                            if not self.tb_client.is_connected(): break
                            self.__send_data(devices_data_in_event_pack)

                        if self.tb_client.is_connected():
                            success = True
                            while not self.__published_events.empty():
                                event = self.__published_events.get()
                                try:
                                    success = event.get() == event.TB_ERR_SUCCESS
                                except Exception as e:
                                    log.exception(e)
                                    success = False
                            if success:
                                self._event_storage.event_pack_processing_done()
                                del devices_data_in_event_pack
                                devices_data_in_event_pack = {}
                        else:
                            break
                    else:
                        time.sleep(.01)
                else:
                    time.sleep(.1)
            except Exception as e:
                log.exception(e)
                time.sleep(1)

    def __send_data(self, devices_data_in_event_pack):
        for device in devices_data_in_event_pack:
            self.__published_events.put(self.tb_client.client.gw_send_attributes(device,
                                                                                 devices_data_in_event_pack[
                                                                                     device][
                                                                                     "attributes"]))
            self.__published_events.put(self.tb_client.client.gw_send_telemetry(device,
                                                                                devices_data_in_event_pack[
                                                                                    device][
                                                                                    "telemetry"]))
            devices_data_in_event_pack[device] = {"telemetry": [], "attributes": {}}

    def _rpc_request_handler(self, _, content):
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
                log.debug(content)
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

    def _attribute_update_callback(self, content, *args):
        log.debug("Attribute request received with content: \"%s\"", content)
        if content.get('device') is not None:
            try:
                self.__connected_devices[content["device"]]["connector"].on_attributes_update(content)
            except Exception as e:
                log.exception(e)
        else:
            if content.get('RemoteLoggingLevel') == 'NONE':
                self.remote_handler.deactivate()
                log.info('Remote logging has being deactivated.')
            elif content.get('RemoteLoggingLevel') is not None:
                self.remote_handler.activate(content.get('RemoteLoggingLevel'))
                log.info('Remote logging has being activated.')
            else:
                self.__attributes_parse(content)

    def add_device(self, device_name, content, wait_for_publish=False):
        if device_name not in self.__saved_devices:
            self.__connected_devices[device_name] = content
            self.__saved_devices[device_name] = content
            if wait_for_publish:
                self.tb_client.client.gw_connect_device(device_name).wait_for_publish()
            else:
                self.tb_client.client.gw_connect_device(device_name)
            self.__save_persistent_devices()

    def update_device(self, device_name, event, content):
        if event == 'connector' and self.__connected_devices[device_name].get(event) != content:
            self.__save_persistent_devices()
        self.__connected_devices[device_name][event] = content

    def del_device(self, device_name):
        del self.__connected_devices[device_name]
        self.tb_client.client.gw_disconnect_device(device_name)
        self.__save_persistent_devices()

    def get_devices(self):
        return self.__connected_devices

    def __load_persistent_devices(self):
        devices = {}
        if self.__connected_devices_file in listdir(self._config_dir) and \
                path.getsize(self._config_dir + self.__connected_devices_file) > 0:
            try:
                with open(self._config_dir + self.__connected_devices_file) as devices_file:
                    devices = load(devices_file)
            except Exception as e:
                log.exception(e)
        else:
            connected_devices_file = open(self._config_dir + self.__connected_devices_file, 'w')
            connected_devices_file.close()

        if devices is not None:
            log.debug("Loaded devices:\n %s", devices)
            for device_name in devices:
                try:
                    if self.available_connectors.get(devices[device_name]):
                        self.__connected_devices[device_name] = {
                            "connector": self.available_connectors[devices[device_name]]}
                    else:
                        log.warning("Device %s connector not found, maybe it had been disabled.", device_name)
                except Exception as e:
                    log.exception(e)
                    continue
        else:
            log.debug("No device found in connected device file.")
            self.__connected_devices = {} if self.__connected_devices is None else self.__connected_devices

    def __save_persistent_devices(self):
        with open(self._config_dir + self.__connected_devices_file, 'w') as config_file:
            try:
                data_to_save = {}
                for device in self.__connected_devices:
                    if self.__connected_devices[device]["connector"] is not None:
                        data_to_save[device] = self.__connected_devices[device]["connector"].get_name()
                config_file.write(dumps(data_to_save, indent=2, sort_keys=True))
            except Exception as e:
                log.exception(e)
        log.debug("Saved connected devices.")


if __name__ == '__main__':
    TBGatewayService(path.dirname(path.dirname(path.abspath(__file__))) + '/config/tb_gateway.yaml')
