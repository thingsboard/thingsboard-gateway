#     Copyright 2019. ThingsBoard
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
import yaml
from json import load, loads, dumps
from os import listdir, path
from threading import Thread
from thingsboard_gateway.gateway.tb_client import TBClient
from thingsboard_gateway.gateway.tb_logger import TBLoggerHandler
from thingsboard_gateway.tb_utility.tb_utility import TBUtility
from thingsboard_gateway.connectors.mqtt.mqtt_connector import MqttConnector
from thingsboard_gateway.connectors.opcua.opcua_connector import OpcUaConnector
from thingsboard_gateway.connectors.modbus.modbus_connector import ModbusConnector
from thingsboard_gateway.connectors.ble.ble_connector import BLEConnector
from thingsboard_gateway.storage.memory_event_storage import MemoryEventStorage
from thingsboard_gateway.storage.file_event_storage import FileEventStorage


log = logging.getLogger('service')


class TBGatewayService:
    def __init__(self, config_file=None):
        if config_file is None:
            config_file = path.dirname(path.dirname(path.abspath(__file__))) + '/config/tb_gateway.yaml'
        with open(config_file) as config:
            config = yaml.safe_load(config)
            self.__config_dir = path.dirname(path.abspath(config_file))+'/'
            TBUtility.check_logs_directory(self.__config_dir+"logs.conf")
            logging.config.fileConfig(self.__config_dir+"logs.conf")
            global log
            log = logging.getLogger('service')
            self.available_connectors = {}
            self.__connector_incoming_messages = {}
            self.__connected_devices = {}
            self.__events = []
            self.__rpc_requests_in_progress = {}
            self.__connected_devices_file = "connected_devices.json"
            self.tb_client = TBClient(config["thingsboard"])
            self.main_handler = logging.handlers.MemoryHandler(1000)
            self.remote_handler = TBLoggerHandler(self)
            self.main_handler.setTarget(self.remote_handler)
            self.__implemented_connectors = {
                "mqtt": MqttConnector,
                "modbus": ModbusConnector,
                "opcua": OpcUaConnector,
                "ble": BLEConnector,
            }
            self.__event_storage_types = {
                "memory": MemoryEventStorage,
                "file": FileEventStorage,
            }
            self.__load_connectors(config)
            self.__connect_with_connectors()
            self.__load_persistent_devices()
            self.__send_thread = Thread(target=self.__read_data_from_storage, daemon=True)
            self.__event_storage = self.__event_storage_types[config["storage"]["type"]](config["storage"])
            self.tb_client.connect()
            self.tb_client.client.gw_set_server_side_rpc_request_handler(self.__rpc_request_handler)
            self.tb_client.client.set_server_side_rpc_request_handler(self.__rpc_request_handler)
            self.tb_client.client.subscribe_to_all_attributes(self.__attribute_update_callback)
            self.tb_client.client.gw_subscribe_to_all_attributes(self.__attribute_update_callback)
            self.__send_thread.start()

            try:
                gateway_statistic_send = 0
                while True:
                    for rpc_in_progress in self.__rpc_requests_in_progress:
                        if time.time() >= self.__rpc_requests_in_progress[rpc_in_progress][1]:
                            self.__rpc_requests_in_progress[rpc_in_progress][2](rpc_in_progress)
                            self.cancel_rpc_request(rpc_in_progress)

                    if int(time.time()*1000) - gateway_statistic_send >= 60000:
                        summary_messages = {"SummaryReceived": 0, "SummarySent": 0}
                        telemetry = {}
                        for connector in self.available_connectors:
                            if self.available_connectors[connector].is_connected():
                                telemetry[(connector+' MessagesReceived').replace(' ', '')] = self.available_connectors[connector].statistics['MessagesReceived']
                                telemetry[(connector+' MessagesSent').replace(' ', '')] = self.available_connectors[connector].statistics['MessagesSent']
                                self.tb_client.client.send_telemetry(telemetry)
                                summary_messages['SummaryReceived'] += telemetry[(connector+' MessagesReceived').replace(' ', '')]
                                summary_messages['SummarySent'] += telemetry[(connector+' MessagesSent').replace(' ', '')]
                        self.tb_client.client.send_telemetry(summary_messages)
                        gateway_statistic_send = int(time.time()*1000)

                    time.sleep(.1)
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

    def get_config_path(self):
        return self.__config_dir

    def __load_connectors(self, config):
        self._connectors_configs = {}
        if not config.get("connectors"):
            raise Exception("Configuration for connectors not found, check your config file.")
        for connector in config['connectors']:
            try:
                with open(self.__config_dir+connector['configuration'], 'r') as conf_file:
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
                        self.available_connectors[connector.get_name()] = connector
                        connector.open()
                    except Exception as e:
                        log.exception(e)
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
                            log.debug(current_event)
                            telemetry = {}
                            if type(current_event["telemetry"]) == list:
                                for item in current_event["telemetry"]:
                                    for key in item:
                                        telemetry[key] = item[key]
                            else:
                                telemetry = current_event["telemetry"]
                            log.debug(telemetry)
                            data_to_send = loads('{"ts": %f,"values": %s}' % (int(time.time()*1000), dumps(telemetry)))
                            # data_to_send = loads('{"ts": %f,"values": {%s}}' % (int(time.time()*1000),
                            #                                                   ','.join(dumps(param) for param in current_event["telemetry"])))
                            self.__published_events.append(self.tb_client.client.gw_send_telemetry(current_event["deviceName"],
                                                                                                   data_to_send))
                        if current_event.get("attributes"):
                            log.debug(current_event)
                            attributes = {}
                            if type(current_event["attributes"]) == list:
                                for item in current_event["attributes"]:
                                    for key in item:
                                        attributes[key] = item[key]
                            else:
                                attributes = current_event["attributes"]
                            log.debug(attributes)
                            data_to_send = loads('%s' % dumps(attributes))
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

    def __attribute_update_callback(self, content, *args):
        if content.get('device') is not None:
            try:
                self.__connected_devices[content["device"]]["connector"].on_attributes_update(content)
            except Exception as e:
                log.error(e)
        else:
            if content.get('RemoteLoggingLevel') == 'NONE':
                self.remote_handler.deactivate()
                log.info('Remote logging has being deactivated.')
            elif content.get('RemoteLoggingLevel') is not None:
                self.remote_handler.activate(content.get('RemoteLoggingLevel'))
                log.info('Remote logging has being activated.')
            else:
                log.debug('Attributes on the gateway has being updated!')
                log.debug(args)
                log.debug(content)

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
        if self.__connected_devices_file in listdir(self.__config_dir) and \
                path.getsize(self.__config_dir+self.__connected_devices_file) > 0:
            try:
                with open(self.__config_dir+self.__connected_devices_file) as devices_file:
                    devices = load(devices_file)
            except Exception as e:
                log.exception(e)
        else:
            connected_devices_file = open(self.__config_dir + self.__connected_devices_file, 'w')
            connected_devices_file.close()

        if devices is not None:
            log.debug("Loaded devices:\n %s", devices)
            for device_name in devices:
                try:
                    if self.available_connectors.get(devices[device_name]):
                        self.__connected_devices[device_name] = {"connector": self.available_connectors[devices[device_name]]}
                    else:
                        log.warning("Device %s connector not found, maybe it had been disabled.", device_name)
                except Exception as e:
                    log.exception(e)
                    continue
        else:
            log.debug("No device found in connected device file.")
            self.__connected_devices = {} if self.__connected_devices is None else self.__connected_devices

    def __save_persistent_devices(self):
        with open(self.__config_dir+self.__connected_devices_file, 'w') as config_file:
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
