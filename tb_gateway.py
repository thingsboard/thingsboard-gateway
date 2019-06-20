from tb_device_storage import TBDeviceStorage
import logging
import time
from importlib import import_module
from json import load, dumps, dump, loads
from queue import Queue
from threading import Lock, Thread
from mqtt.tb_mqtt_extension import TBMQTT
from apscheduler.events import EVENT_JOB_ERROR
from apscheduler.executors.pool import ThreadPoolExecutor, ProcessPoolExecutor
from apscheduler.schedulers.background import BackgroundScheduler
from tb_event_storage import TBEventStorage
from modbus.tb_modbus_extension import TBModbus
from ble.tb_ble_extension import TBBluetoothLE
from tb_gateway_mqtt import TBGatewayMqttClient
from tb_utility import TBUtility

log = logging.getLogger(__name__)


class TBGateway:
    def __init__(self, config_file):
        with open(config_file) as config:
            config = load(config)
            host = config["host"]
            port = TBUtility.get_parameter(config, "port", 1883)
            token = config["token"]
            dict_extensions_settings = config["extensions"]
            dict_storage_settings = config["storage"]
            dict_performance_settings = config.get("performance")
            data_folder_path = dict_storage_settings["path"]
            max_records_per_file = dict_storage_settings["max_records_per_file"]
            max_records_between_fsync = dict_storage_settings["max_records_between_fsync"]
            max_file_count = dict_storage_settings["max_file_count"]
            read_interval = dict_storage_settings["read_interval"]
            max_read_record_count = dict_storage_settings["max_read_record_count"]
            if dict_performance_settings:
                number_of_processes = TBUtility.get_parameter(dict_performance_settings, "processes_to_use", 20)
                number_of_workers = TBUtility.get_parameter(dict_performance_settings, "additional_threads_to_use", 5)
            self.dict_ext_by_device_name = {}
            self.dict_rpc_handlers_by_device = {}
            self.lock = Lock()

            # initialize scheduler
            executors = {'default': ThreadPoolExecutor(number_of_workers)}
            if number_of_processes > 1:
                executors.update({'processpool': ProcessPoolExecutor(number_of_processes)})
            self.scheduler = BackgroundScheduler(executors=executors)
            self.scheduler.add_listener(TBGateway.listener, EVENT_JOB_ERROR)
            self.scheduler.start()

            # initialize client
            self.mqtt_gateway = TBGatewayMqttClient(host, token, self)

            # todo add tls
            while not self.mqtt_gateway._TBDeviceMqttClient__is_connected:
                try:
                    self.mqtt_gateway.connect(port=port)
                except Exception as e:
                    log.error(e)
                log.debug("connecting to ThingsBoard...")
                time.sleep(1)

            def rpc_request_handler(self, request_body):
                method = request_body["data"]["method"]
                device = request_body.get("device")
                # device = "MJ_HT_V1_4c65a8df8e3f"
                #todo this is for testing, in worst case test for modbus
                extension_class = self.gateway.dict_ext_by_device_name[device]
                values = None
                # todo remove if true, uncomment if type..., remove connect(test device), they were for testing purpses
                try:
                    dict_device_handlers = self.gateway.dict_rpc_handlers_by_device[device]
                except KeyError:
                    pass
                    self.send_rpc_reply("o device {} found".format(device))
                    log.error("no device {} found".format(device))
                    return
                try:
                    handler = dict_device_handlers[method]
                except KeyError:
                    self.send_rpc_reply(device, request_body["data"]["id"], {"error": "Unsupported RPC method"})
                    log.error('"error": "Unsupported RPC method": {}'.format(request_body))
                    return
                if type(extension_class) == TBModbus:
                # if True:
                #     device = "Temp Sensor"
                    if type(handler) == str:
                        m = import_module("extensions.modbus."+handler).Extension()
                        params = None
                        try:
                            params = request_body["data"]["params"]
                            values = m.rpc_handler(method, params)
                        except KeyError:
                            pass
                    elif type(handler) == dict:
                        values = handler
                        values.update({"deviceName": device})
                    else:
                        log.error("rpc handler not in dict format nor in string path to python file")
                        return
                    if "tag" not in values:
                        self.send_rpc_reply(device, request_body["data"]["id"], {"ErrorWriteTag": "No tag found"})
                        log.error('"ErrorWriteTag": "No tag found": {}'.format(request_body))
                        return
                    resp = self.gateway.dict_ext_by_device_name[device](values)
                    if resp:
                        self.gw_send_rpc_reply(device, request_body["data"]["id"], resp)

                elif type(extension_class) == TBBluetoothLE:
                    if method == "doRescan":
                        self.gateway.dict_ext_by_device_name[device].rescan()
                    if handler.get("getTelemetry"):
                        self.gateway.dict_ext_by_device_name[device].get_data_from_device_once(device)
                    # todo imploment write to device rpc
                    # if handler.get("handler"):
                    #     m = import_module("extensions.ble."+handler["handler"])
                    #     params = None
                    #     try:
                    #         params = request_body["data"]["params"]
                    #         values = m.rpc_handler(method, params)
                    #     except KeyError:
                    #         pass

            self.mqtt_gateway.devices_server_side_rpc_request_handler = rpc_request_handler
            self.mqtt_gateway.gw_connect_device("Test Device A2")
            # connect devices from file
            self.mqtt_gateway.connect_devices_from_file(self.mqtt_gateway)
            # initialize connected device logging

            self.q = Queue()
            # todo fix!!!!!
            device_storage = TBDeviceStorage(self)

            # initialize event_storage
            self.event_storage = TBEventStorage(data_folder_path, max_records_per_file, max_records_between_fsync,
                                                max_file_count, read_interval, max_read_record_count, self.scheduler,
                                                self)

            # initialize extensions
            for ext_id in dict_extensions_settings:
                extension = dict_extensions_settings[ext_id]
                if extension["extension type"] == "Modbus" and extension["enabled"]:
                    conf = TBUtility.get_parameter(extension, "config file name", "modbus-config.json")
                    with open(conf, "r") as config_file:
                        for modbus_config in load(config_file)["servers"]:
                            TBModbus(modbus_config, self.scheduler, self, ext_id)
                elif extension["extension type"] == "BLE" and extension["enabled"]:
                    conf = TBUtility.get_parameter(extension, "config file name", "ble-config.json")
                    TBBluetoothLE(self, conf)
                elif extension["extension type"] == "MQTT" and extension["enabled"]:
                    conf = TBUtility.get_parameter(extension, "config file name", "mqtt-config.json")
                    with open(conf, "r") as config_file:
                        for broker_config in load(config_file)["brokers"]:
                            TBMQTT(broker_config, self, ext_id)
                elif extension["extension type"] == "OPC-UA" and extension["enabled"]:
                    log.warning("OPC UA isn't implemented yet")
                elif extension["extension type"] == "Sigfox" and extension["enabled"]:
                    log.warning("Sigfox isn't implemented yet")
                elif extension["enabled"]:
                    log.error("unknown extension type: {}".format(extension["extension type"]))

    def on_device_connected(self, device_name, handler, rpc_handlers):
        self.q.put((True, device_name, handler, rpc_handlers))

    def on_device_disconnected(self, device_name):
        self.q.put((False, device_name))

    def send_data_to_storage(self, data, type_of_data, device):
        if type_of_data == "tms":
            self.event_storage.write(dumps({"eventType": "TELEMETRY", "device": device, "data": data}) + "\n")
        else:
            self.event_storage.write(dumps({"eventType": "ATTRIBUTES", "device": device, "data": data}) + "\n")

    def send_data_to_tb(self, data):
        for event in data:
            event = loads(event)
            print(event)
            device = event["device"]
            # if event["eventType"] == "TELEMETRY":
            #     self.mqtt_gateway.gw_send_telemetry(device, event["data"])
            # else:
            #     self.mqtt_gateway.gw_send_attributes(device, event["data"]["values"])
        return True

    @staticmethod
    def listener(event):
        log.exception(event.exception)


