from importlib import import_module
from tb_modbus_init import TBModbusInitializer
from tb_modbus_server import TBModbusServer
from tb_modbus_transport_manager import TBModbusTransportManager as Manager
from tb_gateway_mqtt import TBGatewayMqttClient
from tb_event_storage import TBEventStorage
from json import load, dumps, dump
from queue import Queue
import time
import logging
from apscheduler.executors.pool import ThreadPoolExecutor, ProcessPoolExecutor
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.events import EVENT_JOB_ERROR
from threading import Lock, Thread
log = logging.getLogger(__name__)


class TBGateway:
    def __init__(self, config_file):
        with open(config_file) as config:
            config = load(config)
            host = config["host"]
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
                number_of_processes = Manager.get_parameter(dict_performance_settings, "processes_to_use", 20)
                number_of_workers = Manager.get_parameter(dict_performance_settings, "threads_to_use", 1)
            self.dict_ext_by_device_name = {}
            self.dict_rpc_handlers_by_device = {}
            self.lock = Lock()

            # initialize scheduler
            executors = {'default': ThreadPoolExecutor(number_of_workers)}
            if number_of_processes > 1:
                executors.update({'processpool': ProcessPoolExecutor(number_of_processes)})
            self._scheduler = BackgroundScheduler(executors=executors)
            self._scheduler.add_listener(TBGateway.listener, EVENT_JOB_ERROR)
            self._scheduler.start()

            # initialize client
            self.mqtt_gateway = TBGatewayMqttClient(host, token, self)

            while not self.mqtt_gateway._TBDeviceMqttClient__is_connected:
                try:
                    self.mqtt_gateway.connect()
                except Exception as e:
                    log.error(e)
                log.debug("connecting to ThingsBoard...")
                time.sleep(1)

            def rpc_request_handler(self, request_body):
                method = request_body["data"]["method"]
                device = request_body.get("device")
                dict_values = None
                # todo remove if true, uncomment if type..., remove connect(test device), they were for testing purpses
                if type(self.gateway.dict_ext_by_device_name[device]) == TBModbusServer:
                # if True:
                #     device = "Temp Sensor"
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
                    if type(handler) == str:
                        m = import_module("extensions.modbus."+handler)
                        params = None
                        try:
                            params = request_body["data"]["params"]
                        except KeyError:
                            pass
                        dict_values = m.rpc_handler(method, params)
                    elif type(handler) == dict:
                        dict_values = handler
                        dict_values.update({"deviceName": device})
                    else:
                        log.error("rpc handler not in dict format nor in string path to python file")
                        return
                    if "tag" not in dict_values:
                        self.send_rpc_reply(device, request_body["data"]["id"], {"ErrorWriteTag": "No tag found"})
                        log.error('"ErrorWriteTag": "No tag found": {}'.format(request_body))
                        return
                resp = self.gateway.dict_ext_by_device_name[device](dict_values)
                if resp:
                    self.gw_send_rpc_reply(device, request_body["data"]["id"], resp)

            self.mqtt_gateway.devices_server_side_rpc_request_handler = rpc_request_handler
            # self.mqtt_gateway.gw_connect_device("Test Device A2")
            # connect devices from file
            self.mqtt_gateway.connect_devices_from_file()
            # initialize connected device logging thread
            self.q = Queue()

            def update_connected_devices():
                while True:
                    item = self.q.get()
                    is_method_connect = item[0]
                    device_name = item[1]
                    # if method is "connect device"
                    if is_method_connect:
                        handler = item[2]
                        rpc_handlers = item[3]
                        self.mqtt_gateway.gw_connect_device(device_name)
                        self.dict_ext_by_device_name.update({device_name: handler})
                        self.dict_rpc_handlers_by_device.update({device_name: rpc_handlers})

                        with open("connectedDevices.json") as f:
                            try:
                                connected_devices = load(f)
                            except:
                                connected_devices = {}
                        if device_name in connected_devices:
                            log.debug("{} already in connected devices json".format(device_name))
                        else:
                            connected_devices.update({device_name: {}})
                            with open("connectedDevices.json", "w") as f:
                                dump(connected_devices, f)
                    # if method is "disconnect device"
                    else:
                        try:
                            self.dict_ext_by_device_name.pop(device_name)
                            with open("connectedDevices.json") as f:
                                try:
                                    connected_devices = load(f)
                                except:
                                    log.debug("there are no connected devices json")
                            if device_name not in connected_devices:
                                log.debug("{} not connected in json file".format(device_name))
                            else:
                                connected_devices.pop(device_name)
                                with open("connectedDevices.json", "w") as f:
                                    dump(connected_devices, f)
                        except KeyError:
                            log.warning("tried to remove {}, device not found".format(device_name))
                    queue_size = self.q.qsize()
                    if queue_size == 0:
                        timeout = 0.5
                    elif queue_size < 5:
                        timeout = 0.1
                    else:
                        timeout = 0.05
                    time.sleep(timeout)
            self.t = Thread(target=update_connected_devices).start()

            # initialize event_storage
            self.event_storage = TBEventStorage(data_folder_path, max_records_per_file, max_records_between_fsync,
                                                max_file_count, read_interval, max_read_record_count, self._scheduler,
                                                self)

            # initialize extensions
            for ext_id in dict_extensions_settings:
                extension = dict_extensions_settings[ext_id]
                if extension["extension type"] == "Modbus" and extension["enabled"]:
                    conf = Manager.get_parameter(extension, "config file name", "modbus-config.json")
                    self.modbus_initializer = TBModbusInitializer(self, ext_id, self._scheduler, conf)
                    self.dict_ext_by_device_name.update(self.modbus_initializer.dict_devices_servers)
                elif extension["extension type"] == "OPC-UA" and extension["enabled"]:
                    log.warning("OPC UA isn't implemented yet")
                elif extension["extension type"] == "Sigfox" and extension["enabled"]:
                    log.warning("Sigfox isn't implemented yet")
                elif extension["extension type"] == "BLE" and extension["enabled"]:
                    conf = Manager.get_parameter(extension, "config file name", "ble-config.json")
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

    @staticmethod
    def listener(event):
        log.exception(event.exception)


