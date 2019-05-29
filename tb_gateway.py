from importlib import import_module
from tb_modbus_init import TBModbusInitializer
from tb_modbus_transport_manager import TBModbusTransportManager as Manager
from tb_gateway_mqtt import TBGatewayMqttClient
from tb_event_storage import TBEventStorage
from json import load, dumps
import time
import logging
from apscheduler.executors.pool import ThreadPoolExecutor, ProcessPoolExecutor
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.events import EVENT_JOB_ERROR
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
                # if type(self.gateway.dict_ext_by_device_name[device]) == TBModbusServer:
                device = request_body["device"]
                method = request_body["data"]["method"]
                dict_values = None
                if True:
                    #todo for testing, remove later
                    device = "Temp Sensor"
                    handler = self.gateway.dict_rpc_handlers_by_device[device][method]
                    if type(handler) == str:
                        m = import_module(handler)
                        # m = import_module(self.gateway.dict_rpc_handlers_by_device[request_body["device"]])
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
                        log.warning("rpc handler not in dict format nor in string path to python file")
                        return
                # self.gateway.dict_ext_by_device_name[device](dict_values.update({"deviceName": device}))
                self.gateway.dict_ext_by_device_name[device](dict_values)

            self.mqtt_gateway.devices_server_side_rpc_request_handler = rpc_request_handler
            # todo remove, its hardcode for presentation
            self.mqtt_gateway.gw_connect_device("Test Device A2")

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
                elif extension["enabled"]:
                    log.error("unknown extension type: {}".format(extension["extension type"]))

    def on_device_connected(self, device_name, handler, rpc_handlers):
        self.mqtt_gateway.gw_connect_device(device_name)
        self.dict_ext_by_device_name.update({device_name: handler})
        self.dict_rpc_handlers_by_device.update({device_name: rpc_handlers})

    def on_device_disconnected(self, device_name):
        try:
            self.dict_ext_by_device_name.pop(device_name)
        except KeyError:
            log.warning("tried to remove {}, device not found".format(device_name))

    def send_data_to_storage(self, data, type_of_data, device):
        if type_of_data == "tms":
            self.event_storage.write(dumps({"eventType": "TELEMETRY", "device": device, "data": data}) + "\n")
        else:
            self.event_storage.write(dumps({"eventType": "ATTRIBUTES", "device": device, "data": data}) + "\n")

    @staticmethod
    def listener(event):
        log.exception(event.exception)
