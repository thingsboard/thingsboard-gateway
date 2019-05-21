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
            self.devices = {}

            # initialize scheduler
            executors = {'default': ThreadPoolExecutor(number_of_workers)}
            if number_of_processes > 1:
                executors.update({'processpool': ProcessPoolExecutor(number_of_processes)})
            self._scheduler = BackgroundScheduler(executors=executors)
            self._scheduler.add_listener(TBGateway.listener, EVENT_JOB_ERROR)

            # initialize client
            self.mqtt_gateway = TBGatewayMqttClient(host, token)
            while not self.mqtt_gateway._TBDeviceMqttClient__is_connected:
                try:
                    self.mqtt_gateway.connect()
                except Exception as e:
                    log.error(e)
                log.debug("connecting to ThingsBoard...")
                time.sleep(1)
            self.mqtt_gateway.gw_set_server_side_rpc_request_handler(self.rpc_request_handler)

            # initialize event_storage
            self.event_storage = TBEventStorage(data_folder_path, max_records_per_file, max_records_between_fsync,
                                                max_file_count, read_interval, max_read_record_count, self._scheduler,
                                                self)

            # initialize extensions
            for ext_id in dict_extensions_settings:
                extension = dict_extensions_settings[ext_id]
                if extension["extension type"] == "Modbus" and extension["enabled"]:
                    conf = Manager.get_parameter(extension, "config file name", "modbus-config.json")
                    initializer = TBModbusInitializer(self, ext_id, self._scheduler, conf)
                    self.devices.update(initializer.dict_devices_servers)
                elif extension["extension type"] == "OPC-UA" and extension["enabled"]:
                    log.warning("OPC UA isn't implemented yet")
                elif extension["extension type"] == "Sigfox" and extension["enabled"]:
                    log.warning("Sigfox isn't implemented yet")
                else:
                    log.error("unknown extension type")

    def on_device_connected(self, device_name, rpc_handler):
        self.devices.update({device_name: rpc_handler})

    def on_device_disconnected(self, device_name):
        try:
            self.devices.pop(device_name)
        except KeyError:
            log.warning("tried to remove {}, device not found".format(device_name))

    def send_data_to_storage(self, data, type_of_data):
        result = {"eventType": type_of_data.upper(), "data": data}
        self.event_storage.write(dumps(result))

    def rpc_request_handler(self, request_body):
        # todo transform request to standard of redone modbus functions to fit request form
        # request body contains id, method and other parameters
        log.debug(request_body)
        device = request_body["device"]
        self.devices[device](request_body)

        # todo if there are not such device return error, check java code
        # todo return data back?

    @staticmethod
    def listener(event):
        log.exception(event.exception)
