from tb_modbus_init import TBModbusInitializer
from tb_modbus_transport_manager import TBModbusTransportManager as Manager
from tb_gateway_mqtt import TBGatewayMqttClient
from tb_event_storage import TBEventStorage
from json import load
import time
import logging
log = logging.getLogger(__name__)


class TBGateway:
    def __init__(self, config_file):
        with open(config_file) as config:
            config = load(config)
            host = config["host"]
            token = config["token"]
            # todo validate?
            dict_extensions_settings = config["extensions"]
            dict_storage_settings = config["storage"]
            data_folder_path = dict_storage_settings["path"]
            max_records_per_file = dict_storage_settings["max_records_per_file"]
            max_records_between_fsync = dict_storage_settings["max_records_between_fsync"]
            max_file_count = dict_storage_settings["max_file_count"]
            self.devices = {}
            # initialize client
            self.mqtt_gateway = TBGatewayMqttClient(host, token)
            # todo this may work only on first connection
            while not self.mqtt_gateway._TBDeviceMqttClient__is_connected:
                try:
                    self.mqtt_gateway.connect()
                except Exception as e:
                    log.error(e)
                log.debug("connecting to ThingsBoard...")
                time.sleep(1)
            self.mqtt_gateway.gw_set_server_side_rpc_request_handler(self.rpc_request_handler)

            self.event_storage = TBEventStorage(data_folder_path,
                                                max_records_per_file,
                                                max_records_between_fsync,
                                                max_file_count)
            # todo if client receives rpc, extension must process it, maybe extract to somewhere elsewhere
            for ext_id in dict_extensions_settings:
                extension = dict_extensions_settings[ext_id]
                if extension["extension type"] == "Modbus" and extension["enabled"]:
                    conf = Manager.get_parameter(extension, "config file name", "modbus-config.json")
                    number_of_workers = Manager.get_parameter(extension, "threads number", 20)
                    number_of_processes = Manager.get_parameter(extension, "processes number", 1)
                    initializer = TBModbusInitializer(self, ext_id, conf, number_of_workers, number_of_processes)
                    self.devices.update(initializer.dict_devices_servers)
                elif extension["extension type"] == "OPC-UA" and extension["enabled"]:
                    log.warning("OPC UA isn't implemented yet")
                elif extension["extension type"] == "Sigfox" and extension["enabled"]:
                    log.warning("Sigfox isn't implemented yet")
                else:
                    log.error("unknown extension type")

    def send_modbus_data_to_storage(self, data, type_of_data):
        result = {"eventType": type_of_data.upper(), "data": data}
        log.critical(result)
        self.event_storage.write(str(result))

    def send_tb_request_to_modbus(self, *args):
        pass

    def rpc_request_handler(self, request_body):
        # request body contains id, method and other parameters
        log.debug(request_body)
        # todo this may be relevant for mqtt, but not for modbus
        method = request_body["data"]["method"]
        device = request_body["device"]
        # todo if there are not such device return error, check java code
        # todo add try catch to not fall from exception
        req_id = request_body["data"]["id"]

        # dependently of request method we send different data back

        #todo every extension has its reply method in init or server, or we do it here?
