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
            # initialize client
            host = config["host"]
            token = config["token"]
            # todo validate?
            dict_extensions_settings = config["extensions"]
            dict_storage_settings = config["storage"]
            data_folder_path = dict_storage_settings["data_folder_path"]
            max_records_per_file = dict_storage_settings["max_records_per_file"]
            max_records_between_fsync = dict_storage_settings["max_records_between_fsync"]
            max_file_count = dict_storage_settings["max_file_count"]
            mqtt = TBGatewayMqttClient(host, token)
            while not mqtt._TBDeviceMqttClient__is_connected:
                try:
                    mqtt.connect()
                except Exception as e:
                    log.error(e)
                log.debug("connecting to ThingsBoard...")
                time.sleep(1)

            self.event_storage = TBEventStorage(data_folder_path,
                                                max_records_per_file,
                                                max_records_between_fsync,
                                                max_file_count)
            # todo if client receives rpc, extension must process it, maybe extract to somewhere elsewhere
            for ext_id in dict_extensions_settings:
                extension = dict_extensions_settings[ext_id]
                if extension["extension type"] == "Modbus":
                    conf = Manager.get_parameter(extension, "config file name", "modbus-config.json")
                    number_of_workers = Manager.get_parameter(extension, "threads number", 20)
                    number_of_processes = Manager.get_parameter(extension, "processes number", 1)
                    TBModbusInitializer(self, ext_id, conf, number_of_workers, number_of_processes)
                elif extension["extension type"] == "OPC-UA":
                    log.warning("OPC UA isn't implemented yet")
                elif extension["extention type"] == "Sigfox":
                    log.warning("Sigfox isn't implemented yet")
                else:
                    log.error("unknown extension type")

    def send_modbus_data_to_storage(self, data, type_of_data):
        result = {"eventType": type_of_data.upper(), "data": data}
        self.event_storage.write(result)

    def send_tb_request_to_modbus(self, *args):
        pass


def main(config_json_file):
    if __name__ == "main":
        TBGateway(config_json_file)
