from tb_modbus_init import TBModbusInitializer
from tb_modbus_transport_manager import TBModbusTransportManager as Manager
from tb_gateway_mqtt import TBGatewayMqttClient
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

            # todo if client wasn't created repeat creation and connection and log it
            mqtt = TBGatewayMqttClient(host, token)
            mqtt.connect()
            # # todo not working
            # conn = False
            # def conn_callback(*args):
            #     print(1)
            #     global conn
            #     conn = True

            # while not conn:
            #     print(conn)
            #     print(mqtt.connect(callback=conn_callback))
            #     time.sleep(1)
            # mqtt.gw_connect_device("Example Name 3")

            # todo create storage here

            # todo connect mqtt devices via...?
            # todo if client receives rpc, extension must process it, maybe extract to somewhere elsewhere
            for id in dict_extensions_settings:
                extension = dict_extensions_settings["id"]
                if extension["extension type"] == "Modbus":
                    conf = Manager.get_parameter(extension, "config file name", "modbus-config.json")
                    number_of_workers = Manager.get_parameter(extension, "threads number", 20)
                    number_of_processes = Manager.get_parameter(extension, "processes number", 1)
                    # todo log extension start w/ its id or throw id into start
                    init = TBModbusInitializer(conf, number_of_workers, number_of_processes)
                    init.start()
                elif extension["extension type"] == "OPC-UA":  # todo check spelling in docs
                    pass
                elif extension["extention type"] == "Sigfox":
                    pass
                else:
                    log.debug("unknown extension type")  # todo is it enough for?
            # todo init extensions


def main(config_json_file):
    if __name__ == "main":
        TBGateway(config_json_file)
