import random
import string
from threading import Thread
from queue import Queue
from time import sleep, time

from thingsboard_gateway.tb_utility.tb_utility import TBUtility
from thingsboard_gateway.connectors.connector import Connector, log
from thingsboard_gateway.gateway.constants import STATISTIC_MESSAGE_SENT_PARAMETER, STATISTIC_MESSAGE_RECEIVED_PARAMETER

try:
    from twisted.internet import reactor
except ImportError:
    TBUtility.install_package('twisted')
    from twisted.internet import reactor

try:
    from pymodbus.constants import Defaults
except ImportError:
    print("Modbus library not found - installing...")
    TBUtility.install_package("pymodbus", ">=2.3.0")
    TBUtility.install_package('pyserial')
    from pymodbus.constants import Defaults

from pymodbus.client.sync import ModbusTcpClient as ModbusClient
from pymodbus.client.asynchronous import schedulers


class Device(Thread):
    def __init__(self, **kwargs):
        super().__init__()
        self.host = kwargs['host']
        self.port = kwargs['port']
        self.name = kwargs['deviceName']
        self.unit_id = kwargs['unitId']
        self.poll_period = kwargs['pollPeriod'] / 1000
        self.attributes = kwargs['attributes']
        self.timeseries = kwargs['timeseries']
        self.callback = ModbusServerConnector.callback

        self.last_polled_time = None
        self.daemon = True

        self.start()

    def timer(self):
        self.callback(self)
        self.last_polled_time = time()

        while True:
            if time() - self.last_polled_time >= self.poll_period:
                self.callback(self)
                self.last_polled_time = time()

            sleep(.2)

    def run(self):
        self.timer()


class ModbusServerConnector(Connector, Thread):
    process_requests = Queue(-1)

    def __init__(self, gateway, config, connector_type):
        self.statistics = {STATISTIC_MESSAGE_RECEIVED_PARAMETER: 0,
                           STATISTIC_MESSAGE_SENT_PARAMETER: 0}
        self.__config = config
        super().__init__()
        self.__gateway = gateway
        self._connector_type = connector_type

        self.__connected = False
        self.__stopped = False
        self.daemon = True

        self.__devices = []
        self.load_devices()

    def close(self):
        self.__stopped = True

    def open(self):
        self.__stopped = False
        self.start()
        log.info("Starting Modbus Master")

    def run(self) -> None:
        self.__stopped = False

        while True:
            if not self.__stopped and not ModbusServerConnector.process_requests.empty():
                thread = Thread(daemon=True, target=self.process_device)
                thread.start()

                sleep(.2)

    def get_name(self):
        return self.name

    def is_connected(self):
        return self.__connected

    def load_devices(self):
        self.__devices = [Device(**device) for device in self.__config.get('devices', [])]

    @classmethod
    def callback(cls, device: Device):
        cls.process_requests.put(device)

    def process_device(self):
        device = ModbusServerConnector.process_requests.get()
        client = ModbusClient(device.host, port=device.port)
        if client.connect():
            pass

    def on_attributes_update(self, content):
        pass

    def server_side_rpc_handler(self, content):
        pass
