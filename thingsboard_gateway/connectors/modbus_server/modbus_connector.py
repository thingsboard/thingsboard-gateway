import threading

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

from pymodbus.device import ModbusDeviceIdentification
from pymodbus.version import version
from pymodbus.server.asynchronous import StartTcpServer, StartUdpServer, StopServer, _is_main_thread, StartSerialServer
from pymodbus.datastore import ModbusSlaveContext, ModbusServerContext
from pymodbus.datastore import ModbusSequentialDataBlock
from pymodbus.transaction import (ModbusRtuFramer, ModbusAsciiFramer, ModbusBinaryFramer)

MODBUS_SERVER_TYPE = {
    'tcp': {
        'function': StartTcpServer
    },
    'tcp_with_rtu_framer': {
        'function': StartTcpServer,
        'framer': ModbusRtuFramer
    },
    'udp': {
        'function': StartUdpServer
    },
    'rtu': {
        'function': StartSerialServer,
        'framer': ModbusRtuFramer
    },
    'ASCII': {
        'function': StartSerialServer,
        'framer': ModbusAsciiFramer
    },
    'binary': {
        'function': StartSerialServer,
        'framer': ModbusBinaryFramer
    }
}


class ModbusServerConnector(Connector, threading.Thread):
    def __init__(self, gateway, config, connector_type):
        self.statistics = {STATISTIC_MESSAGE_RECEIVED_PARAMETER: 0,
                           STATISTIC_MESSAGE_SENT_PARAMETER: 0}
        self.__config = config
        super().__init__()
        self.__gateway = gateway
        self._connector_type = connector_type

        self.__identity = ModbusDeviceIdentification()
        self.__identity.VendorName = self.__config['server'].get('vendorName', '')
        self.__identity.ProductCode = self.__config['server'].get('productCode', '')
        self.__identity.VendorUrl = self.__config['server'].get('vendorUrl', '')
        self.__identity.ProductName = self.__config['server'].get('productName', '')
        self.__identity.ModelName = self.__config['server'].get('modelName', '')
        self.__identity.MajorMinorRevision = version.short()

        self.__store = ModbusSlaveContext()
        self.__load_store()

        self.__context = ModbusServerContext(slaves=self.__store, single=True)

        MODBUS_SERVER_TYPE[
            self.__config['server']['type']
        ]['function'](self.__context,
                      defer_reactor_run=True,
                      identity=self.__identity,
                      address=(self.__config['server'].get('address'), self.__config['server'].get('port')) if
                      self.__config['server'].get('address') else None,
                      port=self.__config['server'].get('port') if not self.__config['server'].get('address') else None,
                      framer=MODBUS_SERVER_TYPE[self.__config['server']['type']].get('framer'))

        self.__connected = False
        self.__stopped = False
        self.daemon = True

    def close(self):
        StopServer()
        self.__stopped = True

    def open(self):
        self.__stopped = False
        self.start()
        log.info("Starting Modbus Server")

    def run(self) -> None:
        self.__connected = True
        reactor.run(installSignalHandlers=_is_main_thread())

    def __load_store(self):
        for store in self.__config['stores']:
            self.__store.register(store['functionCode'], store['tag'],
                                  ModbusSequentialDataBlock(store['address'], [0] * store['objectsCount']))

    def get_name(self):
        return self.name

    def is_connected(self):
        return self.__connected

    def on_attributes_update(self, content):
        pass

    def server_side_rpc_handler(self, content):
        pass
