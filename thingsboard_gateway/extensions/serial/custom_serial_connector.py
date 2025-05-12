#     Copyright 2025. ThingsBoard
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


from queue import Queue
from threading import Event, Thread, Lock
from typing import List, TYPE_CHECKING

import serial.tools
import serial.tools.list_ports
from thingsboard_gateway.tb_utility.tb_utility import TBUtility
from time import monotonic, sleep

try:
    import serial
except ImportError:
    print("pyserial library not found - installing...")
    TBUtility.install_package("pyserial")
    import serial

from thingsboard_gateway.connectors.connector import Connector
from thingsboard_gateway.tb_utility.tb_loader import TBModuleLoader
from thingsboard_gateway.tb_utility.tb_logger import init_logger

if TYPE_CHECKING:
    #  necessary for type checking to avoid circular import
    from thingsboard_gateway.gateway.tb_gateway_service import TBGatewayService


class SerialDevice(Thread):
    """
    Serial device class is used to represent a device that is connected to the serial port.
    It is used to read data from the device and send it to the platform.
    """
    def __init__(self, device_config, uplink_converter, stop_event: Event, logger, uplink_queue):
        super().__init__()
        self.__log = logger
        self.uplink_queue = uplink_queue
        self.daemon = True
        self.stopped = True
        self.__connector_stopped = stop_event
        self.config = device_config
        self.name = self.config.get('deviceName', self.config.get('name', 'SerialDevice'))
        self.type = self.config.get('deviceType', self.config.get('type', 'default'))
        self.uplink_converter = uplink_converter
        self.downlink_converter = None
        self.delimiter = self.config.get('delimiter', '\n')
        self.__rpc_in_progress = Event()
        self.__previous_connect = 0

        self.port = self.config.get('port', '/dev/ttyUSB0')
        self.baudrate = self.config.get('baudrate', 9600)
        self.timeout = self.config.get('timeout', 1)
        self.bytesize = self.config.get('bytesize', serial.EIGHTBITS)
        self.stopbits = self.config.get('stopbits', serial.STOPBITS_ONE)
        self.parity = self.config.get('parity', serial.PARITY_NONE)
        self.dsrdtr = self.config.get('dsrdtr', False)
        self.rtscts = self.config.get('rtscts', False)
        self.xonxoff = self.config.get('xonxoff', False)
        self.write_timeout = self.config.get('writeTimeout', None)
        self.inter_byte_timeout = self.config.get('interByteTimeout', None)
        self.exclusive = self.config.get('exclusive', None)
        self.__serial_lock = Lock()
        self.__serial = None

    def get_serial(self):
        """
        Method to get serial connection to the device.
        If connection is not established, it tries to connect to the device.
        """
        with self.__serial_lock:
            if self.__serial is None or not self.__serial.is_open:
                try:
                    self.__serial = serial.Serial(
                        port=self.port,
                        baudrate=self.baudrate,
                        timeout=self.timeout,
                        bytesize=self.bytesize,
                        stopbits=self.stopbits,
                        parity=self.parity,
                        dsrdtr=self.dsrdtr,
                        rtscts=self.rtscts,
                        xonxoff=self.xonxoff,
                        write_timeout=self.write_timeout,
                        inter_byte_timeout=self.inter_byte_timeout,
                        exclusive=self.exclusive
                    )
                    self.__log.info("Connected to device %s", self.name)
                except Exception as e:
                    self.__log.error("Failed to connect to device %s: %s", self.name, e)
                    self.__serial = None
        return self.__serial

    def run(self):
        """
        Main method to read data from the device and send it to the platform.
        """
        self.__log.info("Device %s started", self.name)
        self.stopped = False
        while not self.__connector_stopped.is_set() and not self.stopped:
            try:
                # Avoid excessive "Failed to connect to device" errors when __read_data_from_serial() calls get_serial()
                if self.get_serial() is None or not self.__serial.is_open:
                    sleep(3)
                else:
                    if not self.__rpc_in_progress.is_set():
                        data_from_device = self.__read_data_from_serial()
                        if data_from_device:
                            try:
                                converted_data = self.uplink_converter.convert(None, data_from_device)
                                self.uplink_queue.put(converted_data)
                            except Exception as e:
                                self.__log.error("Failed to convert data from device %s: %s", self.name, e)
            except Exception as e:
                self.__log.exception("Error in device %s: %s", self.name, e)
                self.stop()
        self.__log.info("Device %s stopped", self.name)

    def handle_rpc_request(self, rpc_method, params):
        """
        Method to process RPC requests from the platform.
        """
        result = {"success": True}
        processed = False
        for rpc_config in self.config.get("serverSideRpc", []):
            if rpc_method == rpc_config.get("method"):
                processed = True
                self.__rpc_in_progress.set()
                try:
                    if self.downlink_converter is not None:
                        converted_data = self.downlink_converter.convert(rpc_config, params)
                        if converted_data:
                            with_response = rpc_config.get("withResponse", False)
                            response_timeout = rpc_config.get("responseTimeoutSec", 5)
                            response = self.write(converted_data,
                                                  with_response=with_response,
                                                  response_timeout=response_timeout)
                            if with_response:
                                response_uplink_config = {}
                                if rpc_config.get("responseType"):
                                    response_uplink_config["type"] = rpc_config.get("responseType")
                                if rpc_config.get("responseFromByte"):
                                    response_uplink_config["fromByte"] = rpc_config.get("responseFromByte")
                                if rpc_config.get("responseToByte"):
                                    response_uplink_config["toByte"] = rpc_config.get("responseToByte")
                                if rpc_config.get("responseUntilDelimiter"):
                                    response_uplink_config["delimiter"] = rpc_config.get("responseUntilDelimiter")
                                if response_uplink_config and response:
                                    result = self.uplink_converter.convert(response_uplink_config, response)
                                else:
                                    result = {"error": "Cannot convert response with config: %r and response: %r" % (
                                        response_uplink_config, response), "success": False}
                        else:
                            result = {"error": "No data to send", "success": False}
                    else:
                        result = {"error": "Downlink converter not defined", "success": False}
                except Exception as e:
                    self.__log.error("Failed to process RPC with method: %r, params: %r, config: %r - Error: %s",
                                     rpc_method, params, rpc_config, e)
                    result = {"error": str(e), "success": False}
                finally:
                    self.__rpc_in_progress.clear()
        if not processed:
            result = {"error": "Method not found", "success": False}
        return result

    def write(self, data, with_response=False, response_timeout=5):
        """
        Method to write data to the device.
        If with_response is set to True, it waits for the response from the device.
        """
        try:
            serial_conn = self.get_serial()
            if serial_conn:
                with self.__serial_lock:
                    serial_conn.write(data)
                    self.__log.debug("Written to device %s: %s", self.name, data)
                if with_response:
                    return self.__read_data_from_serial(response_timeout)
        except Exception as e:
            self.__log.exception("Failed to write to device %s: %s", self.name, e)
        return None

    def __read_data_from_serial(self, timeout=1):
        """
        Method to read data from the device.
        It reads data until the delimiter is found.
        """
        data_from_device = b''
        serial_conn = None
        try:
            serial_conn = self.get_serial()
            if serial_conn and serial_conn.is_open: 
                # Ensure serial_conn is not None before accessing its timeout attribute
                previous_timeout = serial_conn.timeout
                while not data_from_device.endswith(self.delimiter.encode('utf-8')):
                    serial_conn.timeout = timeout
                    chunk = serial_conn.read(1)
                    if chunk:
                        data_from_device += chunk
                    if self.__connector_stopped.is_set() or not chunk or self.stopped:
                        break
        except Exception as e:
            self.__log.exception("Failed to read from device %s: %s", self.name, e)
        finally:
            if serial_conn:
                serial_conn.timeout = previous_timeout
        return data_from_device

    def stop(self):
        self.stopped = True
        with self.__serial_lock:
            if self.__serial:
                self.__serial.close()
                self.__serial = None

    def is_connected_reconnect_if_needed(self):
        """
        Method to check if the device is connected.
        If the device is not connected, it tries to reconnect.
        """
        if self.__serial is None or not self.__serial.is_open:
            if monotonic() - self.__previous_connect > 1:
                self.__previous_connect = monotonic()
                self.__log.info("Reconnecting to device %s", self.name)
                self.get_serial()
                # Only return True if the serial connection is successful
                return self.__serial and self.__serial.is_open
        else:
            return True


class SerialConnector(Thread, Connector):
    """
    Serial connector class is used to represent a serial connector.
    It is used to manage devices connected to the serial ports.
    """
    def __init__(self, gateway: 'TBGatewayService', config, connector_type):
        super().__init__()
        self._connector_type = connector_type  # required to have for get connector type method
        self.__config = config  # required to have for get config method
        self.__id = self.__config["id"]  # required to have for get id method
        self.__gateway = gateway  # required to have for send data to storage method or to use other gateway methods
        self.name = self.__config["name"]  # required to have for get name method
        self.__connected = False  # required to have for is connected method
        self.__uplink_queue = Queue(self.__config.get('uplinkQueueSize', 100000))
        self._log = init_logger(self.__gateway, self.name, level=self.__config.get('logLevel'),
                                enable_remote_logging=self.__config.get('enableRemoteLogging', False),
                                is_connector_logger=True)
        self._converter_log = init_logger(self.__gateway, self.name, level=self.__config.get('logLevel'),
                                          enable_remote_logging=self.__config.get('enableRemoteLogging', False),
                                          is_converter_logger=True)
        self._log.info("Starting %s connector", self.get_name())
        self.daemon = True
        self.stopped = Event()
        self.stopped.set()
        self.__devices: List[SerialDevice] = []
        self._log.info('Connector %s initialization success.', self.get_name())

    def __start_devices(self):
        failed_to_connect_devices = len(self.__devices)
        for device in self.__devices:
            try:
                device.start()
                failed_to_connect_devices -= 1
            except Exception as e:
                self._log.exception("Failed to start device %s, error: %s", device.name, e)
        self.__connected = failed_to_connect_devices == 0

    def open(self):
        """
        Service method to start the connector.
        """
        self.stopped.clear()
        self.start()

    def get_name(self):
        return self.name

    def get_type(self):
        return self._connector_type

    def is_connected(self):
        return self.__connected

    def is_stopped(self):
        return self.stopped.is_set()

    def get_config(self):
        return self.__config

    def get_id(self):
        return self.__id

    def __load_devices(self):
        """
        Method to create devices objects using configuration file and create converters for them.
        """
        devices_config = self.__config.get('devices')
        try:
            if devices_config is not None:
                for device_config in devices_config:
                    device = None
                    uplink_converter_class_name = device_config.get('converter', device_config.get('uplink_converter'))
                    if uplink_converter_class_name is not None:
                        converter_class = TBModuleLoader.import_module(self._connector_type,
                                                                       uplink_converter_class_name)
                        uplink_converter = converter_class(device_config, self._log)
                        device = SerialDevice(device_config, uplink_converter, self.stopped,
                                              self._log, self.__uplink_queue)
                    else:
                        self._log.error('Converter configuration for the connector %s -- \
                            not found, please check your configuration file.', self.get_name())
                    if device_config.get('downlink_converter') is not None:
                        downlink_converter_class = TBModuleLoader.import_module(self._connector_type,
                                                                                device_config.get('downlink_converter'))
                        if device is not None:
                            device.downlink_converter = downlink_converter_class(device_config, self._converter_log)
                    if device is not None:
                        self.__devices.append(device)
            else:
                self._log.error('Section "devices" in the configuration not found. \
                    A connector %s has being stopped.', self.get_name())
                self.close()
        except Exception as e:
            self._log.error('Failed to load devices, error: %s', e)

    def run(self):
        """
        Main method to manage devices connected to the serial ports and process data from them.
        """
        try:
            self.__load_devices()
            self.__start_devices()
            self._log.info("Devices in configuration file found: %s ",
                           '\n'.join(device.name for device in self.__devices))
            while not self.stopped.is_set():
                try:
                    connected_devices = len(self.__devices)
                    for device in self.__devices:
                        if not device.stopped and not device.is_connected_reconnect_if_needed():
                            connected_devices -= 1
                            self._log.error("Device %s is not connected", device.name)
                            device.stop()
                            device.join()
                            device = SerialDevice(device.config, device.uplink_converter, self.stopped,
                                                  self._log, self.__uplink_queue)
                            device.start()
                    self.__connected = connected_devices == len(self.__devices)
                    if not self.__uplink_queue.empty():
                        data = self.__uplink_queue.get()
                        self.__gateway.send_to_storage(self.name, self.__id, data)
                    else:
                        sleep(0.05)
                except Exception as e:
                    self._log.error("Failed to process data from device %s, error: %s", self.name, e)
        except Exception as e:
            self._log.error("Failed to process data from device %s, error: %s", self.name, e)

    def close(self):
        """
        Service method to stop the connector and all devices connected to it.
        """
        self.stopped.set()
        for device in self.__devices:
            self.__gateway.del_device(device.name)
            device.stop()
        self._log.stop()

    def on_attributes_update(self, content):
        """
        Callback method to process attribute updates from the platform.
        """
        self._log.debug("Received attribute update: %s", content)
        device_name = content.get("device")
        if device_name is not None:
            for device in self.__devices:
                if device_name == device.name:
                    request_config = device.config.get("attributeUpdates")
                    if request_config is not None:
                        attribute_config_found = False
                        for attribute_config in request_config:
                            attribute = attribute_config.get("attributeOnPlatform")
                            if attribute is not None and attribute in content["data"]:
                                attribute_config_found = True
                                try:
                                    value = content["data"][attribute]
                                    str_to_send = str(attribute_config["stringToDevice"]
                                                      .replace("${" + attribute + "}", str(value))
                                                      .replace("${deviceName}", device_name)
                                                      .replace("${deviceType}", device.type)
                                                      ).encode("UTF-8")
                                    device.write(str_to_send)
                                except Exception as e:
                                    self._log.error("Failed to send attribute update to device %s: %s",
                                                    device_name, e)
                        if not attribute_config_found:
                            self._log.error("Attribute update configuration for key %s for device %s not found",
                                            list(content['data'].keys())[0], device_name)
                    else:
                        self._log.error("Attribute update configuration for device %s not found", device_name)
        else:
            self._log.error("Device name is not provided in the attribute update request: %s", content)

    def server_side_rpc_handler(self, content):
        """
        Callback method to process RPC requests from the platform.
        """
        self._log.debug("Received RPC request: %s", content)
        device_name = content.get("device")
        rpc_data = content.get("data", {})
        rpc_method = rpc_data.get("method")
        req_id = rpc_data.get("id")
        params = rpc_data.get("params")
        if device_name is not None:
            for device in self.__devices:
                if device_name == device.name:
                    result = device.handle_rpc_request(rpc_method, params)
                    if "error" in result:
                        self._log.error("Failed to process RPC request for device %s, error: %s",
                                        device_name, result["error"])
                    if result is not None:
                        self.__gateway.send_rpc_reply(device=device_name,
                                                      req_id=req_id,
                                                      content=result,
                                                      wait_for_publish=True,
                                                      quality_of_service=1)
        else:
            self._log.error("Device name is not provided in the RPC request: %s", content)
