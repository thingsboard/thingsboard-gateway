#     Copyright 2022. ThingsBoard
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

"""Import libraries"""

import time
from random import choice
from string import ascii_lowercase
from threading import Thread
from thingsboard_gateway.tb_utility.tb_utility import TBUtility

try:
    import serial
except ImportError:
    print("pyserial library not found - installing...")
    TBUtility.install_package("pyserial")
    import serial

from thingsboard_gateway.connectors.connector import Connector, log  # Import base class for connector and logger
from thingsboard_gateway.tb_utility.tb_loader import TBModuleLoader


class CustomSerialConnector(Thread, Connector):  # Define a connector class, it should inherit from "Connector" class.
    def __init__(self, gateway, config, connector_type):
        super().__init__()  # Initialize parents classes
        self.statistics = {'MessagesReceived': 0,
                           'MessagesSent': 0}  # Dictionary, will save information about count received and sent messages.
        self.__config = config  # Save configuration from the configuration file.
        self.__gateway = gateway  # Save gateway object, we will use some gateway methods for adding devices and saving data from them.
        self.setName(self.__config.get("name",
                                       "Custom %s connector " % self.get_name() + ''.join(
                                           choice(ascii_lowercase) for _ in range(5))))  # get from the configuration or create name for logs.
        log.info("Starting Custom %s connector", self.get_name())  # Send message to logger
        self.daemon = True  # Set self thread as daemon
        self.stopped = True  # Service variable for check state
        self.__connected = False  # Service variable for check connection to device
        self.__devices = {}  # Dictionary with devices, will contain devices configurations, converters for devices and serial port objects
        self.__load_converters(connector_type)  # Call function to load converters and save it into devices dictionary
        self.__connect_to_devices()  # Call function for connect to devices
        log.info('Custom connector %s initialization success.', self.get_name())  # Message to logger
        log.info("Devices in configuration file found: %s ", '\n'.join(device for device in self.__devices))  # Message to logger

    def __connect_to_devices(self):  # Function for opening connection and connecting to devices
        for device in self.__devices:
            try:  # Start error handler
                connection_start = time.time()
                if self.__devices[device].get("serial") is None \
                        or self.__devices[device]["serial"] is None \
                        or not self.__devices[device]["serial"].isOpen():  # Connect only if serial not available earlier or it is closed.
                    self.__devices[device]["serial"] = None
                    while self.__devices[device]["serial"] is None or not self.__devices[device]["serial"].isOpen():  # Try connect
                        # connection to serial port with parameters from configuration file or default
                        device_config = self.__devices[device]["device_config"]
                        self.__devices[device]["serial"] = serial.Serial(port=device_config.get('port', '/dev/ttyUSB0'),
                                                                         baudrate=device_config.get('baudrate', 9600),
                                                                         bytesize=device_config.get('bytesize', serial.EIGHTBITS),
                                                                         parity=device_config.get('parity', serial.PARITY_NONE),
                                                                         stopbits=device_config.get('stopbits', serial.STOPBITS_ONE),
                                                                         timeout=device_config.get('timeout', 1),
                                                                         xonxoff=device_config.get('xonxoff', False),
                                                                         rtscts=device_config.get('rtscts', False),
                                                                         write_timeout=device_config.get('write_timeout', None),
                                                                         dsrdtr=device_config.get('dsrdtr', False),
                                                                         inter_byte_timeout=device_config.get('inter_byte_timeout', None),
                                                                         exclusive=device_config.get('exclusive', None))
                        time.sleep(.1)
                        if time.time() - connection_start > 10:  # Break connection try if it setting up for 10 seconds
                            log.error("Connection refused per timeout for device %s", device_config.get("name"))
                            break
            except serial.serialutil.SerialException:
                log.error("Port %s for device %s - not found", self.__devices[device]["device_config"].get('port', '/dev/ttyUSB0'), device)
            except Exception as e:
                log.exception(e)
            else:  # if no exception handled - add device and change connection state
                self.__gateway.add_device(self.__devices[device]["device_config"]["name"], {"connector": self}, self.__devices[device]["device_config"]["type"])
                self.__connected = True

    def open(self):  # Function called by gateway on start
        self.stopped = False
        self.start()

    def get_name(self):  # Function used for logging, sending data and statistic
        return self.name

    def is_connected(self):  # Function for checking connection state
        return self.__connected

    def __load_converters(self, connector_type):  # Function for search a converter and save it.
        devices_config = self.__config.get('devices')
        try:
            if devices_config is not None:
                for device_config in devices_config:
                    if device_config.get('converter') is not None:
                        converter = TBModuleLoader.import_module(connector_type, device_config['converter'])
                        self.__devices[device_config['name']] = {'converter': converter(device_config),
                                                                 'device_config': device_config}
                    else:
                        log.error('Converter configuration for the custom connector %s -- not found, please check your configuration file.', self.get_name())
            else:
                log.error('Section "devices" in the configuration not found. A custom connector %s has being stopped.', self.get_name())
                self.close()
        except Exception as e:
            log.exception(e)

    def run(self):  # Main loop of thread
        try:
            while not self.stopped:
                for device in self.__devices:
                    device_serial_port = self.__devices[device]["serial"]
                    received_character = b''
                    data_from_device = b''
                    while not self.stopped and received_character != b'\n':  # We will read until receive LF symbol
                        try:
                            received_character = device_serial_port.read(1)  # Read one symbol per time
                        except AttributeError as e:
                            if device_serial_port is None:
                                self.__connect_to_devices()  # if port not found - try to connect to it
                                raise e
                        except Exception as e:
                            log.exception(e)
                            break
                        else:
                            data_from_device = data_from_device + received_character
                    try:
                        if len(data_from_device) > 0:
                            converted_data = self.__devices[device]['converter'].convert(self.__devices[device]['device_config'], data_from_device)
                            self.__gateway.send_to_storage(self.get_name(), converted_data)
                        time.sleep(.1)
                    except Exception as e:
                        log.exception(e)
                        self.close()
                        raise e
                if not self.__connected:
                    break
        except Exception as e:
            log.exception(e)

    def close(self):  # Close connect function, usually used if exception handled in gateway main loop or in connector main loop
        self.stopped = True
        for device in self.__devices:
            self.__gateway.del_device(self.__devices[device]["device_config"]["name"])
            if self.__devices[device]['serial'].isOpen():
                self.__devices[device]['serial'].close()

    def on_attributes_update(self, content):  # Function used for processing attribute update requests from ThingsBoard
        log.debug(content)
        if self.__devices.get(content["device"]) is not None:
            device_config = self.__devices[content["device"]].get("device_config")
            if device_config is not None and device_config.get("attributeUpdates") is not None:
                requests = device_config["attributeUpdates"]
                for request in requests:
                    attribute = request.get("attributeOnThingsBoard")
                    log.debug(attribute)
                    if attribute is not None and attribute in content["data"]:
                        try:
                            value = content["data"][attribute]
                            str_to_send = str(request["stringToDevice"].replace("${" + attribute + "}", str(value))).encode("UTF-8")
                            self.__devices[content["device"]]["serial"].write(str_to_send)
                            log.debug("Attribute update request to device %s : %s", content["device"], str_to_send)
                            time.sleep(.01)
                        except Exception as e:
                            log.exception(e)

    def server_side_rpc_handler(self, content):
        pass
