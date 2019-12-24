#     Copyright 2019. ThingsBoard
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

import serial
import time
from threading import Thread
from random import choice
from string import ascii_lowercase
from thingsboard_gateway.connectors.connector import Connector, log
from thingsboard_gateway.tb_utility.tb_utility import TBUtility


class CustomSerialConnector(Thread, Connector):
    def __init__(self, gateway,  config, connector_type):
        super().__init__()
        self.statistics = {'MessagesReceived': 0,
                           'MessagesSent': 0}
        self.__config = config
        self.__gateway = gateway
        self.__connector_type = connector_type
        self.__config = config
        self.__gateway = gateway
        self.setName(self.__config.get("name",
                                       "Custom %s connector " % self.get_name() + ''.join(choice(ascii_lowercase) for _ in range(5))))
        log.info("Starting Custom %s connector", self.get_name())
        self.daemon = True
        self.stopped = True
        self.connected = False
        self.devices = {}
        self.load_converters()
        self.__connect_to_devices()
        log.info('Custom connector %s initialization success.', self.get_name())
        log.info("Devices in configuration file found: %s ", '\n'.join(device for device in self.devices))

    def __connect_to_devices(self):
        for device in self.devices:
            try:
                connection_start = time.time()
                if self.devices[device].get("serial") is None \
                        or self.devices[device]["serial"] is None \
                        or not self.devices[device]["serial"].isOpen():
                    self.devices[device]["serial"] = None
                    while self.devices[device]["serial"] is None or not self.devices[device]["serial"].isOpen():
                        self.devices[device]["serial"] = serial.Serial(
                                 port=self.__config.get('port', '/dev/ttyUSB0'),
                                 baudrate=self.__config.get('baudrate', 9600),
                                 bytesize=self.__config.get('bytesize', serial.EIGHTBITS),
                                 parity=self.__config.get('parity', serial.PARITY_NONE),
                                 stopbits=self.__config.get('stopbits', serial.STOPBITS_ONE),
                                 timeout=self.__config.get('timeout', 1),
                                 xonxoff=self.__config.get('xonxoff', False),
                                 rtscts=self.__config.get('rtscts', False),
                                 write_timeout=self.__config.get('write_timeout', None),
                                 dsrdtr=self.__config.get('dsrdtr', False),
                                 inter_byte_timeout=self.__config.get('inter_byte_timeout', None),
                                 exclusive=self.__config.get('exclusive', None)
                        )
                        time.sleep(.1)
                        if time.time() - connection_start > 10:
                            log.error("Connection refused per timeout for device %s", self.devices[device]["device_config"].get("name"))
                            break
            except serial.serialutil.SerialException:
                log.error("Port %s for device %s - not found", self.__config.get('port', '/dev/ttyUSB0'), device)
                time.sleep(10)
            except Exception as e:
                log.exception(e)
                time.sleep(10)
            else:
                self.__gateway.add_device(self.devices[device]["device_config"]["name"], {"connector": self})
                self.connected = True

    def open(self):
        self.stopped = False
        self.start()

    def get_name(self):
        return self.name

    def is_connected(self):
        return self.connected

    def load_converters(self):
        devices_config = self.__config.get('devices')
        try:
            if devices_config is not None:
                for device_config in devices_config:
                    if device_config.get('converter') is not None:
                        converter = TBUtility.check_and_import(self.__connector_type, device_config['converter'])
                        self.devices[device_config['name']] = {'converter': converter(device_config),
                                                               'device_config': device_config}
                    else:
                        log.error('Converter configuration for the custom connector %s -- not found, please check your configuration file.', self.get_name())
            else:
                log.error('Section "devices" in the configuration not found. A custom connector %s has being stopped.', self.get_name())
                self.close()
        except Exception as e:
            log.exception(e)

    def run(self):
        try:
            while True:
                for device in self.devices:
                    serial = self.devices[device]["serial"]
                    ch = b''
                    data_from_device = b''
                    while ch != b'\n':
                        try:
                            ch = serial.read(1)
                        except AttributeError:
                            if serial is None:
                                self.__connect_to_devices()
                        data_from_device = data_from_device + ch
                    try:
                        converted_data = self.devices[device]['converter'].convert(self.devices[device]['device_config'], data_from_device)
                        self.__gateway.send_to_storage(self.get_name(), converted_data)
                        time.sleep(.1)
                    except Exception as e:
                        log.exception(e)
                        self.close()
                        raise e
        except Exception as e:
            log.exception(e)

    def close(self):
        self.stopped = True
        for device in self.devices:
            self.__gateway.del_device(self.devices[device])
            if self.devices[device]['serial'].isOpen():
                self.devices[device]['serial'].close()

    def on_attributes_update(self, content):
        log.debug(content)
        if self.devices.get(content["device"]) is not None:
            device_config = self.devices[content["device"]].get("device_config")
            if device_config is not None:
                log.debug(device_config)
                if device_config.get("attributeUpdates") is not None:
                    requests = device_config["attributeUpdates"]
                    for request in requests:
                        attribute = request.get("attributeOnThingsBoard")
                        log.debug(attribute)
                        if attribute is not None and attribute in content["data"]:
                            try:
                                value = content["data"][attribute]
                                str_to_send = str(request["stringToDevice"].replace("${" + attribute + "}", str(value))).encode("UTF-8")
                                self.devices[content["device"]]["serial"].write(str_to_send)
                                log.debug("Attribute update request to device %s : %s", content["device"], str_to_send)
                                time.sleep(.01)
                            except Exception as e:
                                log.exception(e)

    def server_side_rpc_handler(self, content):
        pass
