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
from thingsboard_gateway.connectors.custom_connector import CustomConnector, log


class CustomSerialConnector(CustomConnector):
    def __init__(self, gateway,  config, connector_type):
        super().__init__(gateway, config, connector_type)
        self.__config = config
        self.__gateway = gateway
        self.load_converters()
        for device in self.devices:
            try:
                connection_start = time.time()
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
            except Exception as e:
                log.exception(e)
            else:
                self.__gateway.add_device(self.devices[device]["device_config"]["name"], {"connector": self})
                self.connected = True

    def run(self):
        try:
            while True:
                for device in self.devices:
                    serial = self.devices[device]["serial"]
                    ch = b''
                    data_from_device = b''
                    while ch != b'\n':
                        ch = serial.read(1)
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
        super().close()
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
