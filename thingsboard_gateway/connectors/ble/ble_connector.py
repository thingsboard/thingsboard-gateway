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
import tb_ble_adapter
import logging
from bluepy.btle import DefaultDelegate, Peripheral, Scanner, BTLEDisconnectError

import random
from string import ascii_lowercase
import time
import codecs
from threading import Thread
from struct import unpack
from bluepy.btle import DefaultDelegate, Peripheral, Scanner
from thingsboard_gateway.connectors.connector import Connector

logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s - %(levelname)s - %(module)s - %(lineno)d - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')
log = logging.getLogger(__name__)


class BLEConnector(Connector, Thread):
    def __init__(self, gateway, config):
        super().__init__()
        self.statistics = {'MessagesReceived': 0,
                           'MessagesSent': 0}
        self.__gateway = gateway
        self.__config = config
        self.setName(self.__config.get("name",
                                       'BLE Connector ' + ''.join(random.choice(ascii_lowercase) for _ in range(5))))

        self._connected = False
        self.__stopped = False
        self.__previous_scan_time = 0
        self.__rescan_time = self.__config['rescanIntervalSeconds'] if self.__config.get('rescanIntervalSeconds') is not None else 1
        self.__scanner = Scanner().withDelegate(ScanDelegate(self))
        self.__devices_around = {}
        self.__devices_found = {}
        self.__notify_delegators = {}
        self.__fill_interest_devices()
        self.daemon = True

    def run(self):
        while True:
            if time.time() - self.__previous_scan_time >= self.__rescan_time != 0:
                self.__scan_ble()
                for device_addr in self.__devices_found:
                    device = self.__devices_found[device_addr]
                    log.debug('%s\t\t%s', device.addr, device.rssi)
                    scanned_data = device.getScanData()
                    try:
                        log.debug(scanned_data[-1][-1])
                    except Exception as e:
                        log.exception(e)
                self.__get_services_and_chars()
            time.sleep(.1)
            if self.__stopped:
                log.debug('STOPPED')
                break

    def close(self):
        self.__stopped = True

    def get_name(self):
        return self.name

    def on_attributes_update(self, content):
        pass

    def server_side_rpc_handler(self, content):
        pass

    def open(self):
        self.__stopped = False
        self.start()

    def device_add(self, device):
        for interested_device in self.__devices_around:
            if device.addr.upper() == interested_device:
                self.__devices_around[interested_device]['scanned_device'] = device
            else:
                if device.addr not in self.__devices_found:
                    self.__devices_found[device.addr] = device
                    log.debug('Device with address: %s - found.', device.addr.upper())

    def __get_services_and_chars(self):
        for device in self.__devices_around:
            try:
                if self.__devices_around.get(device) is not None and self.__devices_around[device].get('scanned_device') is not None:
                    log.debug('Trying connect to device with address: %s',
                              self.__devices_around[device]['scanned_device'].addr.upper())
                    if self.__devices_around[device].get('peripheral') is None:
                        peripheral = Peripheral(self.__devices_around[device]['scanned_device'])
                        self.__devices_around[device]['peripheral'] = peripheral
                    else:
                        peripheral = self.__devices_around[device]['peripheral']
                        peripheral.connect(self.__devices_around[device]['scanned_device'])
                    services = peripheral.getServices()
                    for service in services:
                        if self.__devices_around[device].get('services') is None:
                            self.__devices_around[device]['services'] = {}
                        service_uuid = str(service.uuid).upper()
                        if self.__devices_around[device]['services'].get(service_uuid) is None:
                            for characteristic in service.getCharacteristics():
                                log.debug('\n\nCharacteristic %s found in device %s,\n mode: %s,\n Characteristic name: %s,\n Characteristic UUID: %s,\n Characteristic handle: %s\n',
                                          characteristic,
                                          device,
                                          characteristic.propertiesToString(),
                                          characteristic.uuid.getCommonName(),
                                          characteristic.uuid,
                                          characteristic.handle)
                                log.debug('service uuid: %s', service_uuid)
                                service_available = self.__devices_around[device]['services'].get(service_uuid)
                                if service_available is None:
                                    self.__devices_around[device]['services'][service_uuid] = {"service_object": service,
                                                                                               "characteristic": [characteristic],
                                                                                               }
                                else:
                                    service_available['characteristic'].append(characteristic)
                    self.__service_processing()

            except BTLEDisconnectError:
                log.debug('Cannot connect to device.')
                continue

    def __notify_handler(self, device, notify_handle, delegate=None):
        class NotifyDelegate(DefaultDelegate):
            def __init__(self):
                DefaultDelegate.__init__(self)
                self.device = device
                self.incoming_data = b''
                self.data = {}

            def handleNotification(self, handle, data):
                self.incoming_data = data
                log.debug(handle)
                fromByte = 0
                toByte = -1
                # self.data = {"temperature": data.decode('UTF-8')[2:6], "humidity": data.decode('UTF-8')[9:13]}
                self.data = {"temperature": data.decode('UTF-8')}
        if delegate is None:
            delegate = NotifyDelegate()
        device['peripheral'].withDelegate(delegate)
        device['peripheral'].writeCharacteristic(notify_handle, b'\x01\x00', True)
        if device['peripheral'].waitForNotifications(1):
            log.debug("Data received: %s", delegate.data)
        return delegate

    def __service_processing(self):
        for device in self.__devices_around:
            for service_uuid in self.__devices_around[device]['interest_services']:
                if self.__devices_around[device]['peripheral']._helper is None:
                    self.__devices_around[device]['peripheral'].connect(self.__devices_around[device]['scanned_device'])
                service_processing_conf = self.__devices_around[device]['interest_services'][service_uuid]
                log.debug('Service with UUID: %s processing for %s key %s',
                          service_uuid,
                          service_processing_conf['type'],
                          service_processing_conf['section_config']['key'])
                for characteristic in self.__devices_around[device]['services'][service_uuid]['characteristic']:
                    if self.__devices_around[device]['peripheral']._helper is None:
                        self.__devices_around[device]['peripheral'].connect(self.__devices_around[device]['scanned_device'])

                    log.debug(characteristic)
                    log.debug(characteristic.propertiesToString())
                    if 'READ' not in characteristic.propertiesToString() and 'NOTIFY' in characteristic.propertiesToString():
                        self.__notify_delegators[str(characteristic.uuid).upper()] = self.__notify_handler(
                            self.__devices_around[device],
                            characteristic.handle+1,
                            self.__notify_delegators.get(str(characteristic.uuid).upper()))
                    else:
                        try:
                            data = characteristic.read()
                            log.debug(data)
                            # data = {"temperature": data.decode('UTF-8')[2:6], "humidity": data.decode('UTF-8')[9:13]}
                            data = data.decode('UTF-8')
                            log.debug('data: %s', data)
                        except:
                            pass

    def __scan_ble(self):
        log.debug("Scanning for devices...")
        try:
            devices = self.__scanner.scan(15)
            log.debug(devices)
        except BTLEDisconnectError:
            pass
        except Exception as e:
            log.exception(e)

    def __fill_interest_devices(self):
        for interest_device in self.__config.get('devices'):
            keys_in_config = ['attributes', 'telemetry']
            if interest_device.get('MACAddress') is not None:
                interest_services = {}
                for key_type in keys_in_config:
                    for type_section in interest_device.get(key_type):
                        if type_section.get("serviceId") is not None:
                            interest_services[type_section["serviceId"].upper()] = {'section_config': type_section,
                                                                                    'type': key_type}
                if self.__devices_around.get(interest_device['MACAddress'].upper()) is None:
                    self.__devices_around[interest_device['MACAddress'].upper()] = {}
                self.__devices_around[interest_device['MACAddress'].upper()]['interest_services'] = interest_services
            else:
                log.error("Device address not found, please check your settings.")


class ScanDelegate(DefaultDelegate):
    def __init__(self, ble_connector):
        DefaultDelegate.__init__(self)
        self.__connector = ble_connector

    def handleDiscovery(self, dev, is_new_device, is_new_data):
        if is_new_device:
            self.__connector.device_add(dev)

    def handleNotification(self, handle, data):
        log.debug('Received notification for handle: %s', handle)
        log.debug(data)


if __name__ == '__main__':
    test_config = {
                    "name": "BLE Connector",
                    "rescanIntervalSeconds": 10,
                    "devices": [
                        {
                            "name": "Temperature and humidity sensor",
                            "notify": True,
                            "MACAddress": "EC:C8:BB:6E:05:67",
                            "checkIntervalMillis": 10000,
                            "telemetry": [
                                    {
                                        "key": "temperature",
                                        "serviceId": "00001800-0000-1000-8000-00805F9B34FB",
                                        "fromNotify": True,
                                        "fromByte": 0,
                                        "toByte": -1,
                                    }
                                ],
                            "attributes": [
                                    {
                                        "key": "batteryLevel",
                                        "handle": 0x15,
                                    }
                                ]
                        }
                    ]
                }

    connector = BLEConnector(None, test_config)
    try:
        connector.open()
        log.debug('Started')
    except Exception as e:
        log.exception(e)
        log.error(e)
        connector.close()
    while True:
        time.sleep(.1)
        # connector.close()
        # break
