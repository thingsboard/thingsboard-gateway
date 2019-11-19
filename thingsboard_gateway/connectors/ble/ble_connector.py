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

from bluepy import __path__ as bluepy_path
from pprint import pformat
from bluepy.btle import DefaultDelegate, Peripheral, Scanner
from bluepy.btle import BTLEDisconnectError, BTLEManagementError, BTLEGattError
from random import choice
from string import ascii_lowercase
import time
from threading import Thread
from thingsboard_gateway.connectors.connector import Connector, log


class BLEConnector(Connector, Thread):
    def __init__(self, gateway, config):
        super().__init__()
        self.__default_service_end = '-0000-1000-8000-00805F9B34FB'
        self.statistics = {'MessagesReceived': 0,
                           'MessagesSent': 0}
        self.__gateway = gateway
        self.__config = config
        self.setName(self.__config.get("name",
                                       'BLE Connector ' + ''.join(choice(ascii_lowercase) for _ in range(5))))

        self._connected = False
        self.__stopped = False
        self.__previous_scan_time = 0
        self.__rescan_time = self.__config['rescanIntervalSeconds'] if self.__config.get(
            'rescanIntervalSeconds') is not None else 1
        self.__scanner = Scanner().withDelegate(ScanDelegate(self))
        self.__devices_around = {}
        self.__notify_delegators = {}
        self.__fill_interest_devices()
        self.daemon = True

    def run(self):
        while True:
            if time.time() - self.__previous_scan_time >= self.__rescan_time != 0:
                self.__scan_ble()
                # for device_addr in self.__devices_found:
                #     device = self.__devices_found[device_addr]
                #     # log.debug('%s\t\t%s', device.addr, device.rssi)
                #     scanned_data = device.getScanData()
                #     try:
                #         log.debug(scanned_data[-1][-1])
                #     except Exception as e:
                #         log.exception(e)
                self.__get_services_and_chars()
            time.sleep(.1)
            if self.__stopped:
                log.debug('STOPPED')
                break

    def close(self):
        self.__stopped = True
        for device in self.__devices_around:
            try:
                self.__devices_around[device]['peripheral'].disconnect()
            except Exception as e:
                log.exception(e)
                raise e

    def get_name(self):
        return self.name

    def on_attributes_update(self, content):
        pass

    def server_side_rpc_handler(self, content):
        pass

    def is_connected(self):
        return self._connected

    def open(self):
        self.__stopped = False
        self.start()

    def device_add(self, device):
        for interested_device in self.__devices_around:
            if device.addr.upper() == interested_device:
                self.__devices_around[interested_device]['scanned_device'] = device
                log.debug('Device with address: %s - found.', device.addr.upper())
            else:
                log.debug('Unknown device with address: %s - found.', device.addr.upper())

    def __get_services_and_chars(self):
        for device in self.__devices_around:
            try:
                if self.__devices_around.get(device) is not None and self.__devices_around[device].get(
                        'scanned_device') is not None:
                    log.debug('Connecting to device with address: %s',
                              self.__devices_around[device]['scanned_device'].addr.upper())
                    new_device = False
                    if self.__devices_around[device].get('peripheral') is None:
                        peripheral = Peripheral(self.__devices_around[device]['scanned_device'])
                        self.__devices_around[device]['peripheral'] = peripheral
                        new_device = True
                    else:
                        peripheral = self.__devices_around[device]['peripheral']
                        peripheral.connect(self.__devices_around[device]['scanned_device'])
                    services = peripheral.getServices()
                    for service in services:
                        if self.__devices_around[device].get('services') is None:
                            log.debug('Building device %s map, it may take a time, please wait...', device)
                            self.__devices_around[device]['services'] = {}
                        service_uuid = str(service.uuid).upper()
                        if self.__devices_around[device]['services'].get(service_uuid) is None:
                            self.__devices_around[device]['services'][service_uuid] = {}
                            for characteristic in service.getCharacteristics():
                                descriptors = []
                                try:
                                    self.__check_and_reconnect(device)
                                    try:
                                        descriptors = characteristic.getDescriptors()
                                    except BTLEDisconnectError:
                                        self.__check_and_reconnect(device)
                                        descriptors = characteristic.getDescriptors()
                                    except BTLEGattError:
                                        log.debug('Device %s - services, characteristics and descriptors found.', device)
                                    except Exception as e:
                                        log.exception(e)
                                    characteristic_uuid = str(characteristic.uuid).upper()
                                    self.__devices_around[device]['services'][service_uuid][characteristic_uuid] = {
                                        'characteristic': characteristic,
                                        'descriptors': {}}
                                    for descriptor in descriptors:
                                        self.__devices_around[device]['services'][service_uuid][characteristic_uuid][
                                            'descriptors'] = {str(descriptor.uuid).upper(): descriptor}
                                except BTLEDisconnectError:
                                    self.__check_and_reconnect(device)
                    if new_device:
                        self.__new_device_processing(device)
                    self.__service_processing(device)

            except BTLEDisconnectError:
                log.debug('Cannot connect to device.')
                continue
            except Exception as e:
                log.exception(e)
            self.__previous_scan_time = time.time()

    def __new_device_processing(self, device):
        if self.__config.get('readStandardInformation', False):
            log.debug(pformat(self.__devices_around[device]))
            # TODO READ STANDARD INFORMATION

    def __check_and_reconnect(self, device):
        while self.__devices_around[device]['peripheral']._helper is None:
            self.__devices_around[device]['peripheral'].connect(self.__devices_around[device]['scanned_device'])

    def __notify_handler(self, device, notify_handle, delegate=None):
        class NotifyDelegate(DefaultDelegate):
            def __init__(self):
                DefaultDelegate.__init__(self)
                self.device = device
                self.incoming_data = b''
                self.data = {}

            def handleNotification(self, handle, data):
                self.incoming_data = data
                self.data = data
                log.debug('Notification received handle, data:')
                log.debug(handle)
                log.debug(data)

        if delegate is None:
            delegate = NotifyDelegate()
        device['peripheral'].withDelegate(delegate)
        device['peripheral'].writeCharacteristic(notify_handle, b'\x01\x00', True)
        if device['peripheral'].waitForNotifications(1):
            log.debug("Data received: %s", delegate.data)
        return delegate

    def __service_processing(self, device):
        for uuid in self.__devices_around[device]['interest_uuid']:
            for service in self.__devices_around[device]['services']:
                if uuid in self.__devices_around[device]['services'][service]:
                    characteristic = self.__devices_around[device]['services'][service][uuid]['characteristic']

                    log.debug('Characteristic with name: "%s" and uuid: %s - processing.', characteristic.uuid.getCommonName(), uuid)
                    self.__check_and_reconnect(device)
                    characteristic_processing_conf = self.__devices_around[device]['interest_uuid'][uuid]['section_config']
                    if 'READ' in characteristic.propertiesToString():
                        self.__check_and_reconnect(device)
                        data = characteristic.read()
                        log.debug(data)
            # for desc in self.__devices_around[device]['peripheral'].getDescriptors():
            #     try:
            #         if self.__devices_around[device]['peripheral']._helper is None:
            #             self.__devices_around[device]['peripheral'].connect(self.__devices_around[device]['scanned_device'])
            #         if desc.uuid.getCommonName() == 'Service Changed':
            #
            #             log.debug(desc)
            #             # log.debug(desc.read())
            #             log.debug(desc.uuid)
            #             log.debug(desc.handle)
            #             self.__notify_delegators[str(desc.uuid).upper()] = self.__notify_handler(self.__devices_around[device],
            #                                                                                                int.from_bytes(bytes.fromhex(str(desc.handle)), "big"),
            #                                                                                                self.__notify_delegators.get(str(desc.uuid).upper()))
            #     except Exception as e:
            #         log.error(e)
            # for characteristic in self.__devices_around[device]['services'][service_uuid]['characteristic']:
            #     self.__check_and_reconnect(device)
            #     log.debug(characteristic)
            #     log.debug(characteristic.propertiesToString())
            #     characteristic_properties = characteristic.propertiesToString()
            #     if 'NOTIFY' in characteristic.propertiesToString() or 'READ' in characteristic.propertiesToString():
            #         descriptors = characteristic.getDescriptors()
            #         for descriptor in descriptors:
            #             try:
            #                 if 'READ' in characteristic_properties:
            #                     log.debug(descriptor.handle)
            #                     if descriptor.handle != 17 and descriptor.handle != 26 and descriptor.handle != 43 and descriptor.handle != 47:
            #                         self.__check_and_reconnect(device)
            #                         log.debug(descriptor.read())
            #                 elif 'NOTIFY' in characteristic_properties:
            #                     log.debug(characteristic.handle)
            #                     if descriptor.handle != 17 and descriptor.handle != 26 and descriptor.handle != 43:
            #                         if self.__devices_around[device]['peripheral']._helper is None:
            #                             self.__devices_around[device]['peripheral'].connect(
            #                                 self.__devices_around[device]['scanned_device'])
            #                         self.__notify_delegators[str(descriptor.uuid).upper()] = self.__notify_handler(
            #                             self.__devices_around[device],
            #                             descriptor.handle + 1,
            #                             self.__notify_delegators.get(str(descriptor.uuid).upper()))
            #                         self.__notify_delegators[descriptor.handler] = {'function': self.__notify_handler,
            #                                                                         'args': (
            #                                                                         self.__devices_around[device],
            #                                                                         descriptor.handle + 1,
            #                                                                         self.__notify_delegators.get(
            #                                                                             str(descriptor.uuid).upper()))
            #                                                                         }
            #             except Exception as e:
            #                 log.exception(e)

    def __scan_ble(self):
        log.debug("Scanning for devices...")
        try:
            self.__scanner.scan(self.__config.get('scanTimeSeconds', 5), passive=self.__config.get('passiveScanMode', False))
        except BTLEManagementError as e:
            log.error('BLE working only with root user.')
            log.error('Or you can try this command:\nsudo setcap '
                      '\'cap_net_raw,cap_net_admin+eip\' %s'
                      '\n====== Attention! ====== '
                      '\nCommand above - provided access to ble devices to any user.'
                      '\n========================', str(bluepy_path[0] + '/bluepy-helper'))
            self._connected = False
            raise e
        except Exception as e:
            log.exception(e)
            time.sleep(10)

    def __fill_interest_devices(self):
        for interest_device in self.__config.get('devices'):
            keys_in_config = ['attributes', 'telemetry']
            if interest_device.get('MACAddress') is not None:
                interest_uuid = {}
                for key_type in keys_in_config:
                    for type_section in interest_device.get(key_type):
                        if type_section.get("characteristicUUID") is not None:
                            interest_uuid[type_section["characteristicUUID"].upper()] = {'section_config': type_section,
                                                                                         'type': key_type}
                if self.__devices_around.get(interest_device['MACAddress'].upper()) is None:
                    self.__devices_around[interest_device['MACAddress'].upper()] = {}
                self.__devices_around[interest_device['MACAddress'].upper()]['interest_uuid'] = interest_uuid
            else:
                log.error("Device address not found, please check your settings.")


class ScanDelegate(DefaultDelegate):
    def __init__(self, ble_connector):
        DefaultDelegate.__init__(self)
        self.__connector = ble_connector

    def handleDiscovery(self, dev, is_new_device, is_new_data):
        if is_new_device:
            self.__connector.device_add(dev)
