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
from bluepy.btle import DefaultDelegate, Peripheral, Scanner, UUID, capitaliseName
from bluepy.btle import BTLEDisconnectError, BTLEManagementError, BTLEGattError
from random import choice
from string import ascii_lowercase
import time
from threading import Thread
from thingsboard_gateway.connectors.connector import Connector, log


class BLEConnector(Connector, Thread):
    def __init__(self, gateway, config):
        super().__init__()
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
            if device.addr.upper() == interested_device and self.__devices_around[interested_device].get('scanned_device') is None:
                self.__devices_around[interested_device]['scanned_device'] = device
                self.__devices_around[interested_device]['is_new_device'] = True
            log.debug('Device with address: %s - found.', device.addr.upper())

    def __get_services_and_chars(self):
        for device in self.__devices_around:
            try:
                if self.__devices_around.get(device) is not None and self.__devices_around[device].get('scanned_device') is not None:
                    log.debug('Connecting to device with address: %s', self.__devices_around[device]['scanned_device'].addr.upper())
                    if self.__devices_around[device].get('peripheral') is None:
                        peripheral = Peripheral(self.__devices_around[device]['scanned_device'])
                        self.__devices_around[device]['peripheral'] = peripheral
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

                            try:
                                characteristics = service.getCharacteristics()
                            except BTLEDisconnectError:
                                self.__check_and_reconnect(device)
                                characteristics = service.getCharacteristics()

                            if self.__config.get('buildDevicesMap', False):
                                for characteristic in characteristics:
                                    descriptors = []
                                    try:
                                        self.__check_and_reconnect(device)
                                        try:
                                            descriptors = characteristic.getDescriptors()
                                        except BTLEDisconnectError:
                                            self.__check_and_reconnect(device)
                                            descriptors = characteristic.getDescriptors()
                                        except BTLEGattError as e:
                                            log.debug(e)
                                        except Exception as e:
                                            log.exception(e)
                                        characteristic_uuid = str(characteristic.uuid).upper()
                                        if self.__devices_around[device]['services'][service_uuid].get(characteristic_uuid) is None:
                                            self.__devices_around[device]['services'][service_uuid][characteristic_uuid] = {
                                                'characteristic': characteristic,
                                                'handle': characteristic.handle,
                                                'descriptors': {}}
                                        for descriptor in descriptors:
                                            log.debug(descriptor.handle)
                                            log.debug(str(descriptor.uuid))
                                            log.debug(str(descriptor))
                                            self.__devices_around[device]['services'][service_uuid][characteristic_uuid]['descriptors'][descriptor.handle] = descriptor
                                    except BTLEDisconnectError:
                                        self.__check_and_reconnect(device)
                            else:
                                for characteristic in characteristics:
                                    characteristic_uuid = str(characteristic.uuid).upper()
                                    self.__devices_around[device]['services'][service_uuid][characteristic_uuid] = {
                                        'characteristic': characteristic,
                                        'handle': characteristic.handle}

                    if self.__devices_around[device]['is_new_device']:
                        log.debug('New device %s - processing.', device)
                        self.__devices_around[device]['is_new_device'] = False
                        self.__new_device_processing(device)
                    # for interest_char in self.__devices_around['interest_uuid']:
                    #     config = self.__devices_around[device]['interest_uuid'][interest_char]['section_config']
                    #     data = self.__service_processing(device, config)
                    #     log.debug(data)


            except BTLEDisconnectError:
                log.debug('Cannot connect to device.')
                continue
            except Exception as e:
                log.exception(e)
            self.__previous_scan_time = time.time()

    def __new_device_processing(self, device):
        device_default_attributes = {}
        default_services = [x for x in range(0x1800, 0x183A)]
        default_services_on_device = [service for service in self.__devices_around[device]['services'].keys() if int(service.split('-')[0], 16) in default_services]
        log.debug('Default services found on device %s :%s', device, default_services_on_device)
        for service in default_services_on_device:
            characteristics = [char for char in self.__devices_around[device]['services'][service].keys() if self.__devices_around[device]['services'][service][char]['characteristic'].supportsRead()]
            for char in characteristics:
                read_config = {'characteristicUUID': char,
                               'method': 'READ',
                               }
                try:
                    self.__check_and_reconnect(device)
                    data = self.__service_processing(device, read_config)
                    attribute = capitaliseName(UUID(char).getCommonName())
                    try:
                        data = data.decode('UTF-8')
                    except Exception as e:
                        log.debug(e)
                    device_default_attributes[attribute] = data
                except Exception as e:
                    log.debug('Cannot process %s', e)
                    continue
        log.debug(pformat(device_default_attributes))
        if self.__config.get('readDefaultAttributes', False):
            pass
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
                log.debug('Notification received from device %s handle, data:', self.device)
                log.debug(handle)
                log.debug(data)

        if delegate is None:
            delegate = NotifyDelegate()
        device['peripheral'].withDelegate(delegate)
        device['peripheral'].writeCharacteristic(notify_handle, b'\x01\x00', True)
        if device['peripheral'].waitForNotifications(1):
            log.debug("Data received: %s", delegate.data)
        return delegate

    def __service_processing(self, device, characteristic_processing_conf=None):
        for service in self.__devices_around[device]['services']:
            characteristic_uuid_from_config = characteristic_processing_conf.get('characteristicUUID')
            if characteristic_uuid_from_config is None:
                log.error('Characteristic not found in config: %s', pformat(characteristic_processing_conf))
                return
            if self.__devices_around[device]['services'][service].get(characteristic_uuid_from_config) is None:
                continue
            characteristic = self.__devices_around[device]['services'][service][characteristic_uuid_from_config]['characteristic']
            self.__check_and_reconnect(device)
            data = None
            if characteristic_processing_conf.get('method', '_').upper().split()[0] == "READ":
                if characteristic.supportsRead():
                    self.__check_and_reconnect(device)
                    data = characteristic.read()
                    log.debug(data)
                else:
                    log.error('This characteristic doesn\'t support "READ" method.')
            if characteristic_processing_conf.get('method', '_').upper().split()[0] == "NOTIFY":
                self.__check_and_reconnect(device)
                descriptor = characteristic.getDescriptors(forUUID=0x2902)[0]
                handle = descriptor.handle
                if self.__notify_delegators.get(device) is None:
                    self.__notify_delegators[device] = {}
                if self.__notify_delegators[device].get(handle) is None:
                    self.__notify_delegators[device][handle] = {'function': self.__notify_handler,
                                                                'args': (
                                                                 self.__devices_around[device],
                                                                 handle,
                                                                 self.__notify_delegators[device].get(handle)),
                                                                'delegate': None,
                                                                }
                    self.__notify_delegators[device][handle]['delegate'] = self.__notify_delegators[device][handle]['function'](*self.__notify_delegators[device][handle]['args'])
                else:
                    self.__notify_delegators[device][handle]['args'] = (self.__devices_around[device], handle, self.__notify_delegators[device][handle]['delegate'])
                    self.__notify_delegators[device][handle]['delegate'] = self.__notify_delegators[device][handle]['function'](*self.__notify_delegators[device][handle]['args'])
                data = b'FROM_NOTIFY'
            if data is None:
                log.error('Cannot process characteristic: %s with config:\n%s', str(characteristic.uuid).upper(), pformat(characteristic_processing_conf))
            else:
                log.debug('data: %s', data)
            return data

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
                        elif type_section.get("handle") is not None:
                            interest_uuid[type_section["handle"]] = {'section_config': type_section,
                                                                     'type': key_type}
                        else:
                            log.error("No characteristicUUID or handle found in configuration section for %s:\n%s\n", key_type, pformat(type_section))
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
