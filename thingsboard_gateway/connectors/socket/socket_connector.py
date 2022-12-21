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

import socket
from queue import Queue
from random import choice
from re import findall
from string import ascii_lowercase
from threading import Thread
from time import sleep

from simplejson import dumps

from thingsboard_gateway.connectors.connector import Connector, log
from thingsboard_gateway.tb_utility.tb_loader import TBModuleLoader
from thingsboard_gateway.gateway.statistics_service import StatisticsService
from thingsboard_gateway.connectors.socket.socket_decorators import CustomCollectStatistics

SOCKET_TYPE = {
    'TCP': socket.SOCK_STREAM,
    'UDP': socket.SOCK_DGRAM
}
DEFAULT_UPLINK_CONVERTER = 'BytesSocketUplinkConverter'


class SocketConnector(Connector, Thread):
    def __init__(self, gateway, config, connector_type):
        super().__init__()
        self.__log = log
        self.__config = config
        self._connector_type = connector_type
        self.statistics = {'MessagesReceived': 0,
                           'MessagesSent': 0}
        self.__gateway = gateway
        self.setName(config.get("name", 'TCP Connector ' + ''.join(choice(ascii_lowercase) for _ in range(5))))
        self.daemon = True
        self.__stopped = False
        self._connected = False
        self.__bind = False
        self.__socket_type = config['type'].upper()
        self.__socket_address = config['address']
        self.__socket_port = config['port']
        self.__socket_buff_size = config['bufferSize']
        self.__socket = socket.socket(socket.AF_INET, SOCKET_TYPE[self.__socket_type])
        self.__converting_requests = Queue(-1)

        self.__devices = self.__convert_devices_list()
        self.__connections = {}

    def __convert_devices_list(self):
        devices = self.__config.get('devices', [])

        converted_devices = {}
        for device in devices:
            address = device.get('address')
            module = self.__load_converter(device)
            converter = module(
                {'deviceName': device['deviceName'],
                 'deviceType': device.get('deviceType', 'default')}) if module else None
            device['converter'] = converter

            # validate attributeRequests requestExpression
            attr_requests = device.get('attributeRequests', [])
            device['attributeRequests'] = self.__validate_attr_requests(attr_requests)
            if len(device['attributeRequests']):
                self.__attribute_type = {
                    'client': self.__gateway.tb_client.client.gw_request_client_attributes,
                    'shared': self.__gateway.tb_client.client.gw_request_shared_attributes
                }

            converted_devices[address] = device

        return converted_devices

    def __load_converter(self, device):
        converter_class_name = device.get('converter', DEFAULT_UPLINK_CONVERTER)
        module = TBModuleLoader.import_module(self._connector_type, converter_class_name)

        if module:
            log.debug('Converter %s for device %s - found!', converter_class_name, self.name)
            return module

        log.error("Cannot find converter for %s device", self.name)
        return None

    def __validate_attr_requests(self, attr_requests):
        validated_attrs = []
        for attr in attr_requests:
            valid_attr = False
            if attr['requestExpression'] != '':
                if '${' in attr['requestExpression'] and '}' in attr['requestExpression']:
                    if '==' in attr['requestExpression']:
                        expression_arr = findall(r'\[[^\s][0-9:]*]', attr['requestExpression'])
                        if expression_arr:
                            indexes = expression_arr[0][1:-1].split(':')
                            if len(indexes) == 2:
                                from_index, to_index = indexes
                                attr['requestIndexFrom'] = from_index
                                attr['requestIndexTo'] = to_index
                            else:
                                attr['requestIndex'] = int(indexes[0])

                            attr['haveIndex'] = True
                            try:
                                attr['requestEqual'] = attr['requestExpression'].split('==')[-1][:-1]
                            except IndexError:
                                self.__log.error(f'{attr["requestExpression"]} not valid. Index out of range.')
                                continue

                            valid_attr = True
                            validated_attrs.append(attr)
                else:
                    valid_attr = True
                    attr['haveIndex'] = False
                    validated_attrs.append(attr)

            if not valid_attr:
                self.__log.error(f'{attr["requestExpression"]} not valid expression')

        return validated_attrs

    def open(self):
        self.__stopped = False
        self.start()

    def run(self):
        self._connected = True

        converting_thread = Thread(target=self.__process_data, daemon=True, name='Converter Thread')
        converting_thread.start()

        while not self.__bind:
            try:
                self.__socket.bind((self.__socket_address, self.__socket_port))
            except OSError:
                log.error('Address already in use. Reconnecting...')
                sleep(3)
            else:
                self.__bind = True

        if self.__socket_type == 'TCP':
            self.__socket.listen(5)

        self.__log.info('%s socket is up', self.__socket_type)

        while not self.__stopped:
            try:
                if self.__socket_type == 'TCP':
                    conn, address = self.__socket.accept()
                    self.__connections[address] = conn

                    self.__log.debug('New connection %s established', address)
                    thread = Thread(target=self.__process_tcp_connection, daemon=True,
                                    name=f'Processing {address} connection',
                                    args=(conn, address))
                    thread.start()
                else:
                    data, client_address = self.__socket.recvfrom(self.__socket_buff_size)
                    self.__converting_requests.put((client_address, data))
            except ConnectionAbortedError:
                self.__socket.close()

    def __process_tcp_connection(self, connection, address):
        while not self.__stopped:
            data = connection.recv(self.__socket_buff_size)

            if data:
                self.__converting_requests.put((address, data))
            else:
                break

        connection.close()
        self.__connections.pop(address)
        self.__log.debug('Connection %s closed', address)

    def __process_data(self):
        while not self.__stopped:
            if not self.__converting_requests.empty():
                (address, port), data = self.__converting_requests.get()

                device = self.__devices.get(f'{address}:{port}', None)
                if not device:
                    self.__log.error('Can\'t convert data from %s:%s - not in config file', address, port)

                # check data for attribute requests
                is_attribute_request = False
                attr_requests = device.get('attributeRequests', [])
                if len(attr_requests):
                    for attr in attr_requests:
                        equal = data
                        if attr['haveIndex']:
                            if attr.get('requestIndexFrom') and attr.get('requestIndexTo'):
                                index_from = int(attr['requestIndexFrom']) if attr['requestIndexFrom'] != '' else None
                                index_to = int(attr['requestIndexTo']) if attr['requestIndexTo'] != '' else None
                                equal = data[index_from:index_to]
                            else:
                                equal = data[int(attr['requestIndex'])]

                        if attr['requestEqual'] == equal.decode('utf-8'):
                            is_attribute_request = True
                            self.__process_attribute_request(device['deviceName'], attr, data)

                    if is_attribute_request:
                        continue

                self.__convert_data(device, data)

            sleep(.2)

    def __convert_data(self, device, data):
        address, port = device['address'].split(':')
        converter = device['converter']
        if not converter:
            self.__log.error('Converter not found for %s:%s', address, port)

        try:
            device_config = {
                'encoding': device.get('encoding', 'utf-8').lower(),
                'telemetry': device.get('telemetry', []),
                'attributes': device.get('attributes', [])
            }
            converted_data = converter.convert(device_config, data)

            self.statistics['MessagesReceived'] = self.statistics['MessagesReceived'] + 1

            if converted_data is not None:
                self.__gateway.send_to_storage(self.get_name(), converted_data)
                self.statistics['MessagesSent'] = self.statistics['MessagesSent'] + 1
                log.info('Data to ThingsBoard %s', converted_data)
        except Exception as e:
            self.__log.exception(e)

    def __process_attribute_request(self, device_name, attr, data):
        expression_arr = findall(r'\[[^\s][0-9:]*]', attr['attributeNameExpression'])

        found_attributes = []
        if expression_arr:
            for exp in expression_arr:
                indexes = exp[1:-1].split(':')

                try:
                    if len(indexes) == 2:
                        from_index, to_index = indexes
                        attribute = data[int(from_index) if from_index != '' else None:int(
                            to_index) if to_index != '' else None]
                    else:
                        attribute = data[int(indexes[0])]

                    found_attributes.append(attribute.decode('utf-8'))
                except IndexError:
                    self.__log.error('Data length not valid due to attributeNameExpression')
                    return

        self.statistics['MessagesReceived'] = self.statistics['MessagesReceived'] + 1
        self.__attribute_type[attr['type']](device_name, found_attributes, self.__attribute_request_callback)

    def __attribute_request_callback(self, response, _):
        device = response.get('device')
        if not device:
            self.__log.error('Attribute request does\'t return device name')

        device = tuple(filter(lambda item: item['deviceName'] == device, self.__config['devices']))[0]
        address, port = device['address'].split(':')

        value = response.get('value') or response.get('values')
        converted_value = bytes(dumps(value), encoding='utf-8')

        if self.__socket_type == 'TCP':
            self.__write_value_via_tcp(address, port, converted_value)
        else:
            self.__write_value_via_udp(address, port, converted_value)
        self.statistics['MessagesSent'] = self.statistics['MessagesSent'] + 1

    def close(self):
        self.__stopped = True
        self._connected = False
        self.__connections = {}

    def get_name(self):
        return self.name

    def is_connected(self):
        return self._connected

    def get_config(self):
        return self.__config

    @CustomCollectStatistics(start_stat_type='allBytesSentToDevices')
    def __write_value_via_tcp(self, address, port, value):
        try:
            self.__connections[(address, int(port))].sendall(value)
            return 'ok'
        except KeyError:
            try:
                new_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                new_socket.connect((address, int(port)))
                new_socket.sendall(value)
                new_socket.close()
                return 'ok'
            except ConnectionRefusedError as e:
                self.__log.error('Can\'t connect to %s:%s', address, port)
                self.__log.exception(e)
                return e

    @staticmethod
    @StatisticsService.CollectStatistics(start_stat_type='allBytesSentToDevices')
    def __write_value_via_udp(address, port, value):
        new_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        new_socket.sendto(value, (address, int(port)))
        new_socket.close()

    @StatisticsService.CollectAllReceivedBytesStatistics(start_stat_type='allReceivedBytesFromTB')
    def on_attributes_update(self, content):
        try:
            device = tuple(filter(lambda item: item['deviceName'] == content['device'], self.__config['devices']))[0]

            for attribute_update_config in device['attributeUpdates']:
                for attribute_update in content['data']:
                    if attribute_update_config['attributeOnThingsBoard'] == attribute_update:
                        address, port = device['address'].split(':')
                        encoding = device.get('encoding', 'utf-8').lower()
                        converted_data = bytes(str(content['data'][attribute_update]), encoding=encoding)
                        self.__write_value_via_tcp(address, port, converted_data)
        except IndexError:
            self.__log.error('Device not found')

    @StatisticsService.CollectAllReceivedBytesStatistics(start_stat_type='allReceivedBytesFromTB')
    def server_side_rpc_handler(self, content):
        try:
            device = tuple(filter(lambda item: item['deviceName'] == content['device'], self.__config['devices']))[0]

            # check if RPC method is reserved set
            if content['data']['method'] == 'set':
                params = {}
                for param in content['data']['params'].split(';'):
                    try:
                        (key, value) = param.split('=')
                    except ValueError:
                        continue

                    if key and value:
                        params[key] = value

                result = None
                try:
                    if self.__socket_type == 'TCP':
                        result = self.__write_value_via_tcp(params['address'], int(params['port']), params['value'])
                    else:
                        self.__write_value_via_udp(params['address'], int(params['port']), params['value'])
                except KeyError:
                    self.__gateway.send_rpc_reply(content['device'], content['data']['id'], 'Not enough params')
                except ValueError:
                    self.__gateway.send_rpc_reply(content['device'], content['data']['id'],
                                                  'Param "port" have to be int type')
                else:
                    self.__gateway.send_rpc_reply(content['device'], content['data']['id'], str(result))
            else:
                for rpc_config in device['serverSideRpc']:
                    for (key, value) in content['data'].items():
                        if value == rpc_config['methodRPC']:
                            rpc_method = rpc_config['methodProcessing']
                            return_result = rpc_config['withResponse']
                            result = None

                            address, port = device['address'].split(':')
                            encoding = device.get('encoding', 'utf-8').lower()
                            converted_data = bytes(str(content['data']['params']), encoding=encoding)

                            if rpc_method.upper() == 'WRITE':
                                if self.__socket_type == 'TCP':
                                    result = self.__write_value_via_tcp(address, port, converted_data)
                                else:
                                    self.__write_value_via_udp(address, port, converted_data)

                            if return_result and self.__socket_type == 'TCP':
                                self.__gateway.send_rpc_reply(content['device'], content['data']['id'], str(result))

                            return
        except IndexError:
            self.__log.error('Device not found')
        except Exception as e:
            self.__log.exception(e)
