#     Copyright 2024. ThingsBoard
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
from string import ascii_lowercase
from threading import Thread
from time import sleep

from thingsboard_gateway.tb_utility.tb_loader import TBModuleLoader

from thingsboard_gateway.grpc_connectors.gw_grpc_connector import GwGrpcConnector, log
from thingsboard_gateway.grpc_connectors.gw_grpc_msg_creator import GrpcMsgCreator

SOCKET_TYPE = {
    'TCP': socket.SOCK_STREAM,
    'UDP': socket.SOCK_DGRAM
}
DEFAULT_UPLINK_CONVERTER = 'BytesGrpcSocketUplinkConverter'


class GrpcSocketConnector(GwGrpcConnector):
    def __init__(self, connector_config: str, config_dir_path: str):
        super().__init__(connector_config, config_dir_path)
        self.__config = self.connection_config['config'][list(self.connection_config['config'].keys())[0]]
        self._connector_type = 'socket'
        self.__bind = False
        self.name = self.__config.get("name", 'TCP Connector ' + ''.join(choice(ascii_lowercase) for _ in range(5)))
        self.__socket_type = self.__config['type'].upper()
        self.__socket_address = self.__config['address']
        self.__socket_port = self.__config['port']
        self.__socket_buff_size = self.__config['bufferSize']
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

    def run(self):
        try:
            while not self._grpc_client.connected:
                if self.registered:
                    if not self._grpc_client.connected:
                        self.registered = False
                        continue

                sleep(.2)

            # thread for converting data from devices
            converting_thread = Thread(target=self.__convert_data, daemon=True, name='Converter Thread')
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

            log.info('%s socket is up', self.__socket_type)

            while not self.stopped:
                try:
                    if self.__socket_type == 'TCP':
                        conn, address = self.__socket.accept()
                        self.__connections[address] = conn

                        log.debug('New connection %s established', address)
                        thread = Thread(target=self.__process_tcp_connection, daemon=True,
                                        name=f'Processing {address} connection',
                                        args=(conn, address))
                        thread.start()
                    else:
                        data, client_address = self.__socket.recvfrom(self.__socket_buff_size)
                        self.__converting_requests.put((client_address, data))
                except ConnectionAbortedError:
                    self.__socket.close()

        except KeyboardInterrupt:
            self.stop()

    def __process_tcp_connection(self, connection, address):
        while not self.stopped:
            data = connection.recv(self.__socket_buff_size)

            if data:
                self.__converting_requests.put((address, data))
            else:
                break

        connection.close()
        self.__connections.pop(address)
        log.debug('Connection %s closed', address)

    def __convert_data(self):
        while not self.stopped:
            if not self.__converting_requests.empty():
                (address, port), data = self.__converting_requests.get()

                device = self.__devices.get(f'{address}:{port}', None)
                if not device:
                    log.error('Can\'t convert data from %s:%s - not in config file', address, port)

                device_connect_message = GrpcMsgCreator.create_device_connected_msg(device['deviceName'])
                device_connected = False
                while not device_connected:
                    if not device_connected and self.registered:
                        sleep(2)
                        self._grpc_client.send(device_connect_message)
                        device_connected = True

                    sleep(.2)

                converter = device['converter']
                if not converter:
                    log.error('Converter not found for %s:%s', address, port)

                try:
                    device_config = {
                        'encoding': device.get('encoding', 'utf-8').lower(),
                        'telemetry': device.get('telemetry', []),
                        'attributes': device.get('attributes', [])
                    }
                    converted_data = converter.convert(device_config, data)

                    if converted_data is not None:
                        basic_msg = GrpcMsgCreator.get_basic_message(None)
                        telemetry = converted_data['telemetry']
                        attributes = converted_data['attributes']
                        GrpcMsgCreator.create_telemetry_connector_msg(telemetry, device_name=device['deviceName'],
                                                                      basic_message=basic_msg)
                        GrpcMsgCreator.create_attributes_connector_msg(attributes, device_name=device['deviceName'],
                                                                       basic_message=basic_msg)
                        self._grpc_client.send(basic_msg)
                        log.info('Data to ThingsBoard %s', converted_data)
                except Exception as e:
                    log.exception(e)

            sleep(.2)

    @staticmethod
    def __write_value_via_udp(address, port, value):
        new_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        new_socket.sendto(value, (address, int(port)))
        new_socket.close()

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
                log.error('Can\'t connect to %s:%s', address, port)
                log.exception(e)
                return e

    def server_side_rpc_handler(self, content):
        converted_content = {
            'device': content.deviceName,
            'data': {
                'methodRPC': content.rpcRequestMsg.methodName,
                'params': content.rpcRequestMsg.params,
                'requestId': content.rpcRequestMsg.requestId
            }
        }

        try:
            device = tuple(filter(lambda item: item['deviceName'] == converted_content['device'], self.__config['devices']))[0]

            for rpc_config in device['serverSideRpc']:
                for (key, value) in converted_content['data'].items():
                    if value == rpc_config['methodRPC']:
                        rpc_method = rpc_config['methodProcessing']
                        return_result = rpc_config['withResponse']
                        result = None

                        address, port = device['address'].split(':')
                        encoding = device.get('encoding', 'utf-8').lower()
                        converted_data = bytes(str(converted_content['data']['params']), encoding=encoding)

                        if rpc_method.upper() == 'WRITE':
                            if self.__socket_type == 'TCP':
                                result = self.__write_value_via_tcp(address, port, converted_data)
                            else:
                                self.__write_value_via_udp(address, port, converted_data)

                        if return_result and self.__socket_type == 'TCP':
                            self.send_rpc_reply(device['deviceName'], converted_content['data']['requestId'],
                                                {'result': str(result)})

                        return
        except IndexError:
            log.error('Device not found')

    def on_attributes_update(self, content):
        converted_content = {
            'deviceName': content.deviceName,
            'data': {
                content.notificationMsg.sharedUpdated[0].kv.key: content.notificationMsg.sharedUpdated[0].kv.string_v
            }
        }
        try:
            device = tuple(
                filter(lambda item: item['deviceName'] == converted_content['deviceName'], self.__config['devices'])
            )[0]

            for attribute_update_config in device['attributeUpdates']:
                for attribute_update in converted_content['data']:
                    if attribute_update_config['attributeOnThingsBoard'] == attribute_update:
                        address, port = device['address'].split(':')
                        encoding = device.get('encoding', 'utf-8').lower()
                        converted_data = bytes(str(converted_content['data'][attribute_update]), encoding=encoding)
                        self.__write_value_via_tcp(address, port, converted_data)
        except IndexError:
            log.error('Device not found')

    def unregister_connector_callback(self):
        self.registered = False

    def stop(self):
        super(GrpcSocketConnector, self).stop()
        self.__socket.shutdown(socket.SHUT_RDWR)
        self.__socket.close()

    def get_name(self):
        return self.name

    def get_type(self):
        return self._connector_type


if __name__ == '__main__':
    import sys

    connector_config = sys.argv[1]
    config_path = sys.argv[2]

    connector = GrpcSocketConnector(connector_config=connector_config, config_dir_path=config_path)
    connector.start()
