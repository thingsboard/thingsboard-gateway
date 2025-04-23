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

import io
import re
from ftplib import FTP, FTP_TLS
from queue import Queue
from random import choice
from re import fullmatch
from string import ascii_lowercase
from threading import Thread
from time import perf_counter as timer, sleep

import simplejson

from thingsboard_gateway.connectors.ftp.backward_compatibility_adapter import FTPBackwardCompatibilityAdapter
from thingsboard_gateway.connectors.ftp.file import File
from thingsboard_gateway.connectors.ftp.ftp_uplink_converter import FTPUplinkConverter
from thingsboard_gateway.connectors.ftp.path import Path
from thingsboard_gateway.gateway.entities.converted_data import ConvertedData
from thingsboard_gateway.gateway.statistics.decorators import CollectAllReceivedBytesStatistics
from thingsboard_gateway.gateway.statistics.statistics_service import StatisticsService
from thingsboard_gateway.tb_utility.tb_logger import init_logger

from thingsboard_gateway.connectors.connector import Connector


class FTPConnector(Connector, Thread):
    def __init__(self, gateway, config, connector_type):
        super().__init__()
        self.statistics = {'MessagesReceived': 0,
                           'MessagesSent': 0}
        using_old_config_format_detected = FTPBackwardCompatibilityAdapter.is_old_config_format(config)
        if using_old_config_format_detected:
            self.config = FTPBackwardCompatibilityAdapter(config).convert()
            self.__id = self.config.get("id")
        else:
            self.config = config
            self.__id = self.config.get("id")

        self._connector_type = connector_type
        self.__gateway = gateway
        self.security = {**self.config['parameters']['security']} if self.config['parameters']['security'][
                                                                         'type'] == 'basic' else {
            'username': 'anonymous', "password": 'anonymous@'}
        self.__tls_support = self.config['parameters'].get("TLSSupport", False)
        self.name = self.config.get("name", "".join(choice(ascii_lowercase) for _ in range(5)))
        self.__log = init_logger(self.__gateway, self.name, self.config.get('logLevel', 'INFO'),
                                 enable_remote_logging=self.config.get('enableRemoteLogging', False),
                                 is_connector_logger=True)
        self.__converter_log = init_logger(self.__gateway, self.name + '_converter',
                                           self.config.get('logLevel', 'INFO'),
                                           enable_remote_logging=self.config.get('enableRemoteLogging', False),
                                           is_converter_logger=True, attr_name=self.name)
        self.daemon = True
        self.__stopped = False
        self.__requests_in_progress = []
        self.__convert_queue = Queue(1000000)
        self.__attribute_updates = []
        self.__fill_attributes_update()
        self._connected = False
        self.__rpc_requests = []
        self.start_time = timer()
        self.__fill_rpc_requests()
        self.host = self.config['parameters']['host']
        self.port = self.config['parameters'].get('port', 21)
        self.__ftp = FTP_TLS if self.__tls_support else FTP
        self.paths = [
            Path(
                path=obj['path'],
                read_mode=obj['readMode'],
                telemetry=obj['timeseries'],
                device_name=obj['devicePatternName'],
                attributes=obj['attributes'],
                txt_file_data_view=obj.get('txtFileDataView', 'TABLE'),
                with_sorting_files=obj.get('with_sorting_files', True),
                poll_period=obj.get('pollPeriod', 60),
                max_size=obj.get('maxFileSize', 5),
                delimiter=obj.get('delimiter', ','),
                device_type=obj.get('devicePatternType', 'Device'),
                report_strategy=obj.get('reportStrategy')
            )
            for obj in self.config['paths']
        ]
        self.__log.info('FTP Connector started.')

    def open(self):
        self.__stopped = False
        self.start()

    def run(self):
        try:
            while not self.__stopped:
                with self.__ftp() as ftp:
                    self.__connect(ftp)

                    if self._connected:
                        for path in self.paths:
                            path.find_files(ftp)

                        while True:
                            sleep(.2)
                            self.__process_paths(ftp)
                            if self.__stopped:
                                break
        except Exception as e:
            self.__log.exception(e)
            try:
                self.close()
            except Exception as e:
                self.__log.exception(e)
        while True:
            if self.__stopped:
                break

    def __connect(self, ftp):
        try:
            ftp.connect(self.host, self.port)

            if isinstance(ftp, FTP_TLS):
                ftp.sendcmd('USER ' + self.security['username'])
                ftp.sendcmd('PASS ' + self.security['password'])
                ftp.prot_p()
                self.__log.info('Data protection level set to "private"')
            else:
                ftp.login(self.security['username'], self.security['password'])

        except Exception as e:
            self.__log.error(e)
            sleep(10)
        else:
            self._connected = True
            self.__log.info('Connected to FTP server')

    def __process_paths(self, ftp):
        for path in self.paths:
            time_point = timer()
            if time_point - path.last_polled_time >= path.poll_period or path.last_polled_time == 0:
                configuration = path.config
                converter = FTPUplinkConverter(configuration, self.__converter_log)
                path.last_polled_time = time_point

                if '*' in path.path:
                    path.find_files(ftp)

                for file in path.files:
                    current_hash = file.get_current_hash(ftp)
                    if ((file.has_hash() and current_hash != file.hash)
                        or not file.has_hash()) and file.check_size_limit(ftp):
                        file.set_new_hash(current_hash)

                        handle_stream = io.BytesIO()

                        ftp.retrbinary('RETR ' + file.path_to_file, handle_stream.write)

                        handled_str = str(handle_stream.getvalue(), 'UTF-8')
                        handled_array = handled_str.split('\n')

                        StatisticsService.count_connector_message(self.name,
                                                                  stat_parameter_name='connectorMsgsReceived')
                        StatisticsService.count_connector_bytes(self.name, handled_str,
                                                                stat_parameter_name='connectorBytesReceived')

                        convert_conf = {'file_ext': file.path_to_file.split('.')[-1]}

                        if convert_conf['file_ext'] == 'json':
                            json_data = simplejson.loads(handled_str)
                            if isinstance(json_data, list):
                                for obj in json_data:
                                    converted_data = converter.convert(convert_conf, obj)

                                    if converted_data:
                                        self.__log.info(
                                            'Converted data for device %s with type %s, attributes: %s, telemetry: %s',
                                            converted_data.device_name, converted_data.device_type,
                                            converted_data.attributes_datapoints_count,
                                            converted_data.telemetry_datapoints_count)

                                        self.__log.debug('Converted data: %s', converted_data)
                                        self.__send_data(converted_data)
                            else:
                                converted_data = converter.convert(convert_conf, json_data)

                                if converted_data:
                                    self.__log.info(
                                        'Converted data for device %s with type %s, attributes: %s, telemetry: %s',
                                        converted_data.device_name, converted_data.device_type,
                                        converted_data.attributes_datapoints_count,
                                        converted_data.telemetry_datapoints_count)

                                    self.__log.debug('Converted data: %s', converted_data)
                                    self.__send_data(converted_data)
                        else:
                            cursor = file.cursor or 0

                            for (index, line) in enumerate(handled_array):
                                if index == 0 and not path.txt_file_data_view == 'SLICED':
                                    convert_conf['headers'] = line.split(path.delimiter)
                                else:
                                    if file.read_mode == File.ReadMode.PARTIAL and index >= cursor:
                                        converted_data = converter.convert(convert_conf, line)
                                        if index + 1 == len(handled_array):
                                            file.cursor = index
                                    else:
                                        converted_data = converter.convert(convert_conf, line)

                                    if converted_data:
                                        self.__log.info(
                                            'Converted data for device %s with type %s, attributes: %s, telemetry: %s',
                                            converted_data.device_name, converted_data.device_type,
                                            converted_data.attributes_datapoints_count,
                                            converted_data.telemetry_datapoints_count)

                                        self.__log.debug('Converted data: %s', converted_data)
                                        self.__send_data(converted_data)

                        handle_stream.close()

    def __send_data(self, converted_data: ConvertedData):
        if (converted_data and
                (converted_data.telemetry_datapoints_count > 0 or
                 converted_data.attributes_datapoints_count > 0)):
            self.__gateway.send_to_storage(self.name, self.get_id(), converted_data)
            self.statistics['MessagesSent'] = self.statistics['MessagesSent'] + 1
            self.__log.debug("Data to ThingsBoard: %s", converted_data)

    def close(self):
        self.__stopped = True
        self.__log.info('FTP Connector stopped.')
        self.__log.stop()

    def get_name(self):
        return self.name

    def get_id(self):
        return self.__id

    def get_type(self):
        return self._connector_type

    def is_connected(self):
        return self._connected

    def is_stopped(self):
        return self.__stopped

    def __fill_attributes_update(self):
        for attribute_request in self.config.get('requestsMapping', {}).get('attributeUpdates', []):
            self.__attribute_updates.append(attribute_request)

    @staticmethod
    def __is_json(data):
        try:
            simplejson.loads(data)
        except ValueError:
            return False
        return True

    @CollectAllReceivedBytesStatistics(start_stat_type='allReceivedBytesFromTB')
    def on_attributes_update(self, content):
        try:
            for attribute_request in self.__attribute_updates:
                if fullmatch(attribute_request["deviceNameFilter"], content["device"]):
                    attribute_key, attribute_value = content['data'].popitem()

                    path_str = attribute_request['path'].replace('${attributeKey}', attribute_key).replace(
                        '${attributeValue}', attribute_value)
                    path = Path(path=path_str, device_name=content['device'], attributes=[], telemetry=[],
                                delimiter=',', txt_file_data_view='')

                    data_expression = attribute_request['valueExpression'].replace('${attributeKey}',
                                                                                   attribute_key).replace(
                        '${attributeValue}', attribute_value)

                    with self.__ftp() as ftp:
                        self.__connect(ftp)
                        path.find_files(ftp)
                        for file in path.files:
                            if file.path_to_file.split('.')[-1] == 'json':
                                json_data = data_expression.replace("'", '"')
                                if self.__is_json(json_data):
                                    io_stream = io.BytesIO(str.encode(json_data))
                                    ftp.storbinary('STOR ' + file.path_to_file, io_stream)
                                    io_stream.close()
                                else:
                                    self.__log.error('Invalid json data')
                            else:
                                if attribute_request['writingMode'] == 'OVERRIDE':
                                    io_stream = self._get_io_stream(data_expression)
                                    ftp.storbinary('STOR ' + file.path_to_file, io_stream)
                                    io_stream.close()
                                else:
                                    handle_stream = io.BytesIO()
                                    ftp.retrbinary('RETR ' + file.path_to_file, handle_stream.write)
                                    converted_data = str(handle_stream.getvalue(), 'UTF-8')
                                    handle_stream.close()

                                    io_stream = io.BytesIO(str.encode(str(converted_data + '\n' + data_expression)))
                                    ftp.storbinary('STOR ' + file.path_to_file, io_stream)
                                    io_stream.close()

        except Exception as e:
            self.__log.exception(e)

    @CollectAllReceivedBytesStatistics('allBytesSentToDevices')
    def _get_io_stream(self, data_expression):
        return io.BytesIO(str.encode(data_expression))

    def __fill_rpc_requests(self):
        for rpc_request in self.config.get('requestsMapping', {}).get("serverSideRpc", []):
            self.__rpc_requests.append(rpc_request)

    @CollectAllReceivedBytesStatistics(start_stat_type='allReceivedBytesFromTB')
    def server_side_rpc_handler(self, content):
        try:
            self.__log.info("Incoming server-side RPC: %s", content)

            if content.get('data') is None:
                content['data'] = {'params': content['params'], 'method': content['method'], 'id': content['id']}

            rpc_method = content['data']['method']

            # check if RPC type is connector RPC
            try:
                (connector_type, rpc_method_name) = rpc_method.split('_')
                if connector_type == self._connector_type:
                    value_expression = content['data']['params']['valueExpression']
                    converted_data, success_sent = self.__process_rpc(rpc_method_name, value_expression)
                    self.__send_rpc_reply({}, content, converted_data, success_sent)
                    return
            except ValueError:
                pass

            # check if RPC method is reserved get/set
            if rpc_method == 'get' or rpc_method == 'set':
                params = {}
                for param in content['data']['params'].split(';'):
                    try:
                        (key, value) = param.split('=')
                    except ValueError:
                        continue

                    if key and value:
                        params[key] = value

                rpc_method = 'write' if rpc_method == 'set' else 'read'

                if rpc_method == 'read':
                    value_expression = params.get('filePath')
                else:
                    value_expression = params.get('filePath') + ';' + params.get('value')

                converted_data, success_sent = self.__process_rpc(rpc_method, value_expression)
                self.__send_rpc_reply({}, content, converted_data, success_sent)
                return

            for rpc_request in self.__rpc_requests:
                if fullmatch(rpc_request['methodFilter'], rpc_method):
                    value_expression = content['data']['params']['valueExpression']
                    converted_data, success_sent = self.__process_rpc(rpc_method, value_expression)

                    self.__send_rpc_reply(rpc_request, content, converted_data, success_sent)
        except Exception as e:
            self.__log.exception(e)

    def __process_rpc(self, method, value_expression):
        with self.__ftp() as ftp:
            if not self._connected or not ftp.sock:
                self.__connect(ftp)

            converted_data = None
            success_sent = None
            if method == 'write':
                try:
                    arr = re.sub("'", '', value_expression).split(';')
                    io_stream = self._get_io_stream(arr[1])
                    ftp.storbinary('STOR ' + arr[0], io_stream)
                    io_stream.close()
                    success_sent = True
                except Exception as e:
                    self.__log.error(e)
                    converted_data = '{"error": "' + str(e) + '"}'
            else:
                handle_stream = io.BytesIO()
                ftp.retrbinary('RETR ' + value_expression, handle_stream.write)
                converted_data = str(handle_stream.getvalue(), 'UTF-8')
                handle_stream.close()

            return converted_data, success_sent

    def __send_rpc_reply(self, rpc_request, content, converted_data, success_sent):
        if content.get('device') and fullmatch(rpc_request.get('deviceNameFilter', ''), content.get('device')):
            self.__gateway.send_rpc_reply(device=content["device"], req_id=content["data"]["id"],
                                          success_sent=success_sent, content={'result': converted_data})
        elif content.get('device'):
            self.__gateway.send_rpc_reply(device=content["device"], req_id=content["data"]["id"],
                                          success_sent=success_sent, content={'result': converted_data})
        else:
            return converted_data

    def get_config(self):
        return self.config
    
