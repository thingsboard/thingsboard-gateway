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

from thingsboard_gateway.connectors.ftp.file import File
from thingsboard_gateway.connectors.ftp.ftp_uplink_converter import FTPUplinkConverter
from thingsboard_gateway.connectors.ftp.path import Path
from thingsboard_gateway.gateway.statistics_service import StatisticsService
from thingsboard_gateway.tb_utility.tb_utility import TBUtility

try:
    from requests import Timeout, request
except ImportError:
    print("Requests library not found - installing...")
    TBUtility.install_package("requests")
    from requests import Timeout, request

from thingsboard_gateway.connectors.connector import Connector, log


class FTPConnector(Connector, Thread):
    def __init__(self, gateway, config, connector_type):
        super().__init__()
        self.statistics = {'MessagesReceived': 0,
                           'MessagesSent': 0}
        self.__log = log
        self.__config = config
        self._connector_type = connector_type
        self.__gateway = gateway
        self.security = {**self.__config['security']} if self.__config['security']['type'] == 'basic' else {
            'username': 'anonymous', "password": 'anonymous@'}
        self.__tls_support = self.__config.get("TLSSupport", False)
        self.setName(self.__config.get("name", "".join(choice(ascii_lowercase) for _ in range(5))))
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
        self.host = self.__config['host']
        self.port = self.__config.get('port', 21)
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
                device_type=obj.get('devicePatternType', 'Device')
                )
            for obj in self.__config['paths']
            ]

    def open(self):
        self.__stopped = False
        self.start()

    def run(self):
        try:
            with self.__ftp() as ftp:
                self.__connect(ftp)

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
                converter = FTPUplinkConverter(configuration)
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

                        convert_conf = {'file_ext': file.path_to_file.split('.')[-1]}

                        if convert_conf['file_ext'] == 'json':
                            json_data = simplejson.loads(handled_str)
                            if isinstance(json_data, list):
                                for obj in json_data:
                                    converted_data = converter.convert(convert_conf, obj)
                                    self.__gateway.send_to_storage(self.getName(), converted_data)
                                    self.statistics['MessagesSent'] = self.statistics['MessagesSent'] + 1
                                    log.debug("Data to ThingsBoard: %s", converted_data)
                            else:
                                converted_data = converter.convert(convert_conf, json_data)
                                self.__gateway.send_to_storage(self.getName(), converted_data)
                                self.statistics['MessagesSent'] = self.statistics['MessagesSent'] + 1
                                log.debug("Data to ThingsBoard: %s", converted_data)
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

                                    self.__gateway.send_to_storage(self.getName(), converted_data)
                                    self.statistics['MessagesSent'] = self.statistics['MessagesSent'] + 1
                                    log.debug("Data to ThingsBoard: %s", converted_data)

                        handle_stream.close()

    def close(self):
        self.__stopped = True

    def get_name(self):
        return self.name

    def is_connected(self):
        return self._connected

    def __fill_attributes_update(self):
        for attribute_request in self.__config.get('attributeUpdates', []):
            self.__attribute_updates.append(attribute_request)

    @staticmethod
    def __is_json(data):
        try:
            simplejson.loads(data)
        except ValueError:
            return False
        return True

    @StatisticsService.CollectAllReceivedBytesStatistics(start_stat_type='allReceivedBytesFromTB')
    def on_attributes_update(self, content):
        try:
            for attribute_request in self.__attribute_updates:
                if fullmatch(attribute_request["deviceNameFilter"], content["device"]):
                    attribute_key, attribute_value = content['data'].popitem()

                    path_str = attribute_request['path'].replace('${attributeKey}', attribute_key).replace(
                        '${attributeValue}', attribute_value)
                    path = Path(path=path_str, device_name=content['device'], attributes=[], telemetry=[],
                                delimiter=',', txt_file_data_view='')

                    data_expression = attribute_request['valueExpression'].replace('${attributeKey}', attribute_key).replace(
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
                                    log.error('Invalid json data')
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
            log.exception(e)

    @StatisticsService.CollectAllReceivedBytesStatistics('allBytesSentToDevices')
    def _get_io_stream(self, data_expression):
        return io.BytesIO(str.encode(data_expression))

    def __fill_rpc_requests(self):
        for rpc_request in self.__config.get("serverSideRpc", []):
            self.__rpc_requests.append(rpc_request)

    @StatisticsService.CollectAllReceivedBytesStatistics(start_stat_type='allReceivedBytesFromTB')
    def server_side_rpc_handler(self, content):
        try:
            for rpc_request in self.__rpc_requests:
                if fullmatch(rpc_request['deviceNameFilter'], content['device']) and fullmatch(
                        rpc_request['methodFilter'], content['data']['method']):
                    with self.__ftp() as ftp:
                        if not self._connected:
                            self.__connect(ftp)

                        converted_data = None
                        success_sent = None
                        if content['data']['method'] == 'write':
                            try:
                                arr = re.sub("'", '', content['data']['params']).split(';')
                                io_stream = self._get_io_stream(arr[1])
                                ftp.storbinary('STOR ' + arr[0], io_stream)
                                io_stream.close()
                                success_sent = True
                            except Exception as e:
                                log.error(e)
                                converted_data = '{"error": "' + str(e) + '"}'
                        else:
                            handle_stream = io.BytesIO()
                            ftp.retrbinary('RETR ' + content['data']['params'], handle_stream.write)
                            converted_data = str(handle_stream.getvalue(), 'UTF-8')
                            handle_stream.close()

                        self.__gateway.send_rpc_reply(device=content["device"], req_id=content["data"]["id"],
                                                      success_sent=success_sent, content=converted_data)
        except Exception as e:
            log.exception(e)

    def get_config(self):
        return self.__config
