#     Copyright 2026. ThingsBoard
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
from thingsboard_gateway.tb_utility.tb_logger import init_logger, TbLogger

from thingsboard_gateway.connectors.connector import Connector
from thingsboard_gateway.tb_utility.tb_loader import TBModuleLoader
from thingsboard_gateway.tb_utility.tb_utility import TBUtility


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
        self.paths = self.__fill_ftp_path_parameters()
        self.__log.info("FTP Connector started with %s and %d", self.host, self.port)

    def open(self):
        self.__stopped = False
        self.start()

    def run(self):
        self.__log.debug('Starting connector loop')
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
            self.__log.error("Unexpected exception in loop for %s %d with error %r", self.host, self.port, str(e))
            self.__log.debug("Error:", exc_info=e)
            try:
                self.close()
            except Exception as e:
                self.__log.error(
                    "Can not close the connection for %s %d with error %r", self.host, self.port, str(e))
                self.__log.debug("Error:", exc_info=e)
        while True:
            if self.__stopped:
                break

    def __fill_ftp_path_parameters(self):
        path_parameters_list = []
        path_config_list = self.config.get('paths', [])
        if not isinstance(path_config_list, list):
            self.__log.error("path_config_list must be a list, but got %s", type(path_config_list))
            return path_config_list

        for path in path_config_list:
            if not isinstance(path, dict):
                self.__log.error("path_config_list must be a dict, but got %s", type(path))
                continue
            try:
                base_path_config = {
                    "path": path['path'],
                    "read_mode": path['readMode'],
                    "txt_file_data_view": path.get('txtFileDataView', 'TABLE'),
                    "logger": self.__log,
                    "with_sorting_files": path.get('with_sorting_files', True),
                    "poll_period": path.get('pollPeriod', 60),
                    "max_size": path.get('maxFileSize', 5),
                    "delimiter": path.get('delimiter', ','),
                    "report_strategy": path.get('reportStrategy')
                }
                if isinstance(path.get("converter"), dict) and path["converter"].get("type"):
                    ext_conf = path["converter"].get("extension-config") or {}
                    base_path_config['telemetry'] = ext_conf.get('timeseries')
                    base_path_config['attributes'] = ext_conf.get('attributes')
                    base_path_config['device_name'] = ext_conf['devicePatternName']
                    base_path_config['device_type'] = ext_conf.get('devicePatternType', 'Device')

                    custom_path_config = {
                        "custom_converter_type": path["converter"].get("type"),
                        "extension": path["converter"]["extension"]
                    }

                    path_parameters_list.append(Path(**base_path_config, **custom_path_config))
                else:
                    base_path_config['telemetry'] = path.get('timeseries')
                    base_path_config['attributes'] = path.get('attributes')
                    base_path_config['device_name'] = path['devicePatternName']
                    base_path_config['device_type'] = path.get('devicePatternType', 'Device')
                    path_parameters_list.append(Path(**base_path_config))
            except KeyError as e:
                self.__log.debug("Failed to extract path arguments for path %s", str(e))
                continue

        return path_parameters_list

    def __connect(self, ftp):
        self.__log.debug("Connecting to ftp server on %s:%d", self.host, self.port)
        try:
            ftp.connect(self.host, self.port)

            if isinstance(ftp, FTP_TLS):
                ftp.sendcmd('USER ' + self.security['username'])
                ftp.sendcmd('PASS ' + self.security['password'])
                ftp.prot_p()
                self.__log.info('Data protection level set to "private"')
            else:
                ftp.login(self.security['username'], self.security['password'])
                self.__log.info("Logged in as %s", str(self.security['username']))

        except Exception as e:
            self.__log.error("Connection failed to %s:%d: due to %r", self.host, self.port, str(e))
            self.__log.debug("Error:", exc_info=e)
            sleep(10)
        else:
            self._connected = True
            self.__log.info("Connected to FTP server to %s:%d", self.host, self.port)

    def __process_paths(self, ftp):
        for path in self.paths:
            time_point = timer()
            if time_point - path.last_polled_time >= path.poll_period or path.last_polled_time == 0:
                configuration = path.config

                if path.custom_converter_type == "custom":
                    try:
                        module = TBModuleLoader.import_module(
                            self._connector_type,
                            path.extension
                        )
                        if module:
                            self.__log.debug("Custom converter loaded")
                            converter = module(configuration, self.__converter_log)
                        else:
                            self.__log.error(
                                "Could not find extension module for %s. "
                                "Make sure your extension is under the right path and exists.",
                                path.extension,
                            )
                            continue
                    except Exception as e:
                        self.__log.error(
                            "Failed to load custom converter for type %s with error %s",
                            path.custom_converter_type,
                            e,
                        )
                        continue
                else:
                    converter = FTPUplinkConverter(configuration, self.__converter_log)

                path.last_polled_time = time_point

                if '*' in path.path:
                    path.find_files(ftp)
                    self.__log.trace("Found %d for pattern %s", len(path.files), path.path)

                for file in path.files:
                    current_hash = file.get_current_hash(ftp)

                    if not self.__is_file_hash_changed(file, current_hash):
                        self.__log.info("File %s hash not changed, skipping...", file.path_to_file)
                        continue

                    if not file.check_size_limit(ftp):
                        self.__log.warning("File %s size is larger than the maximum allowed size of %d MB, skipping...",
                                           file.path_to_file, file.max_size)
                        continue

                    file.set_new_hash(current_hash)

                    self._on_file_preprocessing(ftp, self.__log, file)

                    handle_stream = io.BytesIO()
                    ftp.retrbinary('RETR ' + file.path_to_file, handle_stream.write)

                    handled_str = str(handle_stream.getvalue(), 'UTF-8')
                    handled_array = handled_str.split('\n')

                    StatisticsService.count_connector_message(self.name,
                                                              stat_parameter_name='connectorMsgsReceived')
                    StatisticsService.count_connector_bytes(self.name, handled_str,
                                                            stat_parameter_name='connectorBytesReceived')

                    convert_conf = {'file_ext': file.path_to_file.split('.')[-1]}

                    self.__log.trace("Processing data from %s file", file.path_to_file)

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
                    self._on_file_postprocessing(ftp, self.__log, file)

    def __is_file_hash_changed(self, file: File, current_hash: str):
        return (file.has_hash() and current_hash != file.hash) or not file.has_hash()

    def __send_data(self, converted_data: ConvertedData):
        if (converted_data and
                (converted_data.telemetry_datapoints_count > 0 or
                 converted_data.attributes_datapoints_count > 0)):
            self.__gateway.send_to_storage(self.name, self.get_id(), converted_data)
            self.statistics['MessagesSent'] = self.statistics['MessagesSent'] + 1
            self.__log.debug("Data being sent to ThingsBoard: %s", converted_data)

    @staticmethod
    def _on_file_preprocessing(ftp: FTP, log: TbLogger, file: File):
        # Hook called before a file is processed. Override in subclasses if needed.
        log.trace("Retrieving file %s...", file.path_to_file)

    @staticmethod
    def _on_file_postprocessing(ftp: FTP, log: TbLogger, file: File):
        # Hook called after a file has been processed. Override in subclasses if needed.
        pass

    def close(self):
        self.__stopped = True
        for path in self.paths:
            for file in path.files:
                file._hash = None
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
        self.__log.debug("Processing an attribute update from %r", content)
        try:
            for attribute_request in self.__attribute_updates:
                if fullmatch(attribute_request["deviceNameFilter"], content["device"]):
                    attribute_key, attribute_value = content['data'].popitem()
                    self.__log.info("Found an attribute key for %s and value %s", attribute_key, attribute_value)

                    path_str = attribute_request['path'].replace('${attributeKey}', attribute_key).replace(
                        '${attributeValue}', attribute_value)
                    path = Path(path=path_str, device_name=content['device'], attributes=[], telemetry=[],
                                delimiter=',', txt_file_data_view='')

                    data_expression = (attribute_request['valueExpression']
                                       .replace('${attributeKey}', attribute_key)
                                       .replace('${attributeValue}', attribute_value))

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
                                    self.__log.error("Invalid json data in attribute update %s", json_data)
                                    self.__log.debug("Error:", exc_info=True)
                            else:
                                if attribute_request['writingMode'] == 'OVERRIDE':
                                    io_stream = self._get_io_stream(data_expression)
                                    ftp.storbinary('STOR ' + file.path_to_file, io_stream)
                                    io_stream.close()
                                    self.__log.info(
                                        "Successfully process attribute update %s on override request", attribute_key)
                                else:
                                    handle_stream = io.BytesIO()
                                    ftp.retrbinary('RETR ' + file.path_to_file, handle_stream.write)
                                    converted_data = str(handle_stream.getvalue(), 'UTF-8')
                                    handle_stream.close()

                                    io_stream = io.BytesIO(str.encode(str(converted_data + '\n' + data_expression)))
                                    ftp.storbinary('STOR ' + file.path_to_file, io_stream)
                                    io_stream.close()
                                    self.__log.info("Successfully process attribute update for %s", attribute_key)

        except Exception as e:
            self.__log.error("Failed to process attribute update with error %r", str(e))
            self.__log.debug("Error:", exc_info=e)

    @CollectAllReceivedBytesStatistics('allBytesSentToDevices')
    def _get_io_stream(self, data_expression):
        return io.BytesIO(str.encode(data_expression))

    def __fill_rpc_requests(self):
        for rpc_request in self.config.get('requestsMapping', {}).get("serverSideRpc", []):
            self.__rpc_requests.append(rpc_request)

    @CollectAllReceivedBytesStatistics(start_stat_type='allReceivedBytesFromTB')
    def server_side_rpc_handler(self, content):
        try:
            self.__log.debug("Handling incoming server-side RPC with %s", content)

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
                self.__log.info("Successfully sent RPC request to FTP for %s rpc method", rpc_method)
                return

            for rpc_request in self.__rpc_requests:
                if not fullmatch(rpc_request['deviceNameFilter'], content['device']):
                    continue
                if fullmatch(rpc_request['methodFilter'], rpc_method):
                    params_field_expression = rpc_request['valueExpression']

                    value_expression_key = TBUtility.get_value(params_field_expression, content['data'], get_tag=True)
                    value_expression = content['data'][value_expression_key]
                    converted_data, success_sent = self.__process_rpc(rpc_method, value_expression)

                    self.__send_rpc_reply(rpc_request, content, converted_data, success_sent)
                    self.__log.info("Successfully sent RPC request to FTP for %s rpc method", rpc_method)

        except Exception as e:
            self.__log.error(
                "Failed to perform incoming server side RPC for content %s and rpc method due to %r", str(e))
            self.__log.debug("Error:", exc_info=e)

    def __process_rpc(self, method, value_expression):
        self.__log.info("Called __process_rpc, %r %r", method, value_expression)
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
                    converted_data = {"result": {"value": arr[1]}} if len(arr[1]) < 80 else {"result": True}
                    self.__log.info("The value %s is written to %s", arr[1], arr[0])
                except Exception as e:
                    self.__log.error("Can not process for method write due to %r", str(e))
                    self.__log.debug("Error:", exc_info=e)
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
