#     Copyright 2021. ThingsBoard
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

import time
from queue import Queue
from random import choice
from string import ascii_lowercase
from threading import Thread
from ftplib import FTP, FTP_TLS
import io

from thingsboard_gateway.connectors.ftp.path import Path
from thingsboard_gateway.connectors.ftp.file import File
from thingsboard_gateway.tb_utility.tb_utility import TBUtility
from thingsboard_gateway.connectors.ftp.ftp_uplink_converter import FTPUplinkConverter

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
        self.__rpc_requests = []
        self.__config = config
        self.__connector_type = connector_type
        self.__gateway = gateway
        self.security = (self.__config["security"]["username"], self.__config["security"]["password"]) if \
            self.__config["security"]["type"] == 'basic' else ('anonymous', 'anonymous@')
        self.__tls_support = self.__config.get("TLSSupport", False)
        self.setName(self.__config.get("name", "".join(choice(ascii_lowercase) for _ in range(5))))
        self.daemon = True
        self.__stopped = False
        self.__requests_in_progress = []
        self.__convert_queue = Queue(1000000)
        self.__attribute_updates = []
        self._connected = False
        self.host = self.__config['host']
        self.port = self.__config.get('port', 21)
        self.__ftp = FTP_TLS if self.__tls_support else FTP
        self.paths = [Path(path=obj['path'], with_sorting_files=True, poll_period=60, read_mode=obj['readMode'],
                           max_size=obj['maxFileSize'], delimiter=obj['delimiter'], telemetry=obj['timeseries'],
                           device_name=obj['devicePatternName'], device_type=obj.get('devicePatternType', 'Device'),
                           attributes=obj['attributes'], txt_file_data_view=obj['txtFileDataView']) for obj
                      in self.__config['paths']]
        # self.paths = [obj['path'] for obj in self.__config['paths']]

    def open(self):
        self.__stopped = False
        self.start()

    def run(self):
        try:
            with self.__ftp() as ftp:
                self.__connect(ftp)

                for path in self.paths:
                    path.find_files(ftp)

                self.__process_paths(ftp)

                while True:
                    time.sleep(.01)
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
                ftp.sendcmd('USER ' + self.security[0])
                ftp.sendcmd('PASS ' + self.security[1])
                ftp.prot_p()
                self.__log.info('Data protection level set to "private"')
            else:
                ftp.login(self.security[0], self.security[1])

        except Exception as e:
            self.__log.error(e)
            time.sleep(10)
        else:
            self._connected = True
            self.__log.info('FTP connected')

    def __process_paths(self, ftp):
        # TODO: call path on timer
        for path in self.paths:
            configuration = {
                'delimiter': path.delimiter,
                'devicePatternName': path.device_name,
                'devicePatternType': path.device_type,
                'timeseries': path.telemetry,
                'attributes': path.attributes,
                'txt_file_data_view': path.txt_file_data_view
            }
            converter = FTPUplinkConverter(configuration)
            # TODO: check if to rescan path
            for file in path.files:
                current_hash = file.get_current_hash(ftp)
                if ((file.has_hash() and current_hash != file.hash) or not file.has_hash()) and file.check_size_limit(
                        ftp):
                    file.set_new_hash(current_hash)

                    if file.read_mode == File.ReadMode.FULL:
                        handle_stream = io.BytesIO()
                        ftp.retrbinary('RETR ' + file.path_to_file, handle_stream.write)
                        handled_str = str(handle_stream.getvalue(), 'UTF-8').split('\n')

                        convert_conf = {'file_ext': file.path_to_file.split('.')[-1]}

                        for (index, line) in enumerate(handled_str):
                            if index == 0:
                                convert_conf['headers'] = line.split(path.delimiter)
                            else:
                                try:
                                    converted_data = converter.convert(convert_conf, line)
                                    self.__gateway.send_to_storage(self.getName(), converted_data)
                                    self.statistics['MessagesSent'] = self.statistics['MessagesSent'] + 1
                                    log.debug("Data to ThingsBoard: %s", converted_data)
                                except Exception as e:
                                    log.error(e)

                        handle_stream.close()
                    else:
                        handle_stream = io.BytesIO()
                        ftp.retrbinary('RETR ' + file.path_to_file, handle_stream.write)

                        lines = str(handle_stream.getvalue(), 'UTF-8').split(' ')
                        cursor = file.cursor or 0
                        for (index, line) in enumerate(lines):
                            if index >= cursor:
                                if index + 1 == len(lines):
                                    file.cursor = index

                            # TODO: add convert function

                        handle_stream.close()

    def close(self):
        self.__stopped = True

    def get_name(self):
        return self.name

    def is_connected(self):
        return self._connected

    def on_attributes_update(self, content):
        pass

    def server_side_rpc_handler(self, content):
        pass
