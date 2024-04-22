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

import logging
import threading
import logging.handlers
from sys import stdout
from time import time, sleep
from os import environ
from queue import Queue, Empty

from thingsboard_gateway.tb_utility.tb_logger import TbLogger


class TBLoggerHandler(logging.Handler):
    LOGGER_NAME_TO_ATTRIBUTE_NAME = {
        'service': 'SERVICE_LOGS',
        'extension': 'EXTENSION_LOGS',
        'tb_connection': 'CONNECTION_LOGS',
        'storage': 'STORAGE_LOGS',
    }

    def __init__(self, gateway):
        self.current_log_level = 'INFO'
        super().__init__(logging.getLevelName(self.current_log_level))
        self.setLevel(logging.getLevelName('DEBUG'))
        self.__gateway = gateway
        self.activated = False

        self._max_message_count_batch = 20
        self._logs_queue = Queue(1000)

        self._send_logs_thread = threading.Thread(target=self._send_logs, name='Logs Sending Thread', daemon=True)

        self.setFormatter(logging.Formatter('%(asctime)s - |%(levelname)s| - [%(filename)s] - %(module)s - %(lineno)d - %(message)s'))
        self.loggers = ['service',
                        'extension',
                        'tb_connection',
                        'storage'
                        ]
        for logger in self.loggers:
            log = TbLogger(name=logger, gateway=gateway)
            log.addHandler(self.__gateway.main_handler)
            log.debug("Added remote handler to log %s", logger)

    def add_logger(self, name):
        log = TbLogger(name)
        log.addHandler(self.__gateway.main_handler)
        log.debug("Added remote handler to log %s", name)

    def _send_logs(self):
        while self.activated and not self.__gateway.stopped:
            if not self._logs_queue.empty():
                logs_for_sending_list = []

                count = 1
                while count <= self._max_message_count_batch:
                    try:
                        log_msg = self._logs_queue.get(block=False)

                        logs_msg_size = self.__gateway.get_data_size(log_msg)
                        if logs_msg_size > self.__gateway.get_max_payload_size_bytes():
                            print(f'Too big LOG message size to send ({logs_msg_size}). Skipping...')
                            continue

                        if self.__gateway.get_data_size(
                                logs_for_sending_list) + logs_msg_size > self.__gateway.get_max_payload_size_bytes():
                            self.__gateway.tb_client.client.send_telemetry(logs_for_sending_list)
                            logs_for_sending_list = [log_msg]
                        else:
                            logs_for_sending_list.append(log_msg)

                        count += 1
                    except Empty:
                        break

                if logs_for_sending_list:
                    self.__gateway.tb_client.client.send_telemetry(logs_for_sending_list)

            sleep(1)

    def activate(self, log_level=None):
        try:
            for logger in self.loggers:
                if log_level is not None and logging.getLevelName(log_level) is not None:
                    if logger == 'tb_connection' and log_level == 'DEBUG':
                        log = TbLogger(logger, gateway=self.__gateway)
                        log.setLevel(logging.getLevelName('INFO'))
                    else:
                        log = TbLogger(logger, gateway=self.__gateway)
                        self.current_log_level = log_level
                        log.setLevel(logging.getLevelName(log_level))
        except Exception as e:
            log = TbLogger('service')
            log.exception(e)
        self.activated = True
        self._send_logs_thread = threading.Thread(target=self._send_logs, name='Logs Sending Thread', daemon=True)
        self._send_logs_thread.start()

    def handle(self, record):
        if self.activated and not self.__gateway.stopped:
            name = record.name
            record = self.formatter.format(record)
            try:
                telemetry_key = self.LOGGER_NAME_TO_ATTRIBUTE_NAME[name]
            except KeyError:
                telemetry_key = name + '_LOGS'

            self._logs_queue.put({'ts': int(time() * 1000), 'values': {telemetry_key: record, 'LOGS': record}})

    def deactivate(self):
        self.activated = False
        try:
            self._send_logs_thread.join()
        except Exception as e:
            log = TbLogger('service')
            log.debug("Exception while joining logs sending thread.", exc_info=e)

    @staticmethod
    def set_default_handler():
        logger_names = [
            'service',
            'storage',
            'extension',
            'tb_connection'
            ]
        for logger_name in logger_names:
            logger = TbLogger(logger_name)
            handler = logging.StreamHandler(stdout)
            handler.setFormatter(logging.Formatter('[STREAM ONLY] %(asctime)s - %(levelname)s - [%(filename)s] - %(module)s - %(lineno)d - %(message)s'))
            logger.addHandler(handler)


class TimedRotatingFileHandler(logging.handlers.TimedRotatingFileHandler):
    def __init__(self, filename, when='h', interval=1, backupCount=0,
                 encoding=None, delay=False, utc=False):
        config_path = environ.get('TB_GW_LOGS_PATH')
        if config_path:
            filename = config_path + '/' + filename.split('/')[-1]

        super().__init__(filename, when=when, interval=interval, backupCount=backupCount,
                         encoding=encoding, delay=delay, utc=utc)
