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

import logging
import logging.handlers
import threading
from queue import Full, Queue, Empty
from sys import stdout
from time import time, sleep
from typing import TYPE_CHECKING

from thingsboard_gateway.tb_utility.tb_logger import TbLogger
from thingsboard_gateway.tb_utility.tb_utility import TBUtility
if TYPE_CHECKING:
    from thingsboard_gateway.gateway.tb_gateway_service import TBGatewayService

logging.setLoggerClass(TbLogger)


class TBRemoteLoggerHandler(logging.Handler):
    LOGGER_NAME_TO_ATTRIBUTE_NAME = {
        'service': 'SERVICE_LOGS',
        'extension': 'EXTENSION_LOGS',
        'tb_connection': 'CONNECTION_LOGS',
        'storage': 'STORAGE_LOGS',
    }

    def __init__(self, gateway: 'TBGatewayService'):
        logging.setLoggerClass(TbLogger)
        self.current_log_level = self.get_logger_level_id('TRACE')
        super().__init__(self.current_log_level)
        self.setLevel(self.current_log_level)
        self.__gateway = gateway
        self.activated = False
        self.__loggers_lock = threading.Lock()

        self._max_message_count_batch = 20
        self._logs_queue = Queue(1000)

        self._send_logs_thread = None

        self.setFormatter(logging.Formatter('%(asctime)s - |%(levelname)s|<%(threadName)s> [%(filename)s] - %(module)s %(funcName)s - %(lineno)d - %(message)s')) # noqa
        self.loggers = {}
        for logger in TBRemoteLoggerHandler.LOGGER_NAME_TO_ATTRIBUTE_NAME.keys():
            self.add_logger(logger, 100)
        self.activate()

    def activate_remote_logging_for_level(self, log_level_id):
        for logger_name in TBRemoteLoggerHandler.LOGGER_NAME_TO_ATTRIBUTE_NAME.keys():
            logger, _ = self.loggers.get(logger_name)
            if logger:
                self.loggers[logger_name] = logger, log_level_id

    def get_logger(self, name):
        return self.loggers.get(name)[0]

    def add_logger(self, name, remote_logging_level):
        log = logging.getLogger(name)
        if hasattr(self.__gateway, 'main_handler') and self.__gateway.main_handler not in log.handlers:
            log.addHandler(self.__gateway.main_handler)
            log.debug("Added main handler to log %s", name)
        with self.__loggers_lock:
            self.loggers[name] = log, self.get_logger_level_id(remote_logging_level)

    def update_logger(self, name, remote_logging_level):
        if name in self.loggers:
            with self.__loggers_lock:
                self.loggers[name] = self.loggers[name][0], self.get_logger_level_id(remote_logging_level)

    def remove_logger(self, name):
        if name in self.loggers:
            with self.__loggers_lock:
                self.loggers.pop(name)

    def _send_logs(self):
        while self.activated and not self.__gateway.stopped:
            try:
                if self.__gateway.tb_client is None or not self.__gateway.tb_client.is_connected():
                    sleep(1)
                    continue
                logs_for_sending_list = []
                log_msg = self._logs_queue.get_nowait()

                count = 1
                while count <= self._max_message_count_batch:
                    try:
                        if self.__gateway.tb_client is None or not self.__gateway.tb_client.is_connected():
                            sleep(1)
                            continue
                        if log_msg is None:
                            log_msg = self._logs_queue.get_nowait()

                        logs_msg_size = TBUtility.get_data_size(log_msg)
                        if logs_msg_size > self.__gateway.get_max_payload_size_bytes():
                            print(f'Too big LOG message size to send ({logs_msg_size}). Skipping...')
                            print(log_msg)
                            continue

                        if TBUtility.get_data_size(logs_for_sending_list) + logs_msg_size > self.__gateway.get_max_payload_size_bytes(): # noqa
                            self.__gateway.send_telemetry(logs_for_sending_list)
                            logs_for_sending_list = [log_msg]
                        else:
                            logs_for_sending_list.append(log_msg)
                        log_msg = None
                        count += 1
                    except Empty:
                        break

                if logs_for_sending_list and not self.__gateway.stopped:
                    self.__gateway.send_telemetry(logs_for_sending_list)
            except (TimeoutError, Empty):
                sleep(.1)
            except Exception as e:
                log = TbLogger('service')
                log.debug("Exception while sending logs.", exc_info=e)

    def activate(self):
        if not self.activated:
            self.activated = True
            self._send_logs_thread = threading.Thread(target=self._send_logs, name='Logs Sending Thread', daemon=True)
            self._send_logs_thread.start()

    def handle(self, record):
        if self.activated and not self.__gateway.stopped:
            logger, remote_logging_level = self.loggers.get(record.name)
            if record.levelno < remote_logging_level:
                return  # Remote logging level set higher than record level

            name = logger.name

            if name:
                record = self.formatter.format(record)
                try:
                    telemetry_key = self.LOGGER_NAME_TO_ATTRIBUTE_NAME[name]
                except KeyError:
                    telemetry_key = name + '_LOGS'

                log_msg = {'ts': int(time() * 1000), 'values': {telemetry_key: record}}

                if telemetry_key in self.LOGGER_NAME_TO_ATTRIBUTE_NAME.values():
                    log_msg['values']['LOGS'] = record
                try:
                    self._logs_queue.put_nowait(log_msg)
                except Full:
                    print(f"Logs queue is full. Skipping log message: {log_msg}")
                except Exception as e:
                    print(f"Exception while putting log message to queue: {str(e)}")

    def deactivate(self):
        self.activated = False
        try:
            self._send_logs_thread.join()
        except Exception as e:
            log = TbLogger('service')
            log.debug("Exception while joining logs sending thread.", exc_info=e)

    @staticmethod
    def set_default_handler():
        logging.setLoggerClass(TbLogger)
        for logger_name in TBRemoteLoggerHandler.LOGGER_NAME_TO_ATTRIBUTE_NAME.keys():
            logger = logging.getLogger(logger_name)
            handler = logging.StreamHandler(stdout)
            handler.setFormatter(logging.Formatter('[STREAM ONLY] %(asctime)s,%(msecs)03d - %(levelname)s - [%(filename)s] - %(module)s - %(lineno)d - %(message)s')) # noqa
            logger.addHandler(handler)

    @staticmethod
    def get_logger_level_id(log_level):
        if isinstance(log_level, str):
            return 100 if log_level == 'NONE' else logging._nameToLevel.get(log_level, 100)
        return log_level
