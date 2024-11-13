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
from time import monotonic
from threading import RLock

from thingsboard_gateway.gateway.statistics.statistics_service import StatisticsService

TRACE_LOGGING_LEVEL = 5
logging.addLevelName(TRACE_LOGGING_LEVEL, "TRACE")


def init_logger(gateway, name, level, enable_remote_logging=False, is_connector_logger=False,
                is_converter_logger=False, connector_name=None):
    """
    For creating a Logger with all config automatically
    Create a Logger manually only if you know what you are doing!
    """

    log = logging.getLogger(name)
    log.is_connector_logger = is_connector_logger
    log.is_converter_logger = is_converter_logger

    if connector_name:
        log.connector_name = connector_name

    if enable_remote_logging:
        from thingsboard_gateway.tb_utility.tb_handler import TBLoggerHandler
        remote_handler = TBLoggerHandler(gateway)
        log.addHandler(remote_handler)
        log.setLevel(gateway.main_handler.level)
        remote_handler.add_logger(name)
        remote_handler.activate()

    if hasattr(gateway, 'main_handler'):
        log.addHandler(gateway.main_handler)
        log.setLevel(gateway.main_handler.level)

    log_level_conf = level
    if log_level_conf:
        log_level = logging.getLevelName(log_level_conf)

        try:
            log.setLevel(log_level)
        except ValueError:
            log.setLevel(logging.NOTSET)

    return log


class TbLogger(logging.Logger):
    ALL_ERRORS_COUNT = 0
    IS_ALL_ERRORS_COUNT_RESET = False
    RESET_ERRORS_PERIOD = 60
    SEND_ERRORS_PERIOD = 5

    __PREVIOUS_ALL_ERRORS_COUNT = -1
    __PREVIOUS_BATCH_TO_SEND = {}

    ERRORS_MUTEX = RLock()
    ERRORS_BATCH = {}

    PREVIOUS_ERRORS_SENT_TIME = 0
    PREVIOUS_ERRORS_RESET_TIME = 0

    def __init__(self, name, level=logging.NOTSET, is_connector_logger=False, is_converter_logger=False,
                 connector_name=None):
        super(TbLogger, self).__init__(name=name, level=level)
        self.propagate = True
        self.parent = self.root
        self.__previous_number_of_errors = -1
        self.__is_connector_logger = is_connector_logger
        self.__is_converter_logger = is_converter_logger
        self.__previous_reset_errors_time = TbLogger.PREVIOUS_ERRORS_RESET_TIME
        logging.Logger.trace = TbLogger.trace

        if connector_name:
            self.__connector_name = connector_name
        else:
            self.__connector_name = name

        self.errors = 0
        self.attr_name = self.__connector_name + '_ERRORS_COUNT'
        self._is_on_init_state = True

    @property
    def is_connector_logger(self):
        return self.__is_connector_logger

    @is_connector_logger.setter
    def is_connector_logger(self, value):
        self.__is_connector_logger = value

    @property
    def is_converter_logger(self):
        return self.__is_converter_logger

    @is_converter_logger.setter
    def is_converter_logger(self, value):
        self.__is_converter_logger = value

    @property
    def connector_name(self):
        return self.__connector_name

    @connector_name.setter
    def connector_name(self, value):
        self.__connector_name = value
        self.attr_name = self.__connector_name + '_ERRORS_COUNT'

    def reset(self):
        with TbLogger.ERRORS_MUTEX:
            TbLogger.ALL_ERRORS_COUNT = TbLogger.ALL_ERRORS_COUNT - self.errors
        self.errors = 0
        self._update_errors_batch()

    def stop(self):
        with TbLogger.ERRORS_MUTEX:
            TbLogger.ERRORS_BATCH.pop(self.attr_name, None)
        self.reset()

        self.handlers.clear()

    def _send_errors(self):
        with TbLogger.ERRORS_MUTEX:
            TbLogger.ERRORS_BATCH[self.attr_name] = 0
        self._is_on_init_state = False

    def trace(self, msg, *args, **kwargs):
        if self.isEnabledFor(TRACE_LOGGING_LEVEL):
            self._log(TRACE_LOGGING_LEVEL, msg, args, **kwargs)

    def error(self, msg, *args, **kwargs):
        kwargs['stacklevel'] = 2
        super(TbLogger, self).error(msg, *args, **kwargs)

        if self.__is_connector_logger:
            StatisticsService.count_connector_message(self.name, 'connectorsErrors')
        if self.__is_converter_logger:
            StatisticsService.count_connector_message(self.name, 'convertersErrors')

        self._add_error()
        self._update_errors_batch()

    def exception(self, msg, *args, **kwargs) -> None:
        attr_name = kwargs.pop('attr_name', None)
        kwargs['stacklevel'] = 2
        super(TbLogger, self).exception(msg, *args, **kwargs)

        if self.__is_connector_logger:
            StatisticsService.count_connector_message(self.name, 'connectorsErrors')
        if self.__is_converter_logger:
            StatisticsService.count_connector_message(self.name, 'convertersErrors')

        self._add_error()
        self._update_errors_batch(error_attr_name=attr_name)

    def _add_error(self):
        with TbLogger.ERRORS_MUTEX:
            TbLogger.ALL_ERRORS_COUNT += 1
            if TbLogger.PREVIOUS_ERRORS_RESET_TIME != self.__previous_reset_errors_time:
                self.__previous_reset_errors_time = TbLogger.PREVIOUS_ERRORS_RESET_TIME
                self.errors = 0
        self.errors += 1

    def _update_errors_batch(self, error_attr_name=None):
        if error_attr_name:
            error_attr_name = error_attr_name + '_ERRORS_COUNT'
        else:
            error_attr_name = self.attr_name
        TbLogger.ERRORS_BATCH[error_attr_name] = self.errors

    @classmethod
    def send_errors_if_needed(cls, gateway):
        current_monotonic = monotonic()
        if (gateway.tb_client is not None
                and gateway.tb_client.is_connected()
                and (current_monotonic - cls.PREVIOUS_ERRORS_SENT_TIME >= cls.SEND_ERRORS_PERIOD
                     or cls.PREVIOUS_ERRORS_SENT_TIME == 0)):
            if (current_monotonic - cls.PREVIOUS_ERRORS_RESET_TIME >= cls.RESET_ERRORS_PERIOD
                    or cls.PREVIOUS_ERRORS_RESET_TIME == 0):
                cls.PREVIOUS_ERRORS_RESET_TIME = current_monotonic
                with cls.ERRORS_MUTEX:
                    for key in cls.ERRORS_BATCH:
                        cls.ERRORS_BATCH[key] = 0
                    cls.ALL_ERRORS_COUNT = 0
            cls.PREVIOUS_ERRORS_SENT_TIME = current_monotonic
            batch_to_send = {}
            with cls.ERRORS_MUTEX:
                if cls.ERRORS_BATCH:
                    batch_to_send = cls.ERRORS_BATCH
                if cls.__PREVIOUS_ALL_ERRORS_COUNT != cls.ALL_ERRORS_COUNT:
                    batch_to_send['ALL_ERRORS_COUNT'] = cls.ALL_ERRORS_COUNT
                    cls.__PREVIOUS_ALL_ERRORS_COUNT = cls.ALL_ERRORS_COUNT
            if batch_to_send:
                previous_batch_keys = set(cls.__PREVIOUS_BATCH_TO_SEND.keys())
                batch_keys = set(batch_to_send.keys())
                keys_to_check = set(previous_batch_keys).union(batch_keys)
                batch_sending_required = False
                for key in keys_to_check:
                    if cls.__PREVIOUS_BATCH_TO_SEND.get(key) != batch_to_send.get(key):
                        batch_sending_required = True
                        break
                if batch_sending_required:
                    gateway.send_telemetry(batch_to_send)
                    with cls.ERRORS_MUTEX:
                        keys_to_remove = previous_batch_keys - batch_keys
                        for key in keys_to_remove:
                            cls.__PREVIOUS_BATCH_TO_SEND.pop(key, None)
                        cls.__PREVIOUS_BATCH_TO_SEND.update(batch_to_send)

logging.setLoggerClass(TbLogger)
