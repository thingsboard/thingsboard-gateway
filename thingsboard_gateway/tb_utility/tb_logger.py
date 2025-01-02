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
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from thingsboard_gateway.gateway.tb_gateway_service import TBGatewayService

from thingsboard_gateway.tb_utility.tb_rotating_file_handler import TimedRotatingFileHandler
from thingsboard_gateway.gateway.statistics.statistics_service import StatisticsService

TRACE_LOGGING_LEVEL = 5
logging.addLevelName(TRACE_LOGGING_LEVEL, "TRACE")


def init_logger(gateway: 'TBGatewayService', name, level, enable_remote_logging=False, is_connector_logger=False,
                is_converter_logger=False, attr_name=None):
    """
    For creating a Logger with all config automatically
    Create a Logger manually only if you know what you are doing!
    """

    log = logging.getLogger(name)
    log.is_connector_logger = is_connector_logger
    log.is_converter_logger = is_converter_logger

    if attr_name:
        log.attr_name = attr_name + '_ERRORS_COUNT'

    if hasattr(gateway, 'main_handler') and gateway.main_handler not in log.handlers:
        log.addHandler(gateway.main_handler)

    # Add file handler to the connector or converter logger
    # First check if it is a main module logger (for example OPC-UA connector logger)
    # If it is, add a file handler to the main module logger
    # If it is not (for example asyncua logger), add the main module file handler to the logger
    if TbLogger.is_main_module_logger(name, attr_name, is_converter_logger):
        file_handler = None

        if is_connector_logger:
            file_handler = TimedRotatingFileHandler.get_connector_file_handler(log.name + '_connector')

        if is_converter_logger:
            file_handler = TimedRotatingFileHandler.get_converter_file_handler(log.name)

        if file_handler:
            log.addHandler(file_handler)
    else:
        main_file_handler = TimedRotatingFileHandler.get_time_rotating_file_handler_by_logger_name(attr_name)
        if main_file_handler:
            log.addHandler(main_file_handler)

    from thingsboard_gateway.tb_utility.tb_handler import TBRemoteLoggerHandler
    if not hasattr(gateway, 'remote_handler'):
        gateway.remote_handler = TBRemoteLoggerHandler(gateway)
    if gateway.remote_handler not in log.handlers:
        log.addHandler(gateway.remote_handler)
        gateway.remote_handler.add_logger(name, level if enable_remote_logging else 'NONE')

    log_level_conf = level
    if log_level_conf:
        log_level = logging.getLevelName(log_level_conf)

        try:
            log.setLevel(log_level)
        except ValueError:
            log.setLevel(100)

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
                 attr_name=None):
        super(TbLogger, self).__init__(name=name, level=level)
        self.propagate = True
        self.parent = self.root
        self.__is_connector_logger = is_connector_logger
        self.__is_converter_logger = is_converter_logger
        self.__previous_reset_errors_time = TbLogger.PREVIOUS_ERRORS_RESET_TIME
        logging.Logger.trace = TbLogger.trace

        self.errors = 0

        if attr_name:
            self.attr_name = attr_name + '_ERRORS_COUNT'
        else:
            self.attr_name = self.name + '_ERRORS_COUNT'

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

    def reset(self):
        with TbLogger.ERRORS_MUTEX:
            TbLogger.ALL_ERRORS_COUNT = max(0, TbLogger.ALL_ERRORS_COUNT - self.errors)
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
        TbLogger.ERRORS_BATCH[error_attr_name] = max(0, self.errors)

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

    @staticmethod
    def is_main_module_logger(name, attr_name, is_converter_logger):
        return name == attr_name or attr_name is None or (name != attr_name and is_converter_logger)

    @staticmethod
    def update_file_handlers():
        for logger in logging.Logger.manager.loggerDict.values():
            if hasattr(logger, 'is_connector_logger') or hasattr(logger, 'is_converter_logger'):
                file_handler_filter = list(filter(lambda handler: isinstance(handler, TimedRotatingFileHandler),
                                                  logger.handlers))
                if len(file_handler_filter):
                    old_file_handler = file_handler_filter[0]

                    new_file_handler = None
                    filename = old_file_handler.baseFilename.split('/')[-1].split('.')[0]
                    if logger.is_connector_logger:
                        new_file_handler = TimedRotatingFileHandler.get_connector_file_handler(filename) # noqa

                    if logger.is_converter_logger:
                        new_file_handler = TimedRotatingFileHandler.get_converter_file_handler(filename) # noqa

                    if new_file_handler:
                        logger.addHandler(new_file_handler)
                        logger.removeHandler(old_file_handler)

    @staticmethod
    def check_and_update_file_handlers_class_name(config):
        for handler_config in config.get('handlers', {}).values():
            if handler_config.get('class', '') == 'thingsboard_gateway.tb_utility.tb_handler.TimedRotatingFileHandler':
                handler_config['class'] = 'thingsboard_gateway.tb_utility.tb_rotating_file_handler.TimedRotatingFileHandler' # noqa


logging.setLoggerClass(TbLogger)
