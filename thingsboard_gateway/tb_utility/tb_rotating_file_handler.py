import os
import logging
from logging.handlers import TimedRotatingFileHandler
from os import environ
from pathlib import Path


class TimedRotatingFileHandler(TimedRotatingFileHandler):
    def __init__(self, filename, when='h', interval=1, backupCount=0,
                 encoding=None, delay=False, utc=False):
        config_path = environ.get('TB_GW_LOGS_PATH')
        if config_path:
            filename = config_path + os.sep + filename.split(os.sep)[-1]

        if not Path(filename).exists():
            with open(filename, 'w'):
                pass

        super().__init__(filename, when=when, interval=interval, backupCount=backupCount,
                         encoding=encoding, delay=delay, utc=utc)

    @staticmethod
    def get_connector_file_handler(file_name):
        return TimedRotatingFileHandler.__get_file_handler(file_name, 'connector')

    @staticmethod
    def get_converter_file_handler(file_name):
        return TimedRotatingFileHandler.__get_file_handler(file_name, 'converter')

    @staticmethod
    def get_time_rotating_file_handler_by_logger_name(logger_name):
        file_handler_filter = list(filter(lambda x: isinstance(x, TimedRotatingFileHandler),
                                          logging.getLogger(logger_name).handlers))
        if len(file_handler_filter):
            return file_handler_filter[0]

    @staticmethod
    def __get_file_handler(file_name, logger_name):
        file_handler = TimedRotatingFileHandler.get_time_rotating_file_handler_by_logger_name(logger_name)

        if not file_name.endswith('.log'):
            file_name = file_name + '.log'

        if file_handler:
            return TimedRotatingFileHandler.__create_file_handler_copy(file_handler, file_name)
        else:
            return TimedRotatingFileHandler.__create_default_file_handler(file_name)

    @staticmethod
    def __create_file_handler_copy(handler, file_name):
        file_name = f'{os.sep}'.join(handler.baseFilename.split(os.sep)[:-1]) + os.sep + file_name
        handler_copy = TimedRotatingFileHandler(file_name,
                                                when=handler.when,
                                                backupCount=handler.backupCount,
                                                interval=handler.interval,
                                                encoding=handler.encoding,
                                                delay=handler.delay,
                                                utc=handler.utc)
        handler_copy.setFormatter(handler.formatter)
        return handler_copy

    @staticmethod
    def __create_default_file_handler(file_name):
        file_name = file_name + '.log'
        return TimedRotatingFileHandler(file_name)
