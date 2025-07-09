import logging
from os import sep
from os.path import isfile, exists
from re import compile, ASCII
from logging.handlers import TimedRotatingFileHandler as BaseTimedRotatingFileHandler
from os import environ
from pathlib import Path


class TimedRotatingFileHandler(BaseTimedRotatingFileHandler):
    DELIMITER_RE = compile(r'[\s-]+')
    CAMEL_BOUNDARY_RE = compile(r'(?<=[a-z0-9])([A-Z])')
    SPECIAL_CHAR_RE = compile(r'[^A-Za-z0-9_]')
    MULTI_UNDERSCORE_RE = compile(r'_+')

    def __init__(self, filename, when='h', interval=1, backupCount=0,
                 encoding=None, delay=False, utc=False, maxBytes=0):
        file_path = filename
        config_path = environ.get('TB_GW_LOGS_PATH')
        original_log_filename = file_path.split(sep)[-1]

        final_filename = original_log_filename.replace('.log', '')
        final_filename = TimedRotatingFileHandler.to_snake_case(final_filename)
        final_filename += '.log'

        file_path = file_path.replace(original_log_filename, final_filename)

        if config_path:
            file_path = config_path + sep + final_filename

        if not Path(file_path).exists():
            with open(file_path, 'w'):
                pass

        super().__init__(filename=file_path, when=when, interval=interval, backupCount=backupCount,
                         encoding=encoding, delay=delay, utc=utc)

        self.maxBytes = maxBytes
        if self.maxBytes > 0:
            self.suffix = "%Y-%m-%d_%H-%M-%S"
            self.extMatch = compile(r"(?<!\d)\d{4}-\d{2}-\d{2}_\d{2}-\d{2}-\d{2}(?!\d)", ASCII)

    def shouldRolloverOnSize(self, record):
        if self.stream is None:
            return False

        if self.maxBytes > 0:
            pos = self.stream.tell()
            if not pos:
                return False
            msg = "%s\n" % self.format(record)
            if pos + len(msg) >= self.maxBytes:
                if exists(self.baseFilename) and not isfile(self.baseFilename):
                    return False
                return True

        return False

    def shouldRollover(self, record):
        return self.shouldRolloverOnSize(record) or super().shouldRollover(record)

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
        file_name = f'{sep}'.join(handler.baseFilename.split(sep)[:-1]) + sep + file_name
        handler_copy = TimedRotatingFileHandler(file_name,
                                                when=handler.when,
                                                backupCount=handler.backupCount,
                                                interval=handler.interval,
                                                encoding=handler.encoding,
                                                delay=handler.delay,
                                                utc=handler.utc,
                                                maxBytes=handler.maxBytes)
        handler_copy.setFormatter(handler.formatter)
        return handler_copy

    @staticmethod
    def __create_default_file_handler(file_name):
        if not file_name.endswith('.log'):
            file_name = file_name + '.log'
        return TimedRotatingFileHandler(file_name)

    @staticmethod
    def to_snake_case(name: str) -> str:
        s = TimedRotatingFileHandler.DELIMITER_RE.sub('_', name)
        s = TimedRotatingFileHandler.CAMEL_BOUNDARY_RE.sub(r'_\1', s)
        s = s.lower()
        s = TimedRotatingFileHandler.SPECIAL_CHAR_RE.sub('', s)
        s = TimedRotatingFileHandler.MULTI_UNDERSCORE_RE.sub('_', s)
        return s.strip('_')
