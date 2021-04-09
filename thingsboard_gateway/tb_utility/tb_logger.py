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

from sys import stdout
import logging
import logging.handlers


class TBLoggerHandler(logging.Handler):
    def __init__(self, gateway):
        self.current_log_level = 'INFO'
        super().__init__(logging.getLevelName(self.current_log_level))
        self.setLevel(logging.getLevelName('DEBUG'))
        self.__gateway = gateway
        self.activated = False
        self.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - [%(filename)s] - %(module)s - %(lineno)d - %(message)s'))
        self.loggers = ['service',
                        'extension',
                        'converter',
                        'connector',
                        'tb_connection'
                        ]
        for logger in self.loggers:
            log = logging.getLogger(logger)
            log.addHandler(self.__gateway.main_handler)
            log.debug("Added remote handler to log %s", logger)

    def activate(self, log_level=None):
        try:
            for logger in self.loggers:
                if log_level is not None and logging.getLevelName(log_level) is not None:
                    if logger == 'tb_connection' and log_level == 'DEBUG':
                        log = logging.getLogger(logger)
                        log.setLevel(logging.getLevelName('INFO'))
                    else:
                        log = logging.getLogger(logger)
                        self.current_log_level = log_level
                        log.setLevel(logging.getLevelName(log_level))
        except Exception as e:
            log = logging.getLogger('service')
            log.exception(e)
        self.activated = True

    def handle(self, record):
        if self.activated:
            record = self.formatter.format(record)
            self.__gateway.send_to_storage(self.__gateway.name, {"deviceName": self.__gateway.name, "telemetry": [{'LOGS': record}]})

    def deactivate(self):
        self.activated = False

    @staticmethod
    def set_default_handler():
        logger_names = [
                       'service',
                       'storage',
                       'extension',
                       'converter',
                       'connector',
                       'tb_connection'
                       ]
        for logger_name in logger_names:
            logger = logging.getLogger(logger_name)
            handler = logging.StreamHandler(stdout)
            handler.setFormatter(logging.Formatter('[STREAM ONLY] %(asctime)s - %(levelname)s - [%(filename)s] - %(module)s - %(lineno)d - %(message)s'))
            logger.addHandler(handler)
