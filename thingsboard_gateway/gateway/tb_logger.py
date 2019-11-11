#     Copyright 2019. ThingsBoard
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
from time import time
# from thingsboard_gateway.gateway.tb_gateway_service import TBGatewayService


class TBLoggerHandler(logging.Handler):
    def __init__(self, gateway):
        super().__init__(logging.ERROR)
        self.__gateway = gateway
        self.activated = False
        self.log_levels = {
            # 'NONE': 0,
            'DEBUG': 10,
            'INFO': 20,
            'WARNING': 30,
            'ERROR': 40,
            'FATAL': 50,
            'CRITICAL': 50,
            'EXCEPTION': 50

        }
        self.loggers=['service',
                      'tb_connection',
                      'storage',
                      'extension',
                      'connector'
                      ]
        for logger in self.loggers:
            log = logging.getLogger(logger)
            log.addHandler(self.__gateway.main_handler)
        self.__current_log_level = 'ERROR'

    def emit(self, record):
        pass

    def activate(self, log_level=None):
        try:
            for logger in self.loggers:
                if log_level is not None and self.log_levels.get(log_level) is not None:
                    log = logging.getLogger(logger)
                    self.__current_log_level = log_level
                    log.setLevel(self.log_levels[log_level])
        except Exception as e:
            log = logging.getLogger('service')
            log.error(e)
        self.activated = True

    def handle(self, record):
        if self.activated:
            self.__form_message(record)
            self.__gateway.tb_client.client.send_telemetry(self.message, quality_of_service=1)

    def __form_message(self, record):
        self.message = {'ts': int(time()*1000),
                        'values': {
                            'LOGS': str(record.getMessage())
                            }
                        }

    def deactivate(self):
        self.activated = False
