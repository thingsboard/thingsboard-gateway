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
from time import sleep
from threading import Thread


def init_logger(gateway, name, level):
    """
    For creating a Logger with all config automatically
    Create a Logger manually only if you know what you are doing!
    """
    log = TbLogger(name=name, gateway=gateway)

    if hasattr(gateway, 'remote_handler'):
        log.addHandler(gateway.remote_handler)
        log.setLevel(gateway.main_handler.level)
        gateway.remote_handler.add_logger(name)

    if hasattr(gateway, 'main_handler'):
        log.addHandler(gateway.main_handler)
        log.setLevel(gateway.remote_handler.level)

    log_level_conf = level
    if log_level_conf:
        log_level = logging.getLevelName(log_level_conf)
        log.setLevel(log_level)

    return log


class TbLogger(logging.Logger):
    ALL_ERRORS_COUNT = 0
    IS_ALL_ERRORS_COUNT_RESET = False

    def __init__(self, name, gateway=None, level=logging.NOTSET):
        super(TbLogger, self).__init__(name=name, level=level)
        self.propagate = True
        self.parent = self.root
        self._gateway = gateway
        self.errors = 0
        self.attr_name = self.name + '_ERRORS_COUNT'
        self._is_on_init_state = True
        if self._gateway:
            self._send_errors_thread = Thread(target=self._send_errors, name='Send Errors Thread', daemon=True)
            self._send_errors_thread.start()

    def reset(self):
        """
        !!!Need to be called manually in the connector 'close' method!!!
        """
        TbLogger.ALL_ERRORS_COUNT = TbLogger.ALL_ERRORS_COUNT - self.errors

    @property
    def gateway(self):
        return self._gateway

    @gateway.setter
    def gateway(self, gateway):
        self._gateway = gateway

    def _send_errors(self):
        is_tb_client = False

        while not self._gateway:
            sleep(1)

        while not is_tb_client:
            is_tb_client = hasattr(self._gateway, 'tb_client')
            sleep(1)

        if not TbLogger.IS_ALL_ERRORS_COUNT_RESET and self._gateway.tb_client.is_connected():
            self._gateway.tb_client.client.send_telemetry(
                {self.attr_name: 0, 'ALL_ERRORS_COUNT': 0}, quality_of_service=0)
            TbLogger.IS_ALL_ERRORS_COUNT_RESET = True
        self._is_on_init_state = False

    def error(self, msg, *args, **kwargs):
        kwargs['stacklevel'] = 2
        super(TbLogger, self).error(msg, *args, **kwargs)
        self._send_error_count()

    def exception(self, msg, *args, **kwargs) -> None:
        attr_name = kwargs.pop('attr_name', None)
        kwargs['stacklevel'] = 2
        super(TbLogger, self).exception(msg, *args, **kwargs)
        self._send_error_count(error_attr_name=attr_name)

    def _send_error_count(self, error_attr_name=None):
        TbLogger.ALL_ERRORS_COUNT += 1
        self.errors += 1

        while self._is_on_init_state:
            sleep(.2)

        if self._gateway and hasattr(self._gateway, 'tb_client'):
            if error_attr_name:
                error_attr_name = error_attr_name + '_ERRORS_COUNT'
            else:
                error_attr_name = self.attr_name

            self._gateway.tb_client.client.send_telemetry(
                {error_attr_name: self.errors, 'ALL_ERRORS_COUNT': TbLogger.ALL_ERRORS_COUNT})
