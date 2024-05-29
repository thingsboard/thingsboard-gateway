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
from time import sleep, monotonic
from threading import Thread


def init_logger(gateway, name, level, enable_remote_logging=False):
    """
    For creating a Logger with all config automatically
    Create a Logger manually only if you know what you are doing!
    """
    log = TbLogger(name=name, gateway=gateway)

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
    RESET_ERRORS__PERIOD = 60

    def __init__(self, name, gateway=None, level=logging.NOTSET):
        super(TbLogger, self).__init__(name=name, level=level)
        self.propagate = True
        self.parent = self.root
        self._gateway = gateway
        self._stopped = False
        self.errors = 0
        self.attr_name = self.name + '_ERRORS_COUNT'
        self._is_on_init_state = True
        if self._gateway:
            self._send_errors_thread = Thread(target=self._send_errors, name='[LOGGER] Send Errors Thread', daemon=True)
            self._send_errors_thread.start()

        self._start_time = monotonic()
        self._reset_errors_thread = Thread(target=self._reset_errors_timer, name='[LOGGER] Reset Errors Thread',
                                           daemon=True)
        self._reset_errors_thread.start()

    def reset(self):
        """
        !!!Need to be called manually in the connector 'close' method!!!
        """
        if TbLogger.ALL_ERRORS_COUNT > 0 and self.errors > 0:
            TbLogger.ALL_ERRORS_COUNT = TbLogger.ALL_ERRORS_COUNT - self.errors
            self.errors = 0
            self._send_error_count()

    def stop(self):
        self.reset()
        self._stopped = True

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

    def _reset_errors_timer(self):
        while not self._stopped:
            if monotonic() - self._start_time >= TbLogger.RESET_ERRORS__PERIOD:
                self.reset()
                self._start_time = monotonic()

            sleep(1)

    def error(self, msg, *args, **kwargs):
        kwargs['stacklevel'] = 2
        super(TbLogger, self).error(msg, *args, **kwargs)
        self._add_error()
        self._send_error_count()

    def exception(self, msg, *args, **kwargs) -> None:
        attr_name = kwargs.pop('attr_name', None)
        kwargs['stacklevel'] = 2
        super(TbLogger, self).exception(msg, *args, **kwargs)
        self._add_error()
        self._send_error_count(error_attr_name=attr_name)

    def _add_error(self):
        TbLogger.ALL_ERRORS_COUNT += 1
        self.errors += 1

    def _send_error_count(self, error_attr_name=None):
        while self._is_on_init_state:
            sleep(.2)

        if self._gateway and hasattr(self._gateway, 'tb_client'):
            if error_attr_name:
                error_attr_name = error_attr_name + '_ERRORS_COUNT'
            else:
                error_attr_name = self.attr_name

            self._gateway.tb_client.client.send_telemetry(
                {error_attr_name: self.errors, 'ALL_ERRORS_COUNT': TbLogger.ALL_ERRORS_COUNT})
