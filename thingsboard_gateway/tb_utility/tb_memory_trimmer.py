#     Copyright 2026. ThingsBoard
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

import ctypes
from logging import getLogger
from os import environ
from platform import system
from time import monotonic

log = getLogger("service")

MALLOC_TRIM_INTERVAL_ENV = "TB_GW_MALLOC_TRIM_INTERVAL"
DEFAULT_MALLOC_TRIM_INTERVAL_SECONDS = 60


class TBMemoryTrimmer:
    """
    Periodically returns free heap memory to the OS on glibc-based Linux.

    CPython allocates objects bigger than 512 bytes (JSON payloads, converted data, storage events)
    with glibc malloc. After a burst, e.g. a storage backlog drained after reconnect to ThingsBoard,
    the freed memory stays fragmented inside malloc arenas and process RSS does not go down.
    malloc_trim(0) releases free pages from all arenas back to the OS.

    Interval in seconds is taken from TB_GW_MALLOC_TRIM_INTERVAL environment variable, 0 disables trimming.
    """

    def __init__(self, interval=None):
        self.interval = self.__interval_from_env() if interval is None else max(float(interval), 0)
        self.__malloc_trim = self.__load_malloc_trim() if self.interval > 0 else None
        self.__next_trim_time = monotonic() + self.interval

        if self.enabled:
            log.debug("Memory trimming enabled with interval %s seconds", self.interval)

    @property
    def enabled(self):
        return self.__malloc_trim is not None

    def trim_if_needed(self):
        if self.__malloc_trim is None:
            return

        now = monotonic()
        if now < self.__next_trim_time:
            return
        self.__next_trim_time = now + self.interval

        try:
            self.__malloc_trim(0)
        except Exception as e:
            log.debug("Failed to trim memory, disabling memory trimming: %s", e)
            self.__malloc_trim = None

    @staticmethod
    def __interval_from_env():
        value = environ.get(MALLOC_TRIM_INTERVAL_ENV)
        if value is None:
            return DEFAULT_MALLOC_TRIM_INTERVAL_SECONDS

        try:
            return max(float(value), 0)
        except ValueError:
            log.warning("Invalid %s value: %r, using default %s seconds",
                        MALLOC_TRIM_INTERVAL_ENV, value, DEFAULT_MALLOC_TRIM_INTERVAL_SECONDS)
            return DEFAULT_MALLOC_TRIM_INTERVAL_SECONDS

    @staticmethod
    def __load_malloc_trim():
        if system() != "Linux":
            return None

        try:
            # Symbols of the C library the interpreter is linked with, no library lookup needed.
            # malloc_trim is glibc-specific, so on other C libraries (e.g. musl) trimming stays disabled.
            malloc_trim = ctypes.CDLL(None).malloc_trim
        except (OSError, AttributeError):
            log.debug("malloc_trim is not available, memory trimming is disabled")
            return None

        malloc_trim.argtypes = [ctypes.c_size_t]
        malloc_trim.restype = ctypes.c_int
        return malloc_trim
