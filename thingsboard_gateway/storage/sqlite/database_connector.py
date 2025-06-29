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

import sqlite3
from sqlite3 import connect, Connection
from threading import Lock
from time import sleep
from typing import Optional


class DatabaseConnector:
    def __init__(self, data_file_path, logger, database_stopped_event):
        self.__log = logger
        self.data_file_path = data_file_path
        self.connection: Optional[Connection] = None
        self.lock = Lock()
        self.database_stopped_event = database_stopped_event
        self.__closed = True

    def connect(self):
        """
        Create database file in path from settings
        """
        try:
            with self.lock:
                self.connection = connect(self.data_file_path, check_same_thread=False)
                self.connection.execute("PRAGMA journal_mode=WAL;")
                self.connection.execute("PRAGMA synchronous=NORMAL;")
                self.connection.execute("PRAGMA cache_size=-20000;")
                self.connection.execute("PRAGMA temp_store=MEMORY;")
                self.connection.execute("PRAGMA journal_size_limit=5000000;")

                self.connection.execute("PRAGMA mmap_size=536870912;")
                self.connection.execute("PRAGMA busy_timeout=15000;")
                self.connection.execute("PRAGMA page_size=4096;")
                self.connection.execute("PRAGMA locking_mode=NORMAL;")
                self.connection.execute("PRAGMA read_uncommitted=ON;")
                self.connection.execute("PRAGMA auto_vacuum=INCREMENTAL;")
                self.connection.execute("PRAGMA foreign_keys=ON;")
                self.connection.row_factory = sqlite3.Row
                self.__closed = False
        except Exception as e:
            self.__log.exception(
                "Failed to connect reading connection to database", exc_info=e
            )

    def connect_on_closed_db(self, database_path):
        try:
            with self.lock:
                self.closed_db_connection = connect(database_path, check_same_thread=False)
        except Exception as e:
            self.__log.exception(
                "Failed to connect reading connection to database", exc_info=e)



    def commit(self):
        if self.__closed or self.database_stopped_event.is_set():
            return False
        retry_count = 0
        max_retries = 5
        base_sleep_time = 0.05

        while retry_count < max_retries and not self.database_stopped_event.is_set():
            try:
                while (
                        not self.database_stopped_event.is_set()
                        and self.connection is None
                        and not self.__closed
                ):
                    self.__log.debug("Connection is None. Waiting for connection...")
                    sleep(0.1)
                if not self.database_stopped_event.is_set() and not self.__closed:
                    with self.lock:
                        self.connection.commit()
                return True
            except sqlite3.OperationalError:
                sleep_time = base_sleep_time * (2 ** retry_count)
                self.__log.warning(
                    "Database locked! Retrying in %.2f ms...", sleep_time * 1000
                )
                sleep(sleep_time)
                retry_count += 1
            except Exception as e:
                self.__log.exception("Unexpected error during commit", exc_info=e)
                return False
        return False

    def execute_read(self, *args):
        """
        Execute read query to database
        """
        if self.__closed:
            return None
        try:
            while not self.database_stopped_event.is_set() and self.connection is None and not self.__closed:
                self.__log.debug("Connection is None. Waiting for connection...")
                sleep(0.1)
            if (
                    not self.database_stopped_event.is_set()
                    and not self.__closed
                    and self.connection is not None
            ):
                with self.lock:
                    cursor = self.connection.cursor()
                    result = cursor.execute(*args)
                    return result
        except sqlite3.ProgrammingError as e:
            self.__log.debug("Failed to execute read query in database", exc_info=e)
        except sqlite3.OperationalError as e:
            self.__log.debug("Failed to execute read query in database", exc_info=e)
        except Exception as e:
            self.__log.exception("Failed to execute read query in database", exc_info=e)

    def execute_write(self, *args):
        """
        Execute write query to database
        """
        if self.__closed:
            return None
        tries = 4
        current_try = 0
        while current_try < tries:
            try:
                while (
                        not self.database_stopped_event.is_set()
                        and self.connection is None
                        and not self.__closed
                ):
                    self.__log.debug("Connection is None. Waiting for connection...")
                    sleep(0.1)
                if (
                        not self.database_stopped_event.is_set()
                        and not self.__closed
                        and self.connection is not None
                ):
                    with self.lock:
                        cursor = self.connection.cursor()
                        result = cursor.execute(*args)
                        return result
            except sqlite3.OperationalError:
                current_try += 1
                sleep(0.05 * current_try)
            except sqlite3.ProgrammingError as e:
                self.__log.debug(
                    "Failed to execute write query in database", exc_info=e
                )
                current_try += 1
                sleep(0.05 * current_try)
            except Exception as e:
                self.__log.exception(
                    "Failed to execute write query in database", exc_info=e
                )
                current_try += 1
                sleep(0.05 * current_try)

    def execute_many_write(self, *args):
        """
        Execute write query with many records to database
        """
        if self.__closed:
            return None
        tries = 4
        current_try = 0
        while current_try < tries:
            try:
                while (
                        not self.database_stopped_event.is_set() and self.connection is None
                ):
                    self.__log.debug("Connection is None. Waiting for connection...")
                    sleep(0.1)
                if (
                        not self.database_stopped_event.is_set()
                        and not self.__closed
                        and self.connection is not None
                ):
                    with self.lock:
                        cursor = self.connection.cursor()
                        cursor.execute("BEGIN TRANSACTION;")
                        return cursor.executemany(*args)
            except sqlite3.OperationalError:
                current_try += 1
                sleep(0.05 * current_try)
            except sqlite3.ProgrammingError as e:
                self.__log.debug(
                    "Failed to execute write query in database", exc_info=e
                )
                current_try += 1
                sleep(0.05 * current_try)
            except Exception as e:
                self.__log.exception(
                    "Failed to execute write query in database", exc_info=e
                )
                current_try += 1
                sleep(0.05 * current_try)

    def rollback(self):
        """
        Rollback changes after exception
        """
        if self.__closed:
            return
        self.__log.info("Rollback transaction")
        self.connection.rollback()

    def interrupt(self):
        """
        Interrupts database connection
        """
        if not self.__closed:
            self.__log.debug("Interrupting connection to database")
            self.connection.interrupt()

    @property
    def closed(self):
        return self.__closed

    def close(self):
        """
        Closes database file
        """
        if not self.__closed:
            self.__closed = True
            if self.connection.in_transaction:
                self.connection.interrupt()
            try:
                if self.connection is not None:
                    with self.lock:
                        self.__log.debug("Closing connection to database")
                        self.connection.close()
                        self.connection = None
            except Exception as e:
                self.__log.exception(
                    "Failed to close connection to database", exc_info=e
                )

    def get_cursor(self):
        try:
            return self.connection.cursor()

        except Exception as e:
            self.__log.exception("Failed to get cursor", exc_info=e)

    def update_logger(self, logger):
        self.__log = logger
