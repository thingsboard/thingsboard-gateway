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
from threading import RLock
from typing import Optional

from thingsboard_gateway.storage.sqlite.storage_settings import StorageSettings


class DatabaseConnector:
    def __init__(self, settings: StorageSettings, logger, stopped):
        self.__log = logger
        self.data_file_path = settings.data_folder_path
        self.connection: Optional[Connection] = None
        self.lock = RLock()
        self.stopped = stopped

    def connect(self):
        """
        Create database file in path from settings
        """
        try:
            with self.lock:
                self.connection = connect(self.data_file_path, check_same_thread=False)
        except Exception as e:
            self.__log.exception("Failed to connect to database", exc_info=e)

    def commit(self):
        """
        Commit changes
        """
        self.__log.debug("Committing changes to DB")

        committed = False
        while not committed and not self.stopped.is_set():
            try:
                self.__commit()
                committed = True
            except sqlite3.ProgrammingError as e:
                self.__log.exception("Failed to commit changes to database", exc_info=e)
                self.__log.info('Trying to reconnect to database')
                self.connect()
            except Exception as e:
                self.__log.exception("Failed to commit changes to database", exc_info=e)
        return committed

    def __commit(self):
        with self.lock:
            self.connection.commit()

    def execute(self, *args):
        """
        Execute changes
        """
        # log.debug("Execute %s to DB", str(args))
        try:
            with self.lock:
                return self.connection.execute(*args)
        except sqlite3.ProgrammingError as e:
            self.__log.debug("Failed to execute changes to database", exc_info=e)
        except Exception as e:
            self.__log.exception("Failed to execute changes to database", exc_info=e)

    def rollback(self):
        """
        Rollback changes after exception
        """
        self.__log.debug("Rollback transaction")
        try:
            with self.lock:
                self.connection.rollback()

        except Exception as e:
            self.__log.exception(e)

    def close(self):
        """
        Closes database file
        """
        try:
            with self.lock:
                self.connection.close()

        except Exception as e:
            self.__log.exception("Failed to close database", exc_info=e)

    def get_cursor(self):
        try:
            return self.connection.cursor()

        except Exception as e:
            self.__log.exception("Failed to get cursor", exc_info=e)

    def update_logger(self, logger):
        self.__log = logger
