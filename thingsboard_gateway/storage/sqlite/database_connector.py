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

from sqlite3 import connect
from thingsboard_gateway.storage.sqlite.storage_settings import StorageSettings


from logging import getLogger

log = getLogger("storage")


class DatabaseConnector:
    def __init__(self, settings: StorageSettings):
        self.data_file_path = settings.get_data_file_path()
        self.connection = None

    def connect(self):
        """
        Create database file in path from settings
        """
        try:
            self.connection = connect(self.data_file_path, check_same_thread=False)
        except Exception as e:
            log.exception(e)

    def commit(self):
        """
        Commit changes
        """
        log.debug("Committing changes to DB")
        try:
            self.connection.commit()

        except Exception as e:
            log.exception(e)

    def rollback(self):
        """
        Rollback changes after exception
        """
        log.debug("Rollback transaction")
        try:
            self.connection.rollback()

        except Exception as e:
            log.exception(e)

    def close(self):
        """
        Closes database file
        """
        try:
            self.connection.close()

        except Exception as e:
            log.exception(e)

    def get_cursor(self):
        try:
            return self.connection.cursor()

        except Exception as e:
            log.exception(e)
