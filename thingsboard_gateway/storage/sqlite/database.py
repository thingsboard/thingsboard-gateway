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
from os.path import exists, dirname
from os import makedirs
from time import time, sleep
from logging import getLogger
from threading import Thread
from queue import Queue
import datetime

from thingsboard_gateway.storage.sqlite.database_connector import DatabaseConnector
from thingsboard_gateway.storage.sqlite.database_action_type import DatabaseActionType
from thingsboard_gateway.storage.sqlite.storage_settings import StorageSettings


class Database(Thread):
    """
        What this component does:
        - abstracts creating tables for devices.
        - writes to database
        - reads from database
        - delete data older than specified in config
        ------------- ALL OF THIS IN AN ATOMIC WAY ---------
    """

    def __init__(self, config, processing_queue: Queue, logger):
        self.__log = logger
        super().__init__()
        self.daemon = True
        self.settings = StorageSettings(config)

        if not exists(self.settings.data_folder_path):
            directory = dirname(self.settings.data_folder_path)
            if not exists(directory):
                self.__log.info("SQLite database file not found, creating new one...")
                try:
                    makedirs(directory)
                    self.__log.info("Directory %s created" % directory)
                except Exception as e:
                    self.__log.exception("Failed to create directory %s" % directory, exc_info=e)
            with open(self.settings.data_folder_path, 'w'):
                self.__log.info("SQLite database file created at %s" % self.settings.data_folder_path)

        # Pass settings to connector
        self.db = DatabaseConnector(self.settings, self.__log)

        self.db.connect()

        # process Queue
        self.processQueue = processing_queue

        self.__stopped = False

        self.__last_msg_check = time()

        self.msg_counter = 0
        self.start()

    def init_table(self):
        try:
            self.db.execute('''CREATE TABLE IF NOT EXISTS messages (timestamp INTEGER, message TEXT); ''')
            self.db.commit()
        except Exception as e:
            self.db.rollback()
            self.__log.exception(e)

    def run(self):
        while True:
            self.process()

            sleep(.2)

    def process(self):
        try:
            if time() - self.__last_msg_check >= self.settings.messages_ttl_check_in_hours:
                self.__last_msg_check = time()
                self.delete_data_lte(self.settings.messages_ttl_in_days)

            # Signalization so that we can spam call process()
            if not self.__stopped and self.processQueue:
                while self.processQueue.qsize() > 0:

                    req = self.processQueue.get()

                    self.__log.debug("Processing %s" % req.type)
                    if req.type is DatabaseActionType.WRITE_DATA_STORAGE:

                        message = req.data

                        timestamp = time()

                        self.db.execute('''INSERT INTO messages (timestamp, message) VALUES (?, ?);''',
                                        [timestamp, message])

                        self.db.commit()
            else:
                self.__log.info("Storage is closed!")

        except Exception as e:
            self.db.rollback()
            self.__log.exception("Failed to write data to storage! Error: %s", e)

    def read_data(self):
        try:
            data = self.db.execute('''SELECT timestamp, message FROM messages ORDER BY timestamp ASC LIMIT 0, 50;''')
            return data
        except Exception as e:
            self.db.rollback()
            self.__log.exception("Failed to read data from storage! Error: %s", e)

    def delete_data(self, ts):
        try:
            data = self.db.execute('''DELETE FROM messages WHERE timestamp <= ?;''', [ts,])
            self.db.commit()
            return data
        except Exception as e:
            self.db.rollback()
            self.__log.exception("Failed to delete data from storage! Error: %s", e)

    def delete_data_lte(self, days):
        try:
            ts = (datetime.datetime.now() - datetime.timedelta(days=days)).timestamp()
            data = self.db.execute('''DELETE FROM messages WHERE timestamp <= ? ;''', [ts])
            self.db.commit()
            return data
        except Exception as e:
            self.db.rollback()
            self.__log.exception("Failed to delete data from storage! Error: %s", e)

    def setProcessQueue(self, process_queue):
        self.processQueue = process_queue

    def closeDB(self):
        self.db.close()

    def update_logger(self):
        self.__log = getLogger("storage")
        self.db.update_logger()
        self.__log.info("Logger updated")
