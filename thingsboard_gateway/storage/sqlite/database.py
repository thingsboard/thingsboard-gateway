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

from os.path import exists, dirname
from os import makedirs
from sqlite3 import DatabaseError, ProgrammingError, InterfaceError, OperationalError
from time import sleep, monotonic, time
from logging import getLogger
from threading import Event, Thread
from queue import Queue, Empty
import datetime

from thingsboard_gateway.storage.sqlite.database_connector import DatabaseConnector
from thingsboard_gateway.storage.sqlite.storage_settings import StorageSettings


class Database(Thread):
    """
        Handles SQLite database operations for ThingsBoard Gateway:
        - Table creation & migration
        - Writing & reading messages efficiently
        - Deleting old records based on timestamp
        - Using PRIMARY KEY (`id`) for fast operations
    """

    def __init__(self, config, processing_queue: Queue, logger, stopped: Event):
        self.__initialized = False
        self.__log = logger
        super().__init__()
        self.name = "DatabaseThread"
        self.daemon = True
        self.stopped = stopped
        self.stopped_flag = Event()
        self.settings = StorageSettings(config)

        # Ensure database file and directory exist
        if not exists(self.settings.data_folder_path):
            directory = dirname(self.settings.data_folder_path)
            if not exists(directory):
                self.__log.info("SQLite database file not found, creating new one...")
                try:
                    makedirs(directory)
                    self.__log.info(f"Directory {directory} created")
                except Exception as e:
                    self.__log.exception(f"Failed to create directory {directory}", exc_info=e)
            with open(self.settings.data_folder_path, 'w'):
                self.__log.info(f"SQLite database file created at {self.settings.data_folder_path}")

        # Initialize database connections
        self.db = DatabaseConnector(self.settings, self.__log, self.stopped)
        self.db.connect()
        self.init_table()
        self.process_queue = processing_queue
        self.__last_msg_check = 0
        self.__can_prepare_new_batch = True
        self.__next_batch = []
        self.__initialized = True

        self.start()

    def init_table(self):
        try:
            # Check if the old schema exists
            try:
                result = self.db.execute_read(
                    "SELECT sql FROM sqlite_master WHERE type='table' AND name='messages';"
                ).fetchone()
            except OperationalError:
                result = None

            if result:
                if "timestamp" not in result[0] or "id" not in result[0]:
                    self.__log.info("Old schema detected! Starting migration...")
                    self.migrate_old_data()

            # Create table if not exists
            self.db.execute_write('''CREATE TABLE IF NOT EXISTS messages (
                                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                                        timestamp INTEGER NOT NULL,
                                        message TEXT NOT NULL
                                    );''')
            cursor = self.db.execute_write("CREATE INDEX IF NOT EXISTS idx_timestamp ON messages (timestamp);")
            cursor.close()
            self.db.commit()

        except Exception as e:
            self.db.rollback()
            self.__log.exception("Failed to create table or migrate data! Error: %s", e)

    def run(self):
        self.__log.info("Database thread started %r", id(self))
        while not self.stopped.is_set():
            try:
                if self.__can_prepare_new_batch and not self.__next_batch:
                    self.__next_batch = self.read_data()
                    self.__can_prepare_new_batch = False
                self.process()
                if self.process_queue.empty():
                    sleep(.1)
                sleep(.01)
            except Exception as e:
                self.__log.exception("Error in database thread: %s", exc_info=e)
        self.__log.info("Database thread stopped %r", id(self))
        self.close_db()
        self.db.close()
        self.stopped_flag.set()

    def process(self):
        try:
            cur_time = int(time() * 1000)
            if (cur_time - self.__last_msg_check >= self.__last_msg_check + self.settings.messages_ttl_check_in_hours
                    and not self.stopped.is_set() and not self.stopped_flag.is_set()):
                self.__last_msg_check = cur_time
                self.delete_data_lte(self.settings.messages_ttl_in_days)
            if not self.process_queue.empty():
                batch = []
                start_collecting = monotonic()

                while (len(batch) < self.settings.batch_size
                        and not self.stopped.is_set()
                        and monotonic() - start_collecting < 0.1):
                    try:
                        batch.append((cur_time, self.process_queue.get_nowait()))
                    except Empty:
                        if monotonic() - start_collecting > .1:
                            break
                        sleep(.01)

                if batch and not self.stopped.is_set() and not self.stopped_flag.is_set():
                    start_writing = monotonic()
                    self.db.execute_many_write('''INSERT INTO messages (timestamp, message) VALUES (?, ?);''', batch)
                    self.db.commit()

                    self.__log.trace("Wrote %d records in %.2f ms, queue size: %d, Avg time per 1 record: %.2f ms",
                                    len(batch), (monotonic() - start_writing) * 1000, self.process_queue.qsize(),
                                    (monotonic() - start_writing) * 1000 / len(batch))
            else:
                self.stopped.wait(0.1)

        except Exception as e:
            self.db.rollback()
            self.__log.exception("Failed to write data to storage! Error: %s", e)

    def clean_next_batch(self):
        self.__next_batch = []

    def read_data(self):
        if self.stopped_flag.is_set() or not self.__initialized:
            return []
        try:
            if self.db.closed or self.stopped.is_set() or not self.db.connection:
                return []
            if self.__next_batch:
                return self.__next_batch
            start_time = monotonic()
            data = self.db.execute_read(
                '''SELECT id, timestamp, message FROM messages LIMIT ?;''',
                (self.settings.max_read_records_count,)
            )
            if not data:
                return []
            collected_data = data.fetchall()
            elapsed_time = (monotonic() - start_time) * 1000
            if collected_data:
                self.__log.trace("Read %d records in %.2f ms", len(collected_data), elapsed_time)
            return collected_data
        except DatabaseError:
            return []
        except (ProgrammingError, InterfaceError) as e:
            self.__log.debug("Error reading data from storage: %s", e)
            return []
        except MemoryError:
            return []

    def interrupt(self):
        self.db.interrupt()

    def delete_data(self, row_id):
        if self.stopped_flag.is_set():
            return
        try:
            data = self.db.execute_write('''DELETE FROM messages WHERE id <= ?;''', [row_id,])
            self.db.commit()
            return data
        except Exception as e:
            self.db.rollback()
            self.__log.exception("Failed to delete data from storage! Error: %s", e)

    def delete_data_lte(self, days):
        if self.stopped_flag.is_set():
            return
        try:
            ts = (datetime.datetime.now() - datetime.timedelta(days=days)).timestamp()
            data = self.db.execute_write('''DELETE FROM messages WHERE timestamp <= ? ;''', [ts])
            self.db.commit()
            return data
        except Exception as e:
            self.db.rollback()
            self.__log.exception("Failed to delete data from storage! Error: %s", e)

    def migrate_old_data(self):
        if self.stopped_flag.is_set():
            return
        try:
            self.__log.info("Renaming old table...")
            self.db.execute_write("ALTER TABLE messages RENAME TO messages_old;")
            self.db.commit()

            self.__log.info("Creating new optimized table...")
            self.db.execute_write('''CREATE TABLE messages (
                                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                                        timestamp INTEGER NOT NULL,
                                        message TEXT NOT NULL
                                    );''')
            self.db.commit()

            self.__log.info("Migrating old data to new table...")
            self.db.execute_write('''
                INSERT INTO messages (timestamp, message)
                SELECT timestamp, message FROM messages_old;
            ''')
            self.db.commit()

            self.__log.info("Dropping old table...")
            self.db.execute_write("DROP TABLE messages_old;")
            self.db.commit()

            self.__log.info("Migration completed successfully!")

        except Exception as e:
            self.db.rollback()
            self.__log.exception("Failed to migrate old data! Error: %s", e)

    def get_stored_messages_count(self):
        if self.stopped_flag.is_set():
            return -1
        try:
            data = self.db.execute_read('''SELECT COUNT(*) FROM messages;''')
            if not data:
                return -1
            count = data.fetchone()[0]
            return count
        except Exception as e:
            self.__log.exception("Failed to get stored messages count! Error: %s", e)
            return 0

    def close_db(self):
        if not self.stopped.is_set():
            self.stopped.set()

    def can_prepare_new_batch(self):
        self.__next_batch = []
        self.__can_prepare_new_batch = True
        return self.__can_prepare_new_batch

    def update_logger(self):
        self.__log = getLogger("storage")
        self.db.update_logger(logger=self.__log)
        self.__log.info("Logger updated")
