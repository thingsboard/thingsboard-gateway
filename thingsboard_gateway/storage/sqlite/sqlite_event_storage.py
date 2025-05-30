#      Copyright 2025. ThingsBoard
#  #
#      Licensed under the Apache License, Version 2.0 (the "License");
#      you may not use this file except in compliance with the License.
#      You may obtain a copy of the License at
#  #
#          http://www.apache.org/licenses/LICENSE-2.0
#  #
#      Unless required by applicable law or agreed to in writing, software
#      distributed under the License is distributed on an "AS IS" BASIS,
#      WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#      See the License for the specific language governing permissions and
#      limitations under the License.
#
#
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
import copy
from gc import collect
from sqlite3 import InterfaceError
from threading import Event, Lock
from time import sleep, time, monotonic
import re
from aiofiles.ospath import exists
import os
from rdflib.plugins.parsers.ntriples import r_wspace
from simplejson import dumps
from typing import List

from twisted.words.protocols.jabber.jstrports import client

from thingsboard_gateway.storage.event_storage import EventStorage
from thingsboard_gateway.storage.sqlite.database import Database
from thingsboard_gateway.storage.sqlite.sqlite_event_storage_pointer import Pointer

from queue import Queue, Full
from logging import getLogger

from thingsboard_gateway.storage.sqlite.storage_settings import StorageSettings


class SQLiteEventStorage(EventStorage):
    """
    High level api for the gateway main loop
    """

    def __init__(self, config, logger, main_stop_event):
        super().__init__(config, logger, main_stop_event)
        self.__log = logger
        self.__log.info("Sqlite Storage initializing...")
        self.write_queue = Queue(-1)
        self.stopped = Event()
        self.__current_data_from_storage = None
        self.__read_db_file_change_lock = Lock()
        self.__write_db_file_creation_lock = Lock()
        self.__config_copy = copy.deepcopy(config)  # TODO: Work with StorageSettings instead
        self.old_db_data_is_read = False
        self.__settings = StorageSettings(config)
        self.__pointer = Pointer(self.__settings.data_folder_path, log=self.__log, settings=self.__settings)
        self.__read_database = Database(config, self.write_queue, self.__log,
                                        stopped=self.stopped)  # TODO: provide storage settings instead of raw config
        if self.__pointer.read_database_file == self.__pointer.write_database_file:
            self.__write_database = self.__read_database
            self.__write_database.start()

        else:
            write_database_name = self.__pointer.write_database_file
            data_file_path_to_new_db_file = self._config["data_folder_path"] + write_database_name
            self.__config_copy['data_file_path'] = data_file_path_to_new_db_file
            self.__read_database.should_write = False
            self.__write_database = Database(self.__config_copy, self.write_queue, self.__log, stopped=self.stopped,
                                             should_read=False, should_write=True)
            self.__read_database.start()
            self.__write_database.start()

        self.__read_database.init_table()
        self.__log.info("Sqlite storage initialized!")
        self.delete_time_point = None
        self.__event_pack_processing_start = monotonic()
        self.last_read = time()

    def get_event_pack(self):
        if not self.stopped.is_set():
            self.__event_pack_processing_start = monotonic()
            event_pack_messages = []
            try:
                if self.__pointer.read_database_file != self.__pointer.write_database_file and not self.__read_database:
                    with self.__read_db_file_change_lock:
                        read_database_file = self.__pointer.sort_db_files()[0]
                        full_path_to_read_db_file = os.path.join(self.__settings.directory_path, read_database_file)
                        self.__config_copy['data_file_path'] = full_path_to_read_db_file
                        self.__read_database = Database(self.__config_copy, self.write_queue, self.__log,
                                                        stopped=self.stopped, should_read=True, should_write=False)
                        self.__read_database.start()
                        self.__pointer.update_read_database_filename(read_database_file)
                        self.__log.info("Sqlite storage updated read_database_file to: %s", read_database_file)
                #This occurs when we already delete oversize db and read data from it  and write data to new and this new did not reach it's
                # Me seems I do not lock here since at this condition only one thread exists
                # TODO what If I disconnect internet when code in this condition
                elif not self.__read_database:
                    self.__read_database = self.__write_database
                    self.__read_database.should_read = True
                elif self.old_db_data_is_read:
                    with self.__read_db_file_change_lock:
                        if len(self.__pointer.sort_db_files()) > 1:
                            read_database_file = self.__pointer.sort_db_files()[0]
                            full_path_to_read_db_file = os.path.join(self.__settings.directory_path, read_database_file)
                            self.__config_copy['data_file_path'] = full_path_to_read_db_file
                            self.__read_database = Database(self.__config_copy, self.write_queue, self.__log,stopped=self.stopped, should_read=True, should_write=False)
                            self.__read_database.start()
                            self.__log.info("Successfully created new read database after deletion old")
                        else:
                            self.__read_database = self.__write_database
                            self.__read_database.should_read = True

                        self.old_db_data_is_read = False
                with self.__read_db_file_change_lock:
                    data_from_storage = self.read_data()

            except InterfaceError as e:
                self.__log.error("InterfaceError occurred while reading data from storage: %s", e)
                return []
            try:
                for row in data_from_storage:
                    event_pack_messages.append(row['message'])
                    if self.delete_time_point is None or self.delete_time_point < row['id']:
                        self.delete_time_point = row['id']
            except (ValueError, TypeError):
                return []
            except (InterfaceError, MemoryError):
                return []
            if len(event_pack_messages) > 0:
                self.__log.trace("Retrieved %r records from storage in %r milliseconds, left in storage: %r",
                                 len(event_pack_messages),
                                 int((monotonic() - self.__event_pack_processing_start) * 1000),
                                 self.__read_database.get_stored_messages_count())
            self.__read_database.can_prepare_new_batch()
            return event_pack_messages
        else:
            return []

    def event_pack_processing_done(self):
        self.__log.trace("Batch processing done, processing time: %i milliseconds",
                         int((monotonic() - self.__event_pack_processing_start) * 1000))
        if not self.stopped.is_set():
            self.delete_data(self.delete_time_point)
            if self.__read_database.reached_size_limit and self.delete_time_point == self.__pointer.read_position:
                # Will watch over this lock perhaps data loses are during database initialization and threads switching

                with self.__read_db_file_change_lock:
                    self.old_db_data_is_read = True
                    self.__read_database.close_db()
                    self.__read_database.join(5)
                    self.delete_oversize_db_file(self.__read_database.settings.data_folder_path)
                    self.__read_database = None
            self.__pointer.update_position(self.delete_time_point)

        collect()

    def delete_oversize_db_file(self, path_to_oversize_db_file):

        for suffix in ("","-shm", "-wal"):
            path_to_file_with_sufix = f"{path_to_oversize_db_file}{suffix}"
            try:
                os.remove(path_to_file_with_sufix)
                self.__log.debug("Deleted %s ", path_to_file_with_sufix)
            except FileNotFoundError:
                continue
            except Exception as e:
                self.__log.exception("Failed to delete %s: %s", path_to_file_with_sufix, e)
            sleep(0.5)


    def read_data(self):
        data = self.__read_database.read_data()
        return data

    def check_database_has_records(self):
        data = self.__read_database.database_has_records()
        return data

    def delete_data(self, row_id):
        return self.__read_database.delete_data(row_id=row_id)

    def put(self, message):
        try:
            # During shut down the database after the first ocurness of size limit some data is lost
            if not self.stopped.is_set():

                if self.__write_database.reached_size_limit:
                    with self.__write_db_file_creation_lock:
                        new_db_name = self.__pointer.generate_new_file_name()
                        data_file_path_to_new_db_file = self._config["data_folder_path"] + new_db_name
                        self.__config_copy['data_file_path'] = data_file_path_to_new_db_file
                        if self.__read_database != self.__write_database:
                            self.__write_database.close_db()
                            self.__write_database.join(5)
                            del self.__write_database
                        else:
                            self.__read_database.should_write = False
                        sleep(0.1)
                        self.__write_database = Database(
                            self.__config_copy, self.write_queue, self.__log,
                            stopped=self.stopped, should_read=False, should_write=True
                        )
                        self.__write_database.start()
                        self.__pointer.update_write_database_file(new_db_name)
                self.__log.trace("Sending data to storage: %s", message)
                self.write_queue.put_nowait(message)
                return True
            else:
                return False
        except Full:
            self.__log.error("Storage queue is full! Failed to send data to storage!")
            return False
        except Exception as e:
            self.__log.exception("Failed to write data to storage! Error: %s", e)

    def stop(self):
        self.stopped.set()
        self.__read_database.close_db()
        self.__write_database.close_db()
        collect()

    def len(self):
        qsize = self.write_queue.qsize()
        stored_messages = self.__read_database.get_stored_messages_count()
        return qsize + stored_messages

    def update_logger(self):
        self.__log = getLogger("storage")
