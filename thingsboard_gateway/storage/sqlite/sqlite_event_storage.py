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
from threading import Event, Lock
from time import sleep, time, monotonic
from os import path, makedirs, remove
from typing import List
from sqlite3 import ProgrammingError, DatabaseError
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
        self.__read_db_file_change_lock = Lock()
        self.__write_db_file_creation_lock = Lock()
        self.__config_copy = copy.deepcopy(config)
        self.__settings = StorageSettings(config)
        self.create_folder()
        self.__pointer = Pointer(self.__settings.data_folder_path, log=self.__log, state_file_name="state.txt")
        self.__default_database_name = self.__settings.db_file_name
        self.__read_database_name_on_init, self.__databases_file_list_on_init = self.handle_database_files_on_gateway_init()
        self.is_max_db_amount_reached = False
        self.read_database_name = self.__read_database_name_on_init
        self.write_database_name = self.__pointer.write_database_file
        self.read_database_path = path.join(self.__settings.directory_path, self.read_database_name)
        self.write_database_path = path.join(
            self.__settings.directory_path, self.write_database_name
        )
        self.override_read_db_configuration = {
            **self.__config_copy,
            "data_file_path": self.read_database_path,
        }
        self.read_database_is_write_database = self.write_database_name == self.read_database_name

        self.read_database = Database(
            self.override_read_db_configuration,
            self.write_queue,
            self.__log,
            stopped=self.stopped,
            should_read=True,
            should_write=self.read_database_is_write_database,
        )
        self.read_database.start()

        if self.read_database_is_write_database:
            self.write_database = self.read_database

        else:
            self.override_write_configuration = {
                **self.__config_copy,
                "data_file_path": self.write_database_path,
            }
            self.write_database = Database(
                self.override_write_configuration,
                self.write_queue,
                self.__log,
                stopped=self.stopped,
                should_read=False,
                should_write=True,
            )
            self.write_database.start()

        self.write_database.init_table()
        self.__log.info(
            "Sqlite storage initialized (read=%s, write=%s)",
            self.read_database_name,
            self.write_database_name,
        )
        self.delete_time_point = 0
        self.__event_pack_processing_start = monotonic()
        self.last_read = time()

    def handle_database_files_on_gateway_init(self) -> tuple[str, list[str]]:
        all_db_files = self.__pointer.sort_db_files()
        if all_db_files and self.__default_database_name < all_db_files[0]:
            return all_db_files[0], all_db_files
        return self.__default_database_name, all_db_files

    def create_folder(self):

        if not path.exists(self.__settings.data_folder_path):
            directory = path.dirname(self.__settings.data_folder_path)
            if not path.exists(directory):
                self.__log.info("SQLite database file not found, creating new one...")
                try:
                    makedirs(directory)
                    self.__log.info(f"Directory {directory} created")
                except Exception as e:
                    self.__log.exception(
                        f"Failed to create directory {directory}", exc_info=e
                    )

    def assign_existing_read_database(self, read_database_filename: str):

        full_path_to_read_db_file = path.join(
            self.__settings.directory_path, read_database_filename
        )
        self.__config_copy["data_file_path"] = full_path_to_read_db_file
        self.read_database = Database(
            self.__config_copy,
            self.write_queue,
            self.__log,
            stopped=self.stopped,
            should_read=True,
            should_write=False,
        )
        self.read_database.start()
        self.__pointer.update_read_database_filename(read_database_filename)
        self.__log.info(
            "Sqlite storage updated read_database_file to: %s", read_database_filename
        )

    def handle_oversize(self, ) -> None:
        if self.read_database != self.write_database:
            timeout = 2.0
            start = monotonic()
            while (not self.write_database.process_queue.empty() and monotonic() - start < timeout):
                sleep(0.1)
            try:
                self.__clean_database_after_read_and_oversize()
                self.__log.info("Successfully handled SQLite storage on oversize")

            except RuntimeError as e:
                self.__log.error("During oversize clean: thread error: %s", e)

            except DatabaseError as e:
                self.__log.error("During oversize clean: database error: %s", e)

            except Exception as e:
                self.__log.exception("Unexpected error cleaning oversize DB: %s", e)


            finally:
                del self.write_database
        else:
            self.read_database.should_write = False

    def __clean_database_after_read_and_oversize(self) -> None:
        self.write_database.db.commit()
        try:
            self.write_database.interrupt()
            self.__log.info("Successfully interrupted the oversized database")

        except AttributeError as e:
            self.__log.debug("No interrupt() on write_database")

        try:
            self.write_database.join(timeout=1)
            if self.write_database.is_alive():
                self.__log.warning("DB thread still alive after join timeout")

        except RuntimeError as e:
            self.__log.error("Failed to join DB thread: %s", e)

        except Exception as e:
            self.__log.error("Failed to join DB thread: due to %s", e)
        try:
            self.write_database.close_db()
            self.__log.info("Closed oversize DB connection")

        except ProgrammingError as e:
            self.__log.warning("Close called on already-closed DB: %s", e)

        except Exception as e:
            self.__log.warning("Close called on already-closed DB: %s", e)

        self.write_database.db.close()

    def old_db_is_read_and_write_database_in_size_limit(self):
        self.read_database = self.write_database
        self.read_database.should_read = True
        self.__pointer.update_read_database_filename(self.read_database.settings.db_file_name)

    def handle(self):
        all_files = self.__pointer.sort_db_files()
        if len(all_files) > 1:
            self.assign_existing_read_database(all_files[0])
        else:
            self.old_db_is_read_and_write_database_in_size_limit()

    def handle_max_db_amount_reached(self):
        if self.max_db_amount_reached():
            self.is_max_db_amount_reached = True
            return True
        if self.is_max_db_amount_reached:
            self.is_max_db_amount_reached = False
            new_write_database_name = self.__change_database_config()
            self.__create_new_write_database()
            self.__databases_file_list_on_init.append(new_write_database_name)
            self.__pointer.update_write_database_file(new_write_database_name)
            return False

    def get_event_pack(self):
        if not self.stopped.is_set():
            self.__event_pack_processing_start = monotonic()
            event_pack_messages = []
            data_from_storage = self.read_data()

            event_pack_messages = self.process_event_storage_data(
                data_from_storage=data_from_storage,
                event_pack_messages=event_pack_messages,
            )

            if event_pack_messages:
                self.__log.trace(
                    "Retrieved %r records from storage in %r ms, left in storage: %r",
                    len(event_pack_messages),
                    int((monotonic() - self.__event_pack_processing_start) * 1000),
                    self.read_database.get_stored_messages_count(),
                )

            self.read_database.can_prepare_new_batch()

            return event_pack_messages

        else:
            return []

    def process_event_storage_data(self, data_from_storage, event_pack_messages):

        if not data_from_storage:
            return []
        for row in data_from_storage:
            try:
                if not row:
                    return []
                element_to_insert = row["message"]
                if not element_to_insert:
                    continue

                event_pack_messages.append(element_to_insert)
                if not self.delete_time_point or self.delete_time_point < row["id"]:
                    self.delete_time_point = row["id"]
            except (IndexError, KeyError) as e:
                self.__log.error(
                    "IndexError occurred while reading data from storage: %s", e
                )
                continue
            except Exception as e:
                self.__log.error(
                    "Error occurred while reading data from storage: %s", e
                )

        return event_pack_messages

    def event_pack_processing_done(self):
        self.__log.trace(
            "Batch processing done, processing time: %i milliseconds",
            int((monotonic() - self.__event_pack_processing_start) * 1000),
        )

        if not self.stopped.is_set():
            self.delete_data(self.delete_time_point)
            with self.__read_db_file_change_lock:
                if not self.read_database.database_has_records():
                    self.read_database.process_file_limit(self.read_database.settings.data_folder_path,
                                                          self.__settings.size_limit)
                    if self.read_database.reached_size_limit:
                        self.delete_oversize_db_file(self.read_database.settings.data_folder_path)
                        self.delete_time_point = 0
                        self.handle()
                        self.handle_max_db_amount_reached()

        collect()

    def delete_oversize_db_file(self, path_to_oversize_db_file):
        try:
            timeout = 2.0
            start = monotonic()
            while (
                    not self.read_database.process_queue.empty()
                    and monotonic() - start < timeout
            ):
                sleep(0.01)
            self.read_database.db.commit()
            self.read_database.interrupt()
            self.read_database.join(timeout=1)

            self.read_database.close_db()
            self.read_database.db.close()
        except Exception:
            time.sleep(0.1)

        deleted_any = False
        for suffix in ("", "-shm", "-wal"):
            path_to_db = path_to_oversize_db_file + suffix
            if path.exists(path_to_db):
                try:
                    remove(path_to_db)
                    self.__log.info("Deleted %s", path)
                    deleted_any = True
                    sleep(0.05)
                except Exception as e:
                    self.__log.exception("Failed to delete %s: %s", path, e)
        if not deleted_any:
            self.__log.error(
                "No such DB files found to delete under %s", path_to_oversize_db_file
            )

    def read_data(self):
        data = self.read_database.read_data()
        return data

    def delete_data(self, row_id):
        return self.read_database.delete_data(row_id=row_id)

    def max_db_amount_reached(self):
        if self.__settings.max_db_amount == len(self.__pointer.sort_db_files()):
            return True

    def __change_database_config(self):
        new_db_name = self.__pointer.generate_new_file_name()
        data_file_path = path.join(self.__settings.directory_path, new_db_name)
        self.__config_copy["data_file_path"] = data_file_path
        return new_db_name

    def put(self, message):
        try:
            if self.is_max_db_amount_reached:
                return False

            if not self.stopped.is_set():
                if self.write_database.reached_size_limit and self.handle_max_db_amount_reached():
                    return False
                with self.__write_db_file_creation_lock:

                    if self.write_database.reached_size_limit:
                        self.__log.debug(
                            "Write database %s has reached its limit",
                            self.write_database.settings.db_file_name,
                        )

                        new_write_database_name = self.__change_database_config()
                        self.handle_oversize()
                        self.__create_new_write_database()
                        self.__pointer.update_write_database_file(new_write_database_name)

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

    def __create_new_write_database(self):
        self.write_database = Database(
            self.__config_copy,
            self.write_queue,
            self.__log,
            stopped=self.stopped,
            should_read=False,
            should_write=True,
        )
        self.write_database.start()

    def stop(self):
        self.stopped.set()
        self.read_database.close_db()
        self.write_database.close_db()
        collect()

    def len(self):
        qsize = self.write_queue.qsize()
        stored_messages = self.read_database.get_stored_messages_count()
        return qsize + stored_messages

    def update_logger(self):
        self.__log = getLogger("storage")
