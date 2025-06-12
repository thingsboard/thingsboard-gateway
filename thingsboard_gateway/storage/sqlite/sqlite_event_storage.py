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
import os

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
        self.__pointer = Pointer(self.__settings.data_folder_path, log=self.__log)
        self.__all_db_files = self.__pointer.sort_db_files()
        if (
            self.__all_db_files
            and self.__settings.db_file_name < self.__all_db_files[0]
        ):
            read_database_filename = self.__all_db_files[0]
            self.__config_copy["data_file_path"] = os.path.join(
                self.__settings.directory_path, read_database_filename
            )
            self.__read_database = Database(
                self.__config_copy, self.write_queue, self.__log, stopped=self.stopped
            )

        else:
            self.__read_database = Database(
                config, self.write_queue, self.__log, stopped=self.stopped
            )
        if self.__pointer.read_database_file == self.__pointer.write_database_file:
            self.__write_database = self.__read_database
            self.__write_database.start()

        else:
            write_database_name = self.__pointer.write_database_file
            data_file_path_to_new_db_file = os.path.join(
                self.__settings.directory_path, write_database_name
            )
            self.__config_copy["data_file_path"] = data_file_path_to_new_db_file
            self.__read_database.should_write = False
            self.__write_database = Database(
                self.__config_copy,
                self.write_queue,
                self.__log,
                stopped=self.stopped,
                should_read=False,
                should_write=True,
            )
            self.__read_database.start()
            self.__write_database.start()

        self.__write_database.init_table()
        self.__log.info("Sqlite storage initialized!")
        self.delete_time_point = 0
        self.__event_pack_processing_start = monotonic()
        self.last_read = time()

    def __assign_existing_read_database(self, read_database_filename: str):

        full_path_to_read_db_file = os.path.join(
            self.__settings.directory_path, read_database_filename
        )
        self.__config_copy["data_file_path"] = full_path_to_read_db_file
        self.__read_database = Database(
            self.__config_copy,
            self.write_queue,
            self.__log,
            stopped=self.stopped,
            should_read=True,
            should_write=False,
        )
        self.__read_database.start()
        self.__pointer.update_read_database_filename(read_database_filename)
        self.__log.info(
            "Sqlite storage updated read_database_file to: %s", read_database_filename
        )

    def __old_db_is_read_and_write_database_in_size_limit(self):
        self.__read_database = self.__write_database
        self.__read_database.should_read = True
        self.__pointer.update_read_database_filename(
            self.__read_database.settings.db_file_name
        )

    def get_event_pack(self):
        if not self.stopped.is_set():
            self.__event_pack_processing_start = monotonic()
            event_pack_messages = []
            data_from_storage = self.read_data()
            if self.__read_database.reached_size_limit and not data_from_storage:

                self.delete_oversize_db_file(
                    self.__read_database.settings.data_folder_path
                )
                self.delete_time_point = 0
                all_files = self.__pointer.sort_db_files()
                if len(all_files) > 1:
                    first_database_filename = all_files[0]

                    self.__assign_existing_read_database(
                        read_database_filename=first_database_filename
                    )
                else:
                    self.__old_db_is_read_and_write_database_in_size_limit()
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
                    self.__read_database.get_stored_messages_count(),
                )

            self.__read_database.can_prepare_new_batch()

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
        return event_pack_messages

    def event_pack_processing_done(self):
        self.__log.trace(
            "Batch processing done, processing time: %i milliseconds",
            int((monotonic() - self.__event_pack_processing_start) * 1000),
        )

        if not self.stopped.is_set():
            self.delete_data(self.delete_time_point)

        collect()

    def delete_oversize_db_file(self, path_to_oversize_db_file):
        try:
            timeout = 2.0
            start = monotonic()
            while (
                not self.__read_database.process_queue.empty()
                and monotonic() - start < timeout
            ):
                sleep(0.01)
            self.__read_database.db.commit()
            self.__read_database.interrupt()
            self.__read_database.join(timeout=1)

            self.__read_database.close_db()
            self.__read_database.db.close()
        except Exception:
            time.sleep(0.1)

        deleted_any = False
        for suffix in ("", "-shm", "-wal"):
            path = path_to_oversize_db_file + suffix
            if os.path.exists(path):
                try:
                    os.remove(path)
                    self.__log.info("Deleted %s", path)
                    deleted_any = True
                    sleep(0.05)
                except Exception as e:
                    self.__log.exception("Failed to delete %s: %s", path, e)
        if not deleted_any:
            self.__log.error(
                "No such DB files found to delete under %s", path_to_oversize_db_file
            )
        self.__read_database = None

    def read_data(self):
        data = self.__read_database.read_data()
        return data

    def delete_data(self, row_id):
        return self.__read_database.delete_data(row_id=row_id)

    def __max_db_amount_reached(self):
        if self.__settings.max_db_amount == len(self.__pointer.sort_db_files()):
            return True

    def put(self, message):
        try:
            if not self.stopped.is_set():
                if self.__write_database is None and self.__max_db_amount_reached():
                    return False

                if self.__write_database is None and not self.__max_db_amount_reached():
                    self.__create_new_database_existing_after_max_db_amount_is_read()
                    self.write_queue.put_nowait(message)
                    return True

                if self.__write_database.reached_size_limit:
                    self.__log.debug(
                        "Write database %s has reached its limit",
                        self.__write_database.settings.db_file_name,
                    )
                    self.__handle_write_database_reached_size()
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

    def __create_new_database_existing_after_max_db_amount_is_read(self):
        new_db_name = self.__pointer.generate_new_file_name()
        data_file_path = os.path.join(self.__settings.directory_path, new_db_name)
        self.__config_copy["data_file_path"] = data_file_path
        self.__write_database = Database(
            self.__config_copy,
            self.write_queue,
            self.__log,
            stopped=self.stopped,
            should_read=False,
            should_write=True,
        )
        self.__write_database.start()
        self.__pointer.update_write_database_file(new_db_name)

    def __handle_write_database_reached_size(self):

        new_db_name = self.__pointer.generate_new_file_name()
        data_file_path = os.path.join(self.__settings.directory_path, new_db_name)
        self.__config_copy["data_file_path"] = data_file_path
        if self.__read_database != self.__write_database:
            timeout = 2.0
            start = monotonic()
            while (
                not self.__write_database.process_queue.empty()
                and monotonic() - start < timeout
            ):
                sleep(0.01)
            self.__write_database.db.commit()
            self.__write_database.interrupt()
            self.__write_database.join(timeout=1)

            self.__write_database.close_db()
            self.__write_database.db.close()

            del self.__write_database
        else:
            self.__read_database.should_write = False

        if self.__max_db_amount_reached():
            self.__write_database = None
            return

        self.__write_database = Database(
            self.__config_copy,
            self.write_queue,
            self.__log,
            stopped=self.stopped,
            should_read=False,
            should_write=True,
        )
        self.__write_database.start()
        self.__pointer.update_write_database_file(new_db_name)

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
