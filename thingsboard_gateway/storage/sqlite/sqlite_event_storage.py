import copy
from gc import collect
from threading import Event
from time import sleep, monotonic
from os import path, makedirs, remove
from sqlite3 import ProgrammingError, DatabaseError
from thingsboard_gateway.storage.event_storage import EventStorage
from thingsboard_gateway.storage.sqlite.database import Database
from thingsboard_gateway.storage.sqlite.sqlite_event_storage_pointer import Pointer
from queue import Queue, Full
from logging import getLogger
from thingsboard_gateway.storage.sqlite.storage_settings import StorageSettings


class SQLiteEventStorage(EventStorage):

    def __init__(self, config, logger, main_stop_event):
        super().__init__(config, logger, main_stop_event)
        self.__log = logger
        self.__log.info("Sqlite Storage initializing...")
        self.write_queue = Queue(-1)
        self.stopped = Event()
        self.__config_copy = copy.deepcopy(config)
        self.__settings = StorageSettings(config)
        self.__ensure_data_folder_exists()
        self.__pointer = Pointer(self.__settings.data_folder_path, log=self.__log)
        self.__default_database_name = self.__settings.db_file_name
        self.__read_database_name_on_init, self.__write_database_name_on_init = (
            self.__select_initial_db_files()
        )
        self.__is_max_db_amount_reached = False

        self.__read_database_name = self.__read_database_name_on_init
        self.__write_database_name = self.__write_database_name_on_init
        self.__read_database_path = path.join(
            self.__settings.directory_path, self.__read_database_name
        )
        self.__write_database_path = path.join(
            self.__settings.directory_path, self.__write_database_name
        )
        self.__override_read_db_configuration = {
            **self.__config_copy,
            "data_file_path": self.__read_database_path,
        }
        self.__read_database_is_write_database = (
            self.__read_database_name == self.__write_database_name
        )

        self.__read_database = Database(
            self.__override_read_db_configuration,
            self.write_queue,
            self.__log,
            stopped=self.stopped,
            should_read=True,
            should_write=self.__read_database_is_write_database,
        )
        self.__read_database.start()

        if self.__read_database_is_write_database:
            self.__write_database = self.__read_database
        else:
            self.__override_write_configuration = {
                **self.__config_copy,
                "data_file_path": self.__write_database_path,
            }
            self.__write_database = Database(
                self.__override_write_configuration,
                self.write_queue,
                self.__log,
                stopped=self.stopped,
                should_read=False,
                should_write=True,
            )
            self.__write_database.start()

        self.__write_database.init_table()
        self.__log.info(
            "Sqlite storage initialized (read=%s, write=%s)",
            self.__read_database_name,
            self.__write_database_name,
        )
        if (
            self.__read_database.reached_size_limit
            and not self.__read_database.database_has_records()
        ):
            self.__rotate_after_read_completion()

        self.delete_time_point = 0
        self.__event_pack_processing_start = monotonic()

    def __select_initial_db_files(self):
        all_db_files = self.__pointer.sort_db_files()
        if len(all_db_files) == 1 and self.__default_database_name < all_db_files[0]:
            return all_db_files[0], all_db_files[0]
        if len(all_db_files) > 1:
            return all_db_files[0], all_db_files[-1]
        return self.__default_database_name, self.__default_database_name

    def __ensure_data_folder_exists(self):
        if not path.exists(self.__settings.data_folder_path):
            directory = path.dirname(self.__settings.data_folder_path)
            if not path.exists(directory):
                self.__log.info("SQLite database file not found, creating new one...")
                try:
                    makedirs(directory)
                except Exception as e:
                    self.__log.exception(f"Failed to create directory {directory}", exc_info=e)

    def __start_read_database(self, read_database_filename: str):
        full_path = path.join(self.__settings.directory_path, read_database_filename)
        self.__config_copy["data_file_path"] = full_path
        self.__read_database = Database(
            self.__config_copy,
            self.write_queue,
            self.__log,
            stopped=self.stopped,
            should_read=True,
            should_write=False,
        )
        self.__read_database.start()
        self.__log.info("Sqlite storage updated read_database_file to: %s", read_database_filename)

    def __cleanup_oversized_db(self) -> None:
        if self.__read_database != self.__write_database:
            timeout = 2.0
            start = monotonic()
            while (
                not self.__write_database.process_queue.empty()
                and monotonic() - start < timeout
            ):
                sleep(0.1)
            try:
                self.__finalize_oversized_db_cleanup()
                self.__log.info("Successfully handled SQLite storage on oversize")
            except RuntimeError as e:
                self.__log.error("During oversize clean: thread error: %s", e)
            except DatabaseError as e:
                self.__log.error("During oversize clean: database error: %s", e)
            except Exception as e:
                self.__log.exception("Unexpected error cleaning oversize DB: %s", e)
            finally:
                del self.__write_database
        else:
            self.__read_database.should_write = False

    def __finalize_oversized_db_cleanup(self) -> None:
        self.__write_database.db.commit()
        try:
            self.__write_database.interrupt()
        except AttributeError:
            pass
        try:
            self.__write_database.join(timeout=1)
            if self.__write_database.is_alive():
                self.__log.warning("DB thread still alive after join timeout")
        except RuntimeError as e:
            self.__log.error("Failed to join DB thread: %s", e)
        except Exception as e:
            self.__log.error("Failed to join DB thread: due to %s", e)
        try:
            self.__write_database.close_db()
        except ProgrammingError as e:
            self.__log.warning("Close called on already-closed DB: %s", e)
        except Exception as e:
            self.__log.warning("Close called on already-closed DB: %s", e)
        self.__write_database.db.close()

    def __rotate_read_database(self):
        all_files = self.__pointer.sort_db_files()
        if len(all_files) > 1:
            self.__start_read_database(all_files[0])
        else:
            self.__read_database = self.__write_database
            self.__read_database.should_read = True

    def __check_and_handle_max_db_count(self) -> bool:
        if len(self.__pointer.sort_db_files()) >= self.__settings.max_db_amount:
            self.__is_max_db_amount_reached = True
            return True
        if self.__is_max_db_amount_reached:
            self.__is_max_db_amount_reached = False
            self.__prepare_new_db_configuration()
            self.__start_write_database()
        return False

    def __rotate_after_read_completion(self):
        self.__delete_db_files(self.__read_database.settings.data_folder_path)
        self.delete_time_point = 0
        self.__rotate_read_database()
        self.__check_and_handle_max_db_count()

    def event_pack_processing_done(self):
        self.__log.trace(
            "Batch processing done, processing time: %i milliseconds",
            int((monotonic() - self.__event_pack_processing_start) * 1000),
        )
        if not self.stopped.is_set():
            self.delete_data(self.delete_time_point)
            if not self.__read_database.database_has_records():
                self.__read_database.process_file_limit(
                )
                if self.__read_database.reached_size_limit:
                    self.__rotate_after_read_completion()
        collect()

    def get_event_pack(self):
        if self.stopped.is_set():
            return []
        self.__event_pack_processing_start = monotonic()
        event_pack_messages = []
        data_from_storage = self.read_data()
        if not data_from_storage and not path.exists(
            self.__read_database.settings.data_folder_path
        ):
            self.__rotate_after_read_completion()
        for row in data_from_storage or []:
            try:
                element_to_insert = row["message"]
                if element_to_insert:
                    event_pack_messages.append(element_to_insert)
                    if not self.delete_time_point or self.delete_time_point < row["id"]:
                        self.delete_time_point = row["id"]
            except KeyError as e:
                self.__log.error("KeyError occurred while reading data from storage: %s", e)
            except Exception as e:
                self.__log.error("Error occurred while reading data from storage: %s", e)
        if event_pack_messages:
            self.__log.trace(
                "Retrieved %r records from storage in %r ms, left in storage: %r",
                len(event_pack_messages),
                int((monotonic() - self.__event_pack_processing_start) * 1000),
                self.__read_database.get_stored_messages_count(),
            )
        self.__read_database.can_prepare_new_batch()
        return event_pack_messages

    def __delete_db_files(self, path_to_db_file):
        try:
            timeout = 2.0
            start = monotonic()
            while (
                not self.__read_database.process_queue.empty()
                and monotonic() - start < timeout
            ):
                sleep(0.05)
            self.__read_database.db.commit()
            self.__read_database.interrupt()
            self.__read_database.join(timeout=1)
            self.__read_database.close_db()
            self.__read_database.db.close()
        except Exception:
            sleep(0.1)
        deleted_any = False
        for suffix in ("", "-shm", "-wal"):
            full = path_to_db_file + suffix
            if path.exists(full):
                try:
                    remove(full)
                    deleted_any = True
                except Exception as e:
                    self.__log.exception("Failed to delete %s: %s", full, e)
        if not deleted_any:
            self.__log.error("No such DB files found to delete under %s", path_to_db_file)

    def read_data(self):
        return self.__read_database.read_data()

    def delete_data(self, row_id):
        return self.__read_database.delete_data(row_id=row_id)

    def maximum_db_amount_reached(self) -> bool:
        return len(self.__pointer.sort_db_files()) >= self.__settings.max_db_amount

    def __prepare_new_db_configuration(self) -> str:
        new_db_name = self.__pointer.generate_new_file_name()
        data_file_path = path.join(self.__settings.directory_path, new_db_name)
        self.__config_copy["data_file_path"] = data_file_path
        return new_db_name

    def __start_write_database(self):
        self.__write_database = Database(
            self.__config_copy,
            self.write_queue,
            self.__log,
            stopped=self.stopped,
            should_read=False,
            should_write=True,
        )
        self.__write_database.start()

    def put(self, message):
        try:
            if self.__is_max_db_amount_reached:
                return False
            if not self.stopped.is_set():
                if (
                    self.__write_database.reached_size_limit
                    and self.__check_and_handle_max_db_count()
                ):
                    return False
                if self.__write_database.reached_size_limit:
                    self.__log.debug(
                        "Write database %s has reached its limit",
                        self.__write_database.settings.db_file_name,
                    )
                    self.__prepare_new_db_configuration()
                    self.__cleanup_oversized_db()
                    self.__start_write_database()
                self.__log.trace("Sending data to storage: %s", message)
                self.write_queue.put_nowait(message)
                return True
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
        return qsize + self.__read_database.get_stored_messages_count()

    def update_logger(self):
        self.__log = getLogger("storage")
