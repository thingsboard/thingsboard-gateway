import copy
from gc import collect
from logging import getLogger
from os import path, makedirs, remove
from queue import Queue, Full
from sqlite3 import ProgrammingError, DatabaseError
from threading import Event
from time import sleep, monotonic

from thingsboard_gateway.storage.event_storage import EventStorage
from thingsboard_gateway.storage.sqlite.database import Database
from thingsboard_gateway.storage.sqlite.sqlite_event_storage_pointer import Pointer
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
        self.__log.debug("Read DB thread started for %s", self.__read_database_name)

        if self.__read_database_is_write_database:
            self.__write_database = self.__read_database
            self.__log.debug("Write DB is same as read DB")
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
            self.__log.debug(
                "Write DB thread started for %s", self.__write_database_name
            )

        self.__write_database.init_table()
        self.__log.info(
            "Sqlite storage initialized (read=%s, write=%s)",
            self.__read_database_name,
            self.__write_database_name,
        )
        if self.__read_database.reached_size_limit and not self.__read_database.database_has_records():
            self.__log.debug("Initial read DB oversize & empty → rotating")
            self.__rotate_after_read_completion()
        self.__initial_db_file_list = self.__pointer.sort_db_files()
        self.delete_time_point = 0
        self.__event_pack_processing_start = monotonic()

    def __select_initial_db_files(self):
        all_db_files = self.__pointer.sort_db_files()
        if len(all_db_files) == 1 and self.__default_database_name < all_db_files[0]:
            return all_db_files[0], all_db_files[0]
        if len(all_db_files) > 1:
            return all_db_files[0], all_db_files[-1]
        return self.__default_database_name, self.__default_database_name

    def __sort_db_files_list(self):
        all_db_files_from_list = sorted(filter(lambda file: file.endswith(".db"), self.__initial_db_file_list))
        return all_db_files_from_list

    def __ensure_data_folder_exists(self):
        data_path = self.__settings.data_folder_path
        if not path.exists(data_path):
            directory = path.dirname(data_path)
            if not path.exists(directory):
                self.__log.debug("Data folder missing, creating %s", directory)
                try:
                    makedirs(directory)
                    self.__log.info("Created directory %s", directory)
                except OSError as e:
                    self.__log.error("OS error creating %s: %s", directory, e)
                    self.__log.debug("OS stack:", exc_info=e)
                except Exception as e:
                    self.__log.exception("Unexpected error creating %s", directory)
                    self.__log.debug("Stack:", exc_info=e)

    def __start_read_database(self, read_database_filename: str):
        full_path = path.join(self.__settings.directory_path, read_database_filename)
        self.__config_copy["data_file_path"] = full_path
        try:
            self.__read_database = Database(
                self.__config_copy,
                self.write_queue,
                self.__log,
                stopped=self.stopped,
                should_read=True,
                should_write=False,
            )
            self.__read_database.start()
            self.__log.debug("Switched read DB to %s", read_database_filename)
        except Exception:
            self.__log.exception("Failed to start read DB %s", read_database_filename)

    def __cleanup_oversized_db(self) -> None:
        if self.__read_database != self.__write_database:
            timeout = 2.0
            start = monotonic()
            while not self.__write_database.process_queue.empty() and monotonic() - start < timeout:
                sleep(0.1)
            try:
                self.__finalize_oversized_db_cleanup()
                self.__log.debug("Oversize DB cleaned up")
            except RuntimeError as e:
                self.__log.debug("Thread error during oversize cleanup: %s", e)
            except DatabaseError as e:
                self.__log.debug("DB error during oversize cleanup: %s", e)
            except Exception:
                self.__log.debug("Unexpected error cleaning oversize DB")
            finally:
                del self.__write_database
        else:
            self.__log.trace("Oversize cleanup skipped (read==write)")
            self.__read_database.should_write = False

    def __finalize_oversized_db_cleanup(self) -> None:
        self.__write_database.db.commit()
        try:
            self.__write_database.interrupt()
            self.__log.trace("Interrupted oversize DB thread")
        except AttributeError:
            pass
        try:
            self.__write_database.join(timeout=1)
            if self.__write_database.is_alive():
                self.__log.warning("DB thread alive after join timeout")
        except RuntimeError as e:
            self.__log.debug("Join runtime error: %s", e)
        except Exception as e:
            self.__log.debug("Join error: %s", e)
        try:
            self.__write_database.close_db()
            self.__log.trace("Closed oversize DB connection")
        except ProgrammingError as e:
            self.__log.debug("Close_db on closed DB: %s", e)
        except Exception as e:
            self.__log.debug("Close_db error: %s", e)
        self.__write_database.db.close()

    def __rotate_read_database(self):
        self.__log.debug("Rotating read database")
        all_files = self.__sort_db_files_list()
        if len(all_files) > 1:
            self.__start_read_database(all_files[0])
        else:
            self.__log.trace("Only one DB left, reusing write DB")
            self.__read_database = self.__write_database
            self.__read_database.should_read = True

    def __check_and_handle_max_db_count(self) -> bool:
        count = len(self.__sort_db_files_list())
        if count >= self.__settings.max_db_amount:
            self.__is_max_db_amount_reached = True
            self.__log.warning("Max DB count (%d) reached—dropping writes", count)
            return True
        if self.__is_max_db_amount_reached:
            self.__log.debug("DB count cleared, creating new write DB")
            self.__is_max_db_amount_reached = False
            self.__prepare_new_db_configuration()
            self.__start_write_database()
        return False

    def __rotate_after_read_completion(self):
        self.__log.debug("Rotate after read completion")
        self.__delete_db_files(self.__read_database.settings.data_folder_path)
        self.delete_time_point = 0
        self.__rotate_read_database()
        self.__check_and_handle_max_db_count()

    def event_pack_processing_done(self):
        self.__log.trace(
            "Batch done in %d ms",
            int((monotonic() - self.__event_pack_processing_start) * 1000),
        )
        if not self.stopped.is_set():
            try:
                self.delete_data(self.delete_time_point)
                if not self.__read_database.database_has_records():
                    self.__read_database.process_file_limit()
                    if self.__read_database.reached_size_limit:
                        self.__rotate_after_read_completion()
            except Exception:
                self.__log.exception("Error in event_pack_processing_done")
        collect()

    def get_event_pack(self):
        if self.stopped.is_set():
            return []
        self.__event_pack_processing_start = monotonic()
        event_pack_messages = []
        data_from_storage = self.read_data()
        if not data_from_storage and not path.exists(self.__read_database.settings.data_folder_path):
            self.__rotate_after_read_completion()
        for row in data_from_storage or []:
            try:
                element = row["message"]
                if element:
                    event_pack_messages.append(element)
                    if not self.delete_time_point or self.delete_time_point < row["id"]:
                        self.delete_time_point = row["id"]
            except KeyError as e:
                self.__log.error("KeyError reading storage: %s", e)
                self.__log.debug("Stack:", exc_info=e)
            except Exception as e:
                self.__log.error("Error reading storage: %s", e)
                self.__log.debug("Stack:", exc_info=e)
        if event_pack_messages:
            self.__log.trace(
                "Retrieved %d msgs in %d ms, remaining: %d",
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
            while not self.__read_database.process_queue.empty() and monotonic() - start < timeout:
                sleep(0.05)
            self.__read_database.db.commit()
            self.__read_database.interrupt()
            self.__read_database.join(timeout=1)
            self.__read_database.close_db()
            self.__read_database.db.close()
        except Exception:
            self.__log.debug("Interruption during delete cleanup")
            sleep(0.1)

        deleted = False
        for suffix in ("", "-shm", "-wal"):
            full = path_to_db_file + suffix
            if path.exists(full):
                try:
                    remove(full)
                    deleted = True
                except Exception as e:
                    self.__log.exception("Failed delete %s: %s", full, e)
        if deleted:
            self.__initial_db_file_list.remove(self.__read_database.settings.db_file_name)
        if not deleted:
            self.__log.error("No DB files to delete under %s", path_to_db_file)

    def read_data(self):
        return self.__read_database.read_data()

    def delete_data(self, row_id):
        return self.__read_database.delete_data(row_id=row_id)

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
        self.__log.debug(
            "Started new write DB %s", self.__write_database.settings.db_file_name
        )
        self.__initial_db_file_list.append(self.__write_database.settings.db_file_name)

    def put(self, message):
        try:
            if self.__is_max_db_amount_reached:
                return False
            if not self.stopped.is_set():
                if self.__write_database.reached_size_limit and self.__check_and_handle_max_db_count():
                    return False
                if self.__write_database.reached_size_limit:
                    self.__log.debug(
                        "Write DB %s reached limit",
                        self.__write_database.settings.db_file_name,
                    )
                    self.__prepare_new_db_configuration()
                    self.__cleanup_oversized_db()
                    self.__start_write_database()
                self.__log.trace("Queuing message: %r", message)
                self.write_queue.put_nowait(message)
                return True
            return False
        except Full:
            self.__log.error("Storage queue full—dropped message")
            return False
        except Exception:
            self.__log.exception("Failed to put message")

    def stop(self):
        self.stopped.set()
        self.__read_database.close_db()
        self.__write_database.close_db()
        collect()

    def len(self):
        return self.write_queue.qsize() + self.__read_database.get_stored_messages_count()

    def update_logger(self):
        self.__log = getLogger("storage")
