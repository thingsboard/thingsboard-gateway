import os
import threading
import time
import logging
from json import load, dump, loads, dumps

log = logging.getLogger(__name__)


class TBEventStorage:
    class TBStorageInitializationError(Exception):
        def __init__(self, message):
            super(Exception, self).__init__(message)

    class __TBEventStorageDir:
        def __init__(self, data_folder_path, max_file_count):
            self.__max_file_count = max_file_count
            self.__data_folder_path = data_folder_path
            self.__init_data_folder(data_folder_path)
            self.__init_data_files(data_folder_path)

        @staticmethod
        def __init_data_folder(data_folder_path):
            if not os.path.exists(data_folder_path):
                log.info("Creating new storage data directory: %s", data_folder_path)
                os.makedirs(data_folder_path)

        @staticmethod
        def get_new_file_name():
            return 'data_' + str(time.time()) + '.txt'

        @staticmethod
        def file_line_count(file_pass):
            i = 0
            if os.path.isfile(file_pass):
                with open(file_pass, 'r') as f:
                    for line_number, line in enumerate(f):
                        i = line_number + 1
            return i

        def current_file_name(self):
            return self.__files[-1]

        def register_new_file(self, file_name):
            return self.__files.append(file_name)

        def get_full_file_name(self, current_file_name):
            return self.__data_folder_path + "/" + current_file_name

        def __init_data_files(self, data_folder_path):
            files = os.listdir(data_folder_path)
            files = list(filter(lambda file_name: file_name.startswith('data_'), files))
            files.sort()
            self.__files = files
            if len(self.__files) > 0:
                log.info("Following files found: %s", self.__files)
            else:
                files.append(self.get_new_file_name())

        def cleanup(self):
            while len(self.__files) > self.__max_file_count:
                oldest_file = self.__files.pop(0)
                log.info("Deleting old data file: %s", oldest_file)
                os.remove(self.get_full_file_name(oldest_file))
            pass

    class __TBEventStorageReaderState:
        def __init__(self, current_file_name, current_file_pos):
            self.current_file_name = current_file_name
            self.current_file_pos = current_file_pos

        def __str__(self):
            return '[' + str(self.current_file_name) + '|' + str(self.current_file_pos) + ']'

    class __TBEventStorageWriterState:
        def __init__(self, p_dir, max_records_per_file, max_records_between_fsync):
            self.__dir = p_dir
            self.__max_records_per_file = max_records_per_file
            self.__max_records_between_fsync = max_records_between_fsync
            self.current_file_name = self.__dir.current_file_name()
            self.current_file_size = self.__dir.file_line_count(self.__dir.get_full_file_name(self.current_file_name))
            self.__writer_fd = open(self.__dir.get_full_file_name(self.current_file_name), 'a')

        def is_full(self):
            return self.current_file_size >= self.__max_records_per_file

        def switch_to_new_file(self):
            self.__writer_fd.close()
            self.current_file_name = self.__dir.get_new_file_name()
            self.current_file_size = 0
            self.__writer_fd = open(self.__dir.get_full_file_name(self.current_file_name), 'a')
            self.__dir.register_new_file(self.current_file_name)
            pass

        def write(self, data):
            self.__writer_fd.write(data)
            self.current_file_size = self.current_file_size + 1
            if self.current_file_size % self.__max_records_between_fsync == 0:
                self.__writer_fd.flush()

        def __str__(self):
            return '[' + str(self.current_file_name) + '|' + str(self.current_file_size) + ']'

    def __init__(self, data_folder_path, max_records_per_file, max_records_between_fsync, max_file_count):
        if max_records_per_file <= 0:
            raise self.TBStorageInitializationError("'Max records per file' parameter is <= 0")
        elif max_records_per_file > 1000000:
            raise self.TBStorageInitializationError("'Max records per file' parameter is > 1000000")
        if max_records_between_fsync <= 0:
            raise self.TBStorageInitializationError("'Max records between fsync' parameter is <= 0")
        elif max_records_between_fsync > max_records_per_file:
            raise self.TBStorageInitializationError("'Max records between fsync' is bigger "
                                                    "then 'max records per file' parameter")
        if max_file_count <= 0:
            raise self.TBStorageInitializationError("'Max data files' parameter is <= 0")
        elif max_file_count > 1000000:
            raise self.TBStorageInitializationError("'Max data files' parameter is > 1000000")
        self.__writer_lock = threading.Lock()
        self.__dir = self.__TBEventStorageDir(data_folder_path, max_file_count)
        self.__init_reader_state(data_folder_path)
        self.__writer = self.__TBEventStorageWriterState(self.__dir, max_records_per_file, max_records_between_fsync)

    def __init_reader_state(self, data_folder_path):
        self.__reader_state_file_name = data_folder_path + ".reader_state"
        if os.path.isfile(self.__reader_state_file_name):
            with open(self.__reader_state_file_name, 'r') as state_file:
                data = load(state_file)
                self.__reader_state = self.__TBEventStorageReaderState(data['current_file_name'],
                                                                       data['current_file_pos'])
        else:
            self.__reader_state = self.__TBEventStorageReaderState(self.__dir.current_file_name(), 0)
        log.info("Reader state: %s", self.__reader_state)

    def write(self, data):
        if self.__writer_lock.acquire(True, 60):
            try:
                if self.__writer.is_full():
                    log.debug("Writer is full: %s", self.__writer)
                    self.__writer.switch_to_new_file()
                    self.__dir.cleanup()
                self.__writer.write(data)
            finally:
                self.__writer_lock.release()
        else:
            log.warning("Failed to acquire write log for an event storage!")
