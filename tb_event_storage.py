import os
import threading
import time
import logging
from json import load, dump, loads
from datetime import datetime
from json import JSONDecodeError
log = logging.getLogger(__name__)


class TBEventStorage:
    class TBStorageInitializationError(Exception):
        def __init__(self, message):
            super(Exception, self).__init__(message)

    class TBEndOfEventStorageError(Exception):
        pass

    class __TBEventStorageDir:
        def __init__(self, data_folder_path, max_file_count):
            self.__max_file_count = max_file_count
            self._data_folder_path = data_folder_path
            self.files = None
            self.__init_data_folder(data_folder_path)
            self.init_data_files()

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
            line_number = 0
            if os.path.isfile(file_pass):
                with open(file_pass, 'r') as f:
                    for line_number, l in enumerate(f):
                        pass
            return line_number + 1

        def current_file_name(self):
            return self.files[-1]

        def register_new_file(self, file_name):
            return self.files.append(file_name)

        def get_full_file_name(self, current_file_name):
            return self._data_folder_path + "/" + current_file_name

        def init_data_files(self):
            files = os.listdir(self._data_folder_path)
            files = list(filter(lambda file_name: file_name.startswith('data_'), files))
            files.sort()
            self.files = files
            if len(self.files) > 0:
                log.info("Following files found: %s", self.files)
            else:
                files.append(self.get_new_file_name())

        def cleanup(self):
            while len(self.files) > self.__max_file_count:
                oldest_file = self.files.pop(0)
                log.info("Deleting old data file: %s", oldest_file)
                os.remove(self.get_full_file_name(oldest_file))
            pass

    class __TBEventStorageReaderState:
        def __init__(self, current_file_name, current_file_pos,  read_interval, max_read_record_count, storage):
            self.current_file_name = current_file_name
            self.current_file_pos = current_file_pos
            self.read_interval = read_interval
            self.max_read_record_count = max_read_record_count
            self.storage = storage
            self.lock = storage.writer_lock

        def __str__(self):
            return '[' + str(self.current_file_name) + '|' + str(self.current_file_pos) + ']'

        def read(self):
            to_read = self.max_read_record_count
            result = []
            prev_file = None
            pos = 0
            if os.path.isfile(".reader_state"):
                with open(".reader_state") as reader_file:
                    try:
                        state = load(reader_file)
                        try:
                            pos = state["pos"]
                            prev_file = state["prev_file"]
                        except KeyError:
                            pass
                    except JSONDecodeError:
                        pass
            with self.lock:
                # get list of files in data dir
                self.storage.dir.init_data_files()
                files = self.storage.dir.files
                while to_read > 0:
                    # acknowledge previous file
                    if prev_file and os.path.isfile("data/"+prev_file):
                        current_file = prev_file
                    # else get oldest file and set pos to 0
                    else:
                        try:
                            current_file = files[0]
                        except IndexError:
                            # if no files left, on open we get FileNotFound and exit from loop
                            current_file = None
                        pos = 0
                    try:
                        with open("data/"+current_file) as f:
                            # read file by line and add to result lines after pos
                            current_line = 0
                            for line_number, line in enumerate(f):
                                if to_read > 0 and pos <= line_number:
                                    result.append(line.strip())
                                    to_read -= 1
                                    current_line = line_number
                            # if we read needed amount of lines, recognise position and file and save to .reader_state
                            if to_read == 0:
                                prev_file = current_file
                                pos = current_line
                            # else try to get newer data file
                            else:
                                try:
                                    prev_file = files[files.index(current_file) + 1]
                                except IndexError:
                                    # if there are not any data files save position and current file to .reader_state
                                    to_read = 0
                                    with open(".reader_state", "w") as reader_file:
                                        dump({"prev_file": current_file, "pos": current_line + 1}, reader_file)
                                    raise TBEventStorage.TBEndOfEventStorageError()
                                pos = 0
                            with open(".reader_state", "w") as reader_file:
                                dump({"prev_file": current_file, "pos": pos}, reader_file)
                    except FileNotFoundError:
                        pass
                    except TBEventStorage.TBEndOfEventStorageError:
                        break
            return result

    class __TBEventStorageWriterState:
        def __init__(self, p_dir, max_records_per_file, max_records_between_fsync):
            self.dir = p_dir
            self.__max_records_per_file = max_records_per_file
            self.__max_records_between_fsync = max_records_between_fsync
            self.current_file_name = self.dir.current_file_name()
            self.current_file_size = self.dir.file_line_count(self.dir.get_full_file_name(self.current_file_name))
            self.__writer_fd = open(self.dir.get_full_file_name(self.current_file_name), 'a')

        def is_full(self):
            return self.current_file_size >= self.__max_records_per_file

        def switch_to_new_file(self):
            self.__writer_fd.close()
            self.current_file_name = self.dir.get_new_file_name()
            self.current_file_size = 0
            self.__writer_fd = open(self.dir.get_full_file_name(self.current_file_name), 'a')
            self.dir.register_new_file(self.current_file_name)
            pass

        def write(self, data):
            self.__writer_fd.write(data)
            self.current_file_size = self.current_file_size + 1
            if self.current_file_size % self.__max_records_between_fsync == 0:
                self.__writer_fd.flush()

        def __str__(self):
            return '[' + str(self.current_file_name) + '|' + str(self.current_file_size) + ']'

    def __init__(self, data_folder_path, max_records_per_file, max_records_between_fsync, max_file_count,
                 read_interval, max_read_record_count, scheduler, gateway):
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
        if read_interval <= 0:
            raise self.TBStorageInitializationError("'Read interval' parameter is <= 0")
        if max_read_record_count <= 0:
            raise self.TBStorageInitializationError("'Max read record count' parameter is <= 0")
        self.writer_lock = threading.Lock()
        self.dir = self.__TBEventStorageDir(data_folder_path, max_file_count)
        self.__init_reader_state(data_folder_path, read_interval, max_read_record_count)
        self.__writer = self.__TBEventStorageWriterState(self.dir, max_records_per_file, max_records_between_fsync)
        scheduler.add_job(self.read, 'interval', seconds=read_interval, next_run_time=datetime.now())
        self.__gateway = gateway

    def __init_reader_state(self, data_folder_path, read_interval, max_read_record_count):
        self.__reader_state_file_name = data_folder_path + ".reader_state"
        if os.path.isfile(self.__reader_state_file_name):
            with open(self.__reader_state_file_name, 'r') as state_file:
                data = load(state_file)
                self.__reader_state = self.__TBEventStorageReaderState(data['current_file_name'],
                                                                       data['current_file_pos'],
                                                                       read_interval,
                                                                       max_read_record_count,
                                                                       self)
        else:
            self.__reader_state = self.__TBEventStorageReaderState(self.dir.current_file_name(),
                                                                   0,
                                                                   read_interval,
                                                                   max_read_record_count,
                                                                   self)
        log.info("Reader state: %s", self.__reader_state)

    def write(self, data):
        if self.writer_lock.acquire(True, 60):
            try:
                if self.__writer.is_full():
                    log.debug("Writer is full: %s", self.__writer)
                    self.__writer.switch_to_new_file()
                    self.dir.cleanup()
                self.__writer.write(data)
            finally:
                self.writer_lock.release()
        else:
            log.warning("Failed to acquire write lock for an event storage!")

    def read(self):
        result = (self.__reader_state.read())
        for event in result:
            event = loads(event)
            device = event["device"]
            if event["eventType"] == "TELEMETRY":
                self.__gateway.mqtt_gateway.gw_send_telemetry(device, event["data"])
            else:
                self.__gateway.mqtt_gateway.gw_send_attributes(device, event["data"]["values"])
