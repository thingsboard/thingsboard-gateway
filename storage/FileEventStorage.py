import os
import time
from storage.event_storage import EventStorage
from storage.FileEventStorageSettings import file_event_storage_settings as settings
from storage.EventStorageReader import EventStorageReader

import logging

log = logging.getLogger(__name__)


class FileEventStorage(EventStorage):

    def __init__(self):
        self.storage_reader = EventStorageReader()
        self.storage_writer = None # TODO implement writer
        self.init_data_folder_if_not_exist()

    @staticmethod
    def init_data_folder_if_not_exist():
        path = settings.data_folder_path
        if not os.path.exists(path):
            try:
                os.makedirs(path)
            except OSError as e:
                log.error("Failed to create data folder!", e)
                pass

    def init_data_files(self):
        data_files = []
        data_files_size = 0
        state_file = None
        data_dir = settings.data_folder_path
        if os.path.isdir(data_dir):
            for file in os.listdir(data_dir):
                if file.startswith('data_'):
                    data_files.append(file)
                    data_files_size += os.path.getsize(data_dir + file)
                elif file.startswith('state_'):
                    state_file = file
            if data_files_size == 0:
                data_files.append(self.create_new_datafile())
            if not state_file:
                state_file = self.create_file('/state_', 'file')
            files = {'state_file': state_file, 'data_files': data_files}
            return files
        else:
            log.error("{} The specified path is not referred to the directory!".format(settings.data_folder_path))
            pass

    def create_new_datafile(self):
        return self.create_file('/data_', (str(round(time.time() * 1000))))

    @staticmethod
    def create_file(prefix, filename):
        file_path = settings.data_folder_path + prefix + filename + '.txt'
        try:
            file = open(file_path, 'w')
            file.close()
            return file_path
        except IOError as e:
            log.error("Failed to create a new file!", e)
            pass

    # TODO callback?
    def write(self, msg, callback):
        # TODO add writelock
        try:
            self.storage_writer.write(msg, callback)
        finally:
            # TODO add write unlock
            pass

    def read_current_batch(self):
        # TODO add write lock
        try:
            self.storage_writer.flush_if_needed()
        finally:
            # TODO add write unlock
            pass

        # TODO add read lock
        try:
            self.storage_reader.read()
        finally:
            # TODO add read unlock
            pass

    def discard_current_batch(self):
        # TODO add read lock
        try:
            self.storage_reader.discard_batch()
        finally:
            # TODO add read unlock
            pass

    @staticmethod
    def sleep(self):
        try:
            time.sleep(settings.no_records_sleep_interval)
        except InterruptedError as e:
            log.warn("Failed to sleep a bit", e)
