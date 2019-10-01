from storage.event_storage import EventStorage
from storage.event_storage_files import EventStorageFiles
from storage.file_event_storage_settings import FileEventStorageSettings
import os
import time
import logging

log = logging.getLogger(__name__)


class FileEventStorage(EventStorage):
    def __init__(self):
        self.settings = FileEventStorageSettings()
        self.init_data_folder_if_not_exist()
        self.event_storage_files = self.init_data_files()
        self.data_files = self.event_storage_files.get_data_files()
        self.state_file = self.event_storage_files.get_state_file()

    def put(self, event):
        pass

    def get_event_pack(self):
        pass

    def event_pack_processing_done(self):
        pass

    def init_data_folder_if_not_exist(self):
        path = self.settings.get_data_folder_path()
        if not os.path.exists(path):
            try:
                os.makedirs(path)
            except OSError as e:
                log.error('Failed to create data folder!', e)

    def init_data_files(self):
        data_files = []
        state_file = None
        data_files_size = 0
        _dir = self.settings.get_data_folder_path()
        if os.path.isdir(_dir):
            for file in os.listdir(_dir):
                if file.startswith('data_'):
                    data_files.append(file)
                    data_files_size += os.path.getsize(_dir + file)
                elif file.startswith('state_'):
                    state_file = file
            if data_files_size == 0:
                data_files.append(self.create_new_datafile())
            if not state_file:
                state_file = self.create_file('state_', 'file')
            return EventStorageFiles(state_file, data_files)

    def create_new_datafile(self):
        return self.create_file('data_', str(round(time.time() * 1000)))

    def create_file(self, prefix, filename):
        file_path = self.settings.get_data_folder_path() + prefix + filename + '.txt'
        try:
            file = open(file_path, 'w')
            file.close()
            return prefix + filename + '.txt'
        except IOError as e:
            log.error("Failed to create a new file!", e)
            pass
