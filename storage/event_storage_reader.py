from storage.event_storage_files import EventStorageFiles
from storage.file_event_storage_settings import FileEventStorageSettings
import copy
import logging
import time
import io
import os
import base64


class EventStorageReader:
    def __init__(self, files: EventStorageFiles, settings: FileEventStorageSettings):
        self.files = files
        self.settings = settings
        self.current_pos = '' # TODO read_state_file()
        self.new_pos = copy.deepcopy(self.current_pos)

    def read(self):
        pass

    def discard_batch(self):
        pass

    def get_next_file(self, files, new_pos):
        pass

    def get_or_init_buffered_reader(self, pointer):
        pass

    def read_state_file(self):
        pass

    def write_info_to_state_file(self, pointer):
        pass

    def destroy(self):
        pass

