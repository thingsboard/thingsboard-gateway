from storage.event_storage_files import EventStorageFiles
from storage.file_event_storage_settings import FileEventStorageSettings
import logging
import time

log = logging.getLogger(__name__)


class EventStorageWriter:
    def __init__(self, files: EventStorageFiles, settings: FileEventStorageSettings):
        self.files = files
        self.settings = settings
        self.current_file = sorted(files.get_data_files())[-1]
        self.current_files_records_count = self.get_number_of_records_in_file(self.current_file)

    def write(self):
        pass

    def flush_if_needed(self):
        pass

    def get_or_init_buffered_writer(self):
        pass

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

    def get_number_of_records_in_file(self, file):
        if self.current_files_records_count == 0:
            try:
                with open(file) as f:
                    for i, l in enumerate(f):
                        pass
                self.current_files_records_count = i + 1
            except IOError as e:
                log.warning("Could not get the records count from the file!{}".format(file))
            finally:
                return self.current_files_records_count

    def is_file_full(self, current_file_size):
        return current_file_size >= self.settings.get_max_records_per_file()
