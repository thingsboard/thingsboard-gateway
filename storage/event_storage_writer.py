from storage.event_storage_files import EventStorageFiles
from storage.file_event_storage_settings import FileEventStorageSettings
import logging
import time
import io
import os
import base64

log = logging.getLogger(__name__)


class EventStorageWriter:
    def __init__(self, files: EventStorageFiles, settings: FileEventStorageSettings):
        self.files = files
        self.settings = settings
        self.buffered_writer = None
        self.new_record_after_flush = None
        self.current_file = sorted(files.get_data_files())[-1]
        self.current_files_records_count = 0
        self.get_number_of_records_in_file(self.current_file)

    def write(self, msg, callback=None):
        self.new_record_after_flush = True
        if self.is_file_full(self.current_files_records_count):
            if log.getEffectiveLevel() == 10:
                log.debug("File [{}] is full with [{}] records".format(self.current_file,
                                                                       self.current_files_records_count))
            try:
                log.debug("Created new data file: {}".format(self.current_file))
                self.current_file = self.create_datafile()
            except IOError as e:
                log.error("Failed to create a new file!", e)
                # TODO implement callback
                if callback is not None:
                    # callback.onError(e)
                    pass
            if len(self.files.get_data_files()) == self.settings.get_max_files_count():
                first_file = self.files.get_data_files()[0]
                if os.remove(os.path.abspath(first_file)):
                    self.files.get_data_files().pop(0)
                log.info("Cleanup old data file: {}!".format(first_file))
            self.files.get_data_files().append(self.current_file)
            self.current_files_records_count = 0
            try:
                if self.buffered_writer is not None:
                    self.buffered_writer.close()
            except IOError as e:
                log.warning("Failed to close buffered writer!", e)
                # TODO implement callback
                if callback is not None:
                    # callback.onError(e)
                    pass
            self.buffered_writer = None
        encoded = base64.b64encode(msg.encode("utf-8"))
        try:
            self.buffered_writer = self.get_or_init_buffered_writer(self.current_file)
            self.buffered_writer.write(encoded)
            self.buffered_writer.write(os.linesep.encode('utf-8'))
            log.debug("Record written to: [{}:{}]".format(self.current_file, self.current_files_records_count))
            self.current_files_records_count += 1
            if self.current_files_records_count % self.settings.get_max_records_between_fsync() == 0:
                log.debug("Executing flush of the full pack!")
                self.buffered_writer.flush()
                self.new_record_after_flush = False
        except IOError as e:
            log.warning("Failed to update data file![{}]".format(self.current_file), e)
            if callback is not None:
                # callback.onError(e)
                pass
        if callback is not None:
            # callback.onSuccess(e)
            pass

    def flush_if_needed(self):
        if self.new_record_after_flush:
            if self.buffered_writer is not None:
                try:
                    log.debug("Executing flush of the temporary pack!")
                    self.buffered_writer.flush()
                    self.new_record_after_flush = False
                except IOError as e:
                    log.warning("Failed to update data file! [{}]".format(self.current_file), e)

    def get_or_init_buffered_writer(self, file):
        try:
            if self.buffered_writer is None:
                buffered_writer = io.BufferedWriter(io.FileIO(self.settings.get_data_folder_path() + file, 'a'))
                return buffered_writer
        except IOError as e:
            log.error("Failed to initialize buffered writer!", e)
            raise RuntimeError("Failed to initialize buffered writer!", e)

    def create_datafile(self):
        return self.create_file('data_', str(round(time.time() * 1000)))

    def create_file(self, prefix, filename):
        file_path = self.settings.get_data_folder_path() + prefix + filename + '.txt'
        try:
            file = open(file_path, 'w')
            file.close()
            return prefix + filename + '.txt'
        except IOError as e:
            log.error("Failed to create a new file!", e)

    def get_number_of_records_in_file(self, file):
        if self.current_files_records_count == 0:
            try:
                with open(self.settings.get_data_folder_path() + file) as f:
                    for i, l in enumerate(f):
                        self.current_files_records_count = i + 1
            except IOError as e:
                log.warning("Could not get the records count from the file![%s] with error: %s", file, e)

    def is_file_full(self, current_file_size):
        return current_file_size >= self.settings.get_max_records_per_file()
