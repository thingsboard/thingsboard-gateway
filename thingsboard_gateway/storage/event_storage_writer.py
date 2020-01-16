#     Copyright 2020. ThingsBoard
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

from thingsboard_gateway.storage.file_event_storage import log
from thingsboard_gateway.storage.event_storage_files import EventStorageFiles
from thingsboard_gateway.storage.file_event_storage_settings import FileEventStorageSettings
from time import time
from io import BufferedWriter, FileIO
from os import linesep, open as os_open, O_CREAT, O_EXCL
from base64 import b64encode


class EventStorageWriter:
    def __init__(self, files: EventStorageFiles, settings: FileEventStorageSettings):
        self.files = files
        self.settings = settings
        self.buffered_writer = None
        self.current_file = sorted(files.get_data_files())[-1]
        self.current_file_records_count = [0]
        self.previous_file_records_count = [0]
        self.get_number_of_records_in_file(self.current_file)

    def write(self, msg):
        if self.current_file_records_count[0] >= self.settings.get_max_records_per_file():
            try:
                self.current_file = self.create_datafile()
                log.debug("FileStorage_writer -- Created new data file: %s", self.current_file)
            except IOError as e:
                log.error("Failed to create a new file! %s", e)
            self.files.get_data_files().append(self.current_file)
            self.current_file_records_count[0] = 0
            try:
                if self.buffered_writer is not None and self.buffered_writer.closed is False:
                    self.buffered_writer.close()
            except IOError as e:
                log.warning("Failed to close buffered writer! %s", e)
            self.buffered_writer = None
        try:
            encoded = b64encode(msg.encode("utf-8"))
            self.buffered_writer = self.get_or_init_buffered_writer(self.current_file)
            self.buffered_writer.write(encoded + linesep.encode('utf-8'))
            # self.buffered_writer.write(linesep.encode('utf-8'))
            self.current_file_records_count[0] += 1
            if self.current_file_records_count[0] - self.previous_file_records_count[0] >= self.settings.get_max_records_between_fsync():
                self.previous_file_records_count = self.current_file_records_count[:]
                self.buffered_writer.flush()
        except IOError as e:
            log.warning("Failed to update data file![%s]\n%s", self.current_file, e)

    def get_or_init_buffered_writer(self, file):
        try:
            if self.buffered_writer is None or self.buffered_writer.closed:
                buffered_writer = BufferedWriter(FileIO(self.settings.get_data_folder_path() + file, 'a'))
                return buffered_writer
            else:
                return self.buffered_writer
        except IOError as e:
            log.error("Failed to initialize buffered writer!", e)
            raise RuntimeError("Failed to initialize buffered writer!", e)

    def create_datafile(self):
        prefix = 'data_'
        datafile_name = str(int(time() * 1000))
        self.files.data_files.append("%s%s.txt" % (prefix, datafile_name))
        return self.create_file(prefix, datafile_name)

    def create_file(self, prefix, filename):
        full_file_name = "%s%s.txt" % (prefix, filename)
        file_path = "%s%s" % (self.settings.get_data_folder_path(), full_file_name)
        try:
            os_open(file_path, O_CREAT | O_EXCL)
            return full_file_name
        except IOError as e:
            log.error("Failed to create a new file!", e)

    def get_number_of_records_in_file(self, file):
        if self.current_file_records_count[0] <= 0:
            try:
                with open(self.settings.get_data_folder_path() + file) as f:
                    for i, _ in enumerate(f):
                        self.current_file_records_count[0] = i + 1
            except IOError as e:
                log.warning("Could not get the records count from the file![%s] with error: %s", file, e)
            except Exception as e:
                log.exception(e)
        return self.current_file_records_count
