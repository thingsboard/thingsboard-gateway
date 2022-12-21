#     Copyright 2022. ThingsBoard
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

import os
import time

from simplejson import dump

from thingsboard_gateway.storage.event_storage import EventStorage, log
from thingsboard_gateway.storage.file.event_storage_files import EventStorageFiles
from thingsboard_gateway.storage.file.event_storage_reader import EventStorageReader
from thingsboard_gateway.storage.file.event_storage_writer import DataFileCountError, EventStorageWriter
from thingsboard_gateway.storage.file.file_event_storage_settings import FileEventStorageSettings


class FileEventStorage(EventStorage):
    def __init__(self, config):
        self.settings = FileEventStorageSettings(config)
        self.init_data_folder_if_not_exist()
        self.event_storage_files = self.init_data_files()
        self.data_files = self.event_storage_files.get_data_files()
        self.state_file = self.event_storage_files.get_state_file()
        self.__writer = EventStorageWriter(self.event_storage_files, self.settings)
        self.__reader = EventStorageReader(self.event_storage_files, self.settings)
        self.__stopped = False

    def put(self, event):
        success = False
        if not self.__stopped:
            try:
                self.__writer.write(event)
            except DataFileCountError as e:
                log.error(e)
            except Exception as e:
                log.exception(e)
            else:
                success = True
        else:
            log.error("Storage is closed!")
        return success

    def get_event_pack(self):
        return self.__reader.read()

    def event_pack_processing_done(self):
        self.__reader.discard_batch()

    def init_data_folder_if_not_exist(self):
        path = self.settings.get_data_folder_path()
        if not os.path.exists(path):
            try:
                os.makedirs(path)
            except OSError as e:
                log.error('Failed to create data folder! Error: %s', e)

    def init_data_files(self):
        data_files = []
        state_file = None
        data_files_size = 0
        _dir = self.settings.get_data_folder_path()
        event_storage_files = None
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
                with open(self.settings.get_data_folder_path() + state_file, 'w') as state_file_obj:
                    dump({"position": 0, "file": sorted(data_files)[0]}, state_file_obj)
            event_storage_files = EventStorageFiles(state_file, data_files)
        return event_storage_files

    def create_new_datafile(self):
        return self.create_file('data_', str(round(time.time() * 1000)))

    def create_file(self, prefix, filename):
        file_path = self.settings.get_data_folder_path() + prefix + filename + '.txt'
        try:
            file = open(file_path, 'w')
            file.close()
            return prefix + filename + '.txt'
        except IOError as e:
            log.error("Failed to create a new file! Error: %s", e)

    def stop(self):
        self.__stopped = True

    def len(self):
        return len(self.__writer.files.data_files)
