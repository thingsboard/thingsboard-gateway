#     Copyright 2024. ThingsBoard
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

from threading import RLock
from typing import Dict


class EventStorageFiles:
    def __init__(self, state_file, data_files: Dict[str, bool]):
        self.state_file = state_file
        self.data_files = data_files
        self.__data_files_lock = RLock()

    def get_state_file(self):
        return self.state_file

    def get_data_files(self):
        with self.__data_files_lock:
            return sorted(self.data_files.keys())

    def add_data_file(self, data_file):
        with self.__data_files_lock:
            self.data_files[data_file] = False

    def remove_data_file(self, data_file):
        with self.__data_files_lock:
            del self.data_files[data_file]

    def confirm_file_processed(self, data_file):
        with self.__data_files_lock:
            self.data_files[data_file] = True
