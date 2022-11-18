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


class FileEventStorageSettings:
    def __init__(self, config):
        self.data_folder_path = config.get("data_folder_path", "./")
        self.max_files_count = config.get("max_file_count", 5)
        self.max_records_per_file = config.get("max_records_per_file", 3)
        self.max_records_between_fsync = config.get("max_records_between_fsync", 1)
        self.max_read_records_count = config.get("max_read_records_count", 1000)

    def get_data_folder_path(self):
        return self.data_folder_path

    def get_max_files_count(self):
        return self.max_files_count

    def get_max_records_per_file(self):
        return self.max_records_per_file

    def get_max_records_between_fsync(self):
        return self.max_records_between_fsync

    def get_max_read_records_count(self):
        return self.max_read_records_count
