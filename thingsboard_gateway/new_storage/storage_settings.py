#     Copyright 2021. ThingsBoard
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


class StorageSettings:
    def __init__(self, config):
        self.data_folder_path = config.get("data_file_path", "./")
        self.max_days_to_store_data = config.get("max_days_to_store_data", 14)
    def get_data_file_path(self):
        return self.data_folder_path

    def get_max_days_to_store_data(self):
        return self.get_max_days_to_store_data

