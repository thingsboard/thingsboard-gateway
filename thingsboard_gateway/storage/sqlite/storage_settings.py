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

class StorageSettings:
    def __init__(self, config):
        self.data_folder_path = config.get("data_file_path", "./")
        self.messages_ttl_check_in_hours = config.get('messages_ttl_check_in_hours', 1) * 3600
        self.messages_ttl_in_days = config.get('messages_ttl_in_days', 7)
