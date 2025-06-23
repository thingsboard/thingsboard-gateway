#     Copyright 2025. ThingsBoard
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

from os import path


class StorageSettings:
    def __init__(self, config):
        self.data_file_path = config.get("data_file_path", "./")
        self.messages_ttl_check_in_hours = (
                config.get("messages_ttl_check_in_hours", 1) * 3600
        )
        self.messages_ttl_in_days = config.get("messages_ttl_in_days", 7)
        self.max_read_records_count = config.get("max_read_records_count", 100000)
        self.batch_size = config.get("writing_batch_size", 1000)
        self.directory_path = path.dirname(self.data_file_path)
        self.db_file_name = path.basename(self.data_file_path)
        self.size_limit = config.get("size_limit", 1)
        self.max_db_amount = config.get("max_db_amount", 10)
        self.oversize_check_period = config.get("oversize_check_period", 1)
        self.validate_settings()

    def validate_settings(self):
        if not self.db_file_name:
            self.db_file_name = "data.db"
