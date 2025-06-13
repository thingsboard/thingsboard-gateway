#      You may obtain a copy of the License at
#  #
#          http://www.apache.org/licenses/LICENSE-2.0
#  #
#      Unless required by applicable law or agreed to in writing, software
#      distributed under the License is distributed on an "AS IS" BASIS,
#      WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#      See the License for the specific language governing permissions and
#      limitations under the License.
#
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


from abc import ABC, abstractmethod

from thingsboard_gateway.storage.sqlite.sqlite_event_storage_pointer import Pointer


class OnInitDatabaseStrategy(ABC):
    @abstractmethod
    def initial_read_files(self):
        pass

    @abstractmethod
    def initial_write_files(self):
        pass


class PointerInitStrategy(OnInitDatabaseStrategy):

    def __init__(self,
        data_folder_path: str,
        default_database_name: str,
        log,
        state_file_name: str = "state.txt",
    ):
        self.pointer = Pointer(data_folder_path, log, state_file_name)
        self.default_database_name = default_database_name

    def check_should_create_or_assign_database_on_gateway_init(self):
        all_db_files = self.pointer.sort_db_files()
        if all_db_files and self.default_database_name < all_db_files[0]:
            return all_db_files[0]
        return self.default_database_name

    def initial_read_files(self):
        return self.pointer.read_database_file

    def initial_write_files(self):
        return self.pointer.write_database_file

    def update_read_database_file(self, new_filename: str) -> None:
        self.pointer.update_read_database_filename(new_filename)

    def update_write_database_file(self, new_filename: str) -> None:
        self.pointer.update_write_database_file(new_filename)

    def generate_new_file_name(self) -> str:
        return self.pointer.generate_new_file_name()
