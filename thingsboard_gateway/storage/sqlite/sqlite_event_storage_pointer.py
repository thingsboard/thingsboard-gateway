#      Copyright 2025. ThingsBoard
#  #
#      Licensed under the Apache License, Version 2.0 (the "License");
#      you may not use this file except in compliance with the License.
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


from os import path, listdir
import time

from simplejson import JSONDecodeError, dumps, load
from threading import Lock


class Pointer:
    def __init__(self, path_to_folder, log, state_file_name="state.txt"):
        self.__path_to_folder = path_to_folder
        self.__log = log
        self.__state_file_lock = Lock()
        self.__directory = path.dirname(path_to_folder)

        self.__read_database_file = path.basename(self.__path_to_folder)
        self.__write_database_file = path.basename(self.__path_to_folder)
        self.__state_file_name = state_file_name
        self.__full_path_to_state_file = path.join(
            self.__directory, self.__state_file_name
        )
        if path.exists(self.__full_path_to_state_file):
            self.read_and_apply_state_file()
        else:
            self.create_state_file()

    def create_state_file(self):
        all_db_files = self.sort_db_files()
        if len(all_db_files) == 0 or len(all_db_files) == 1:
            with self.__state_file_lock:
                self.__write_info_to_state_file()
        else:
            self.update_read_database_filename(all_db_files[0])
            self.update_write_database_file(all_db_files[-1])

    def sort_db_files(self):
        all_files = listdir(self.__directory)
        all_db_files = sorted(filter(lambda file: file.endswith(".db"), all_files))
        return all_db_files

    def determine_read_and_write__db_file(self):
        all_db_files = self.sort_db_files()
        if len(all_db_files) > 1:
            read_file = all_db_files[0]
            write_file = all_db_files[-1]
            return read_file, write_file
        return all_db_files[0]

    def update_read_database_filename(self, database_file):

        with self.__state_file_lock:
            self.__read_database_file = database_file
            self.__write_info_to_state_file()

    def update_write_database_file(self, database_file):
        with self.__state_file_lock:
            self.__write_database_file = database_file
            self.__write_info_to_state_file()

    def __write_info_to_state_file(self):
        try:
            state_node = {
                "read_database_file": self.__read_database_file,
                "write_database_file": self.__write_database_file,
            }
            with open(self.__full_path_to_state_file, "w") as state_file:
                state_file.write(dumps(state_node))
        except IOError as e:
            self.__log.warning("Failed to update state file! Error: %s", e)
            self.__log.debug("Failed to update state file! Error:", exc_info=e)
        except Exception as e:
            self.__log.exception("Failed to update state file! Error: %s", e)
            self.__log.debug("Failed to update state file! Error:", exc_info=e)

    def generate_new_file_name(self):
        prefix = "data_"
        ts = str(int(time.time()) * 1000)
        db_file_name = f"{prefix}{ts}.db"
        return db_file_name

    def read_and_apply_state_file(self):
        new_file_creation_required = False
        with self.__state_file_lock:
            try:
                with open(self.__full_path_to_state_file, "r") as file:
                    state_data_node = load(file)

                if state_data_node:
                    self.__write_database_file = state_data_node["write_database_file"]
                    self.__read_database_file = state_data_node["read_database_file"]
                else:
                    new_file_creation_required = True

            except KeyError:
                self.__log.warning(
                    "Failed to read existing state file, will create a new one!"
                )
                new_file_creation_required = True
            except JSONDecodeError as e:
                self.__log.error("Failed to decode JSON from state file! Error: %s", e)
                self.__log.debug(
                    "Failed to decode JSON from state file! Error:", exc_info=e
                )

            except IOError as e:
                self.__log.warning("Failed to fetch info from state file! Error: %s", e)
                self.__log.debug(
                    "Failed to fetch info from state file! Error: %s", exc_info=e
                )
        if new_file_creation_required:
            self.create_state_file()

    @property
    def read_database_file(self):
        return self.__read_database_file

    @property
    def write_database_file(self):
        return self.__write_database_file
