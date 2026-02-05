#     Copyright 2026. ThingsBoard
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

import time
from os import path, listdir


class Pointer:
    def __init__(self, path_to_folder, log):
        self.__log = log
        self.__directory = path.dirname(path_to_folder)

    def sort_db_files(self):
        all_files = listdir(self.__directory)
        all_db_files = sorted(filter(lambda file: file.endswith(".db"), all_files))
        return all_db_files

    @staticmethod
    def generate_new_file_name():
        prefix = "data_"
        ts = str(int(time.time()) * 1000)
        db_file_name = f"{prefix}{ts}.db"
        return db_file_name
