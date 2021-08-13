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

from enum import Enum
from zlib import crc32


class File:
    class ReadMode(Enum):
        FULL = 'FULL'
        PARTIAL = 'PARTIAL'

    def __init__(self, path_to_file: str, read_mode: ReadMode, max_size: int):
        self._path_to_file = path_to_file
        self._read_mode = read_mode
        self._max_size = max_size
        self._hash = None
        self._cursor = None

    def __str__(self):
        return f'{self._path_to_file} {self._read_mode}'

    @property
    def path_to_file(self):
        return self._path_to_file

    @property
    def hash(self):
        return self._hash

    @property
    def read_mode(self):
        return self._read_mode

    @property
    def cursor(self):
        return self._cursor

    @cursor.setter
    def cursor(self, val):
        self._cursor = val

    def has_hash(self):
        return True if self._hash else False

    def get_current_hash(self, ftp):
        return crc32((ftp.voidcmd(f'MDTM {self._path_to_file}') + str(ftp.size(self.path_to_file))).encode('utf-8'))

    def set_new_hash(self, file_hash):
        self._hash = file_hash

    @staticmethod
    def convert_bytes_to_mb(bts):
        r = float(bts)
        for _ in range(2):
            r = r / 1024
        return round(r, 2)

    def check_size_limit(self, ftp):
        return self.convert_bytes_to_mb(ftp.size(self.path_to_file)) < self._max_size
