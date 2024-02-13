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

import os

from regex import compile

from thingsboard_gateway.connectors.ftp.file import File

COMPATIBLE_FILE_EXTENSIONS = ('json', 'txt', 'csv')


class Path:
    def __init__(self, path: str, delimiter: str, telemetry: list, device_name: str, attributes: list,
                 txt_file_data_view: str, poll_period=60, with_sorting_files=True, device_type='Device', max_size=5,
                 read_mode='FULL'):
        self._path = path
        self._with_sorting_files = with_sorting_files
        self._poll_period = poll_period
        self._files: [File] = []
        self._delimiter = delimiter
        self._last_polled_time = 0
        self._telemetry = telemetry
        self._attributes = attributes
        self._device_name = device_name
        self._device_type = device_type
        self._txt_file_data_view = txt_file_data_view
        self.__read_mode = File.ReadMode[read_mode]
        self.__max_size = max_size

    @staticmethod
    def __is_file(ftp, filename):
        current = ftp.pwd()
        try:
            ftp.cwd(filename)
        except Exception:
            ftp.cwd(current)
            return True
        ftp.cwd(current)
        return False

    def __get_files(self, ftp, paths, file_name, file_ext):
        kwargs = {}
        pattern = compile(file_name.replace('*', '.*'))
        for item in paths:
            ftp.cwd(item)

            folder_and_files = ftp.nlst()

            for ff in folder_and_files:
                cur_file_name, cur_file_ext = ff.split('.')
                if cur_file_ext in COMPATIBLE_FILE_EXTENSIONS and self.__is_file(ftp, ff) and ftp.size(ff):
                    if (file_name == file_ext == '*') \
                            or pattern.fullmatch(cur_file_name) \
                            or (cur_file_ext == file_ext and file_name == cur_file_name) \
                            or (file_name != '*' and cur_file_name == file_name and (
                            file_ext == cur_file_ext or file_ext == '*')):
                        kwargs[ftp.voidcmd(f"MDTM {ff}")] = (item + '/' + ff)

        if self._with_sorting_files:
            return [File(path_to_file=val, read_mode=self.__read_mode, max_size=self.__max_size) for (_, val) in
                    sorted(kwargs.items(), reverse=True)]

        return [File(path_to_file=val, read_mode=self.__read_mode, max_size=self.__max_size) for val in kwargs.values()]

    def find_files(self, ftp):
        final_arr = []
        current_dir = ftp.pwd()

        dirname, basename = os.path.split(self._path)
        filename, fileex = basename.split('.')

        for (index, item) in enumerate(dirname.split('/')):
            if item == '*':
                current = ftp.pwd()
                arr = []
                for x in final_arr:
                    ftp.cwd(x)
                    node_paths = ftp.nlst()

                    for node in node_paths:
                        if not self.__is_file(ftp, node):
                            arr.append(ftp.pwd() + node)
                    final_arr = arr
                    ftp.cwd(current)
            else:
                if len(final_arr) > 0:
                    current = ftp.pwd()
                    for (j, k) in enumerate(final_arr):
                        ftp.cwd(k)
                        if not self.__is_file(ftp, item):
                            final_arr[j] = str(final_arr[j]) + '/' + item
                        else:
                            final_arr = []
                        ftp.cwd(current)
                else:
                    if not self.__is_file(ftp, item):
                        final_arr.append(item)

        final_arr = self.__get_files(ftp, final_arr, filename, fileex)

        ftp.cwd(current_dir)

        self._files = final_arr

    @property
    def config(self):
        return {
            'delimiter': self.delimiter,
            'devicePatternName': self.device_name,
            'devicePatternType': self.device_type,
            'timeseries': self.telemetry,
            'attributes': self.attributes,
            'txt_file_data_view': self.txt_file_data_view
            }

    @property
    def files(self):
        return self._files

    @property
    def delimiter(self):
        return self._delimiter

    @property
    def telemetry(self):
        return self._telemetry

    @property
    def device_name(self):
        return self._device_name

    @property
    def device_type(self):
        return self._device_type

    @property
    def attributes(self):
        return self._attributes

    @property
    def txt_file_data_view(self):
        return self._txt_file_data_view

    @property
    def last_polled_time(self):
        return self._last_polled_time

    @property
    def path(self):
        return self._path

    @property
    def poll_period(self):
        return self._poll_period

    @last_polled_time.setter
    def last_polled_time(self, value):
        self._last_polled_time = value
