
#     Copyright 2020. ThingsBoard
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


class EventStorageReaderPointer:
    def __init__(self, file, line):
        self.file = file
        self.line = line

    def __eq__(self, other):
        return self.file == other.file and self.line == other.line

    def __hash__(self):
        return hash((self.file, self.line))

    def get_file(self):
        return self.file

    def get_line(self):
        return self.line

    def set_file(self, file):
        self.file = file

    def set_line(self, line):
        self.line = line
