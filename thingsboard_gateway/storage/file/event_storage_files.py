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


class EventStorageFiles:
    def __init__(self, state_file, data_files):
        self.state_file = state_file
        self.data_files = data_files

    def get_state_file(self):
        return self.state_file

    def get_data_files(self):
        return sorted(self.data_files)

    def set_data_files(self, data_files):
        self.data_files = data_files
