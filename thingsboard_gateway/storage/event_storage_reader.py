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

from io import BufferedReader, FileIO
from os import remove
from os.path import exists
from base64 import b64decode
from simplejson import load, dumps, JSONDecodeError
from thingsboard_gateway.storage.file_event_storage import log
from thingsboard_gateway.storage.event_storage_files import EventStorageFiles
from thingsboard_gateway.storage.file_event_storage_settings import FileEventStorageSettings
from thingsboard_gateway.storage.event_storage_reader_pointer import EventStorageReaderPointer


class EventStorageReader:
    def __init__(self, files: EventStorageFiles, settings: FileEventStorageSettings):
        self.files = files
        self.settings = settings
        self.current_batch = None
        self.buffered_reader = None
        self.current_pos = self.read_state_file()
        self.new_pos = self.current_pos

    def read(self):
        if self.current_batch is not None and self.current_batch:
            log.debug("The previous batch was not discarded!")
            return self.current_batch
        self.current_batch = []
        records_to_read = self.settings.get_max_read_records_count()
        while records_to_read > 0:
            try:
                current_line_in_file = self.new_pos.get_line()
                self.buffered_reader = self.get_or_init_buffered_reader(self.new_pos)
                line = self.buffered_reader.readline()
                while line != b'':
                    try:
                        self.current_batch.append(b64decode(line).decode("utf-8"))
                        records_to_read -= 1
                    except IOError as e:
                        log.warning("Could not parse line [%s] to uplink message! %s", line, e)
                    except Exception as e:
                        log.exception(e)
                        current_line_in_file += 1
                        self.new_pos.set_line(current_line_in_file)
                        self.write_info_to_state_file(self.new_pos)
                        break
                    finally:
                        current_line_in_file += 1
                        if records_to_read > 0:
                            line = self.buffered_reader.readline()
                    self.new_pos.set_line(current_line_in_file)
                    if records_to_read == 0:
                        break

                if current_line_in_file >= self.settings.get_max_records_per_file():
                    previous_file = self.new_pos
                    next_file = self.get_next_file(self.files, self.new_pos)
                    if next_file is not None:
                        if self.buffered_reader is not None:
                            self.buffered_reader.close()
                        self.buffered_reader = None
                        self.delete_read_file(previous_file)
                        self.new_pos = EventStorageReaderPointer(next_file, 0)
                        self.write_info_to_state_file(self.new_pos)
                        continue
                    else:
                        # No more records to read for now
                        break
                        # continue
                        ###################
                if line == b'':
                    break
                    #######################
                else:
                    # No more records to read for now
                    continue
            except IOError as e:
                log.warning("[{}] Failed to read file!".format(self.new_pos.get_file(), e))
                break
            except Exception as e:
                log.exception(e)
        return self.current_batch

    def discard_batch(self):
        try:
            if self.current_pos.get_line() >= self.settings.get_max_records_per_file()-1:
                if self.buffered_reader is not None and not self.buffered_reader.closed:
                    self.buffered_reader.close()
            self.write_info_to_state_file(self.new_pos)
            self.current_pos = self.new_pos
            self.current_batch = None
        except Exception as e:
            log.exception(e)

    def get_next_file(self, files: EventStorageFiles, new_pos: EventStorageReaderPointer):
        found = False
        data_files = files.get_data_files()
        target_file = None
        for file_index in range(len(data_files)):
            if found:
                target_file = data_files[file_index]
                break
            if data_files[file_index] == new_pos.get_file():
                found = True
        return target_file

    def get_or_init_buffered_reader(self, pointer):
        try:
            if self.buffered_reader is None or self.buffered_reader.closed:
                new_file_to_read_path = self.settings.get_data_folder_path() + pointer.get_file()
                self.buffered_reader = BufferedReader(FileIO(new_file_to_read_path, 'r'))
                lines_to_skip = pointer.get_line()
                if lines_to_skip > 0:
                    while self.buffered_reader.readline() is not None:
                        if lines_to_skip != 0:
                            lines_to_skip -= 1
                        else:
                            break
            return self.buffered_reader

        except IOError as e:
            log.error("Failed to initialize buffered reader!", e)
            raise RuntimeError("Failed to initialize buffered reader!", e)

    def read_state_file(self):
        try:
            state_data_node = {}
            try:
                with BufferedReader(FileIO(self.settings.get_data_folder_path() +
                                                 self.files.get_state_file(), 'r')) as br:
                    state_data_node = load(br)
            except JSONDecodeError:
                log.error("Failed to decode JSON from state file")
                state_data_node = 0
            except IOError as e:
                log.warning("Failed to fetch info from state file!", e)
            reader_file = None
            reader_pos = 0
            if state_data_node:
                reader_pos = state_data_node['position']
                for file in sorted(self.files.get_data_files()):
                    if file == state_data_node['file']:
                        reader_file = file
                        break
            if reader_file is None:
                reader_file = sorted(self.files.get_data_files())[0]
                reader_pos = 0
            log.info("FileStorage_reader -- Initializing from state file: [%s:%i]",
                     self.settings.get_data_folder_path() + reader_file,
                     reader_pos)
            return EventStorageReaderPointer(reader_file, reader_pos)
        except Exception as e:
            log.exception(e)

    def write_info_to_state_file(self, pointer: EventStorageReaderPointer):
        try:
            state_file_node = {'file': pointer.get_file(), 'position': pointer.get_line()}
            with open(self.settings.get_data_folder_path() + self.files.get_state_file(), 'w') as outfile:
                outfile.write(dumps(state_file_node))
        except IOError as e:
            log.warning("Failed to update state file!", e)
        except Exception as e:
            log.exception(e)

    def delete_read_file(self, current_file: EventStorageReaderPointer):
        data_files = self.files.get_data_files()
        if exists(self.settings.get_data_folder_path() + current_file.file):
            remove(self.settings.get_data_folder_path() + current_file.file)
            try:
                data_files = data_files[1:]
            except Exception as e:
                log.exception(e)
            log.info("FileStorage_reader -- Cleanup old data file: %s%s!", self.settings.get_data_folder_path(), current_file.file)

    def destroy(self):
        if self.buffered_reader is not None:
            self.buffered_reader.close()
            raise IOError
