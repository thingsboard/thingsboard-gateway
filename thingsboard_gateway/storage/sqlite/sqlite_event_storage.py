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

from gc import collect
from sqlite3 import InterfaceError
from threading import Event
from time import sleep, time, monotonic

from thingsboard_gateway.storage.event_storage import EventStorage
from thingsboard_gateway.storage.sqlite.database import Database
from queue import Queue, Full
from logging import getLogger


class SQLiteEventStorage(EventStorage):
    """
    HIGH level api for thingsboard_gateway main loop
    """

    def __init__(self, config, logger, main_stop_event):
        super().__init__(config, logger, main_stop_event)
        self.__log = logger
        self.__log.info("Sqlite Storage initializing...")
        self.write_queue = Queue(-1)
        self.stopped = Event()
        self.db = Database(config, self.write_queue, self.__log, stopped=self.stopped)
        self.db.init_table()
        self.__log.info("Sqlite storage initialized!")
        self.delete_time_point = None
        self.__event_pack_processing_start = monotonic()
        self.last_read = time()

    def get_event_pack(self):
        if not self.stopped.is_set():
            self.__event_pack_processing_start = monotonic()
            event_pack_messages = []
            try:
                data_from_storage = self.read_data()
            except InterfaceError as e:
                self.__log.error("InterfaceError occurred while reading data from storage: %s", e)
                return []
            try:
                for row in data_from_storage:
                    event_pack_messages.append(row['message'])
                    if self.delete_time_point is None or self.delete_time_point < row['id']:
                        self.delete_time_point = row['id']
            except (ValueError, TypeError):
                return []
            except (InterfaceError, MemoryError):
                return []
            if len(event_pack_messages) > 0:
                self.__log.trace("Retrieved %r records from storage in %r milliseconds, left in storage: %r",
                                len(event_pack_messages),
                                int((monotonic() - self.__event_pack_processing_start) * 1000),
                                self.db.get_stored_messages_count())
            self.db.can_prepare_new_batch()
            return event_pack_messages
        else:
            return []

    def event_pack_processing_done(self):
        self.__log.trace("Batch processing done, processing time: %i milliseconds",
                        int((monotonic() - self.__event_pack_processing_start) * 1000))
        if not self.stopped.is_set():
            self.delete_data(self.delete_time_point)
        collect()

    def read_data(self):
        data = self.db.read_data()
        return data

    def delete_data(self, row_id):
        return self.db.delete_data(row_id=row_id)

    def put(self, message):
        try:
            if not self.stopped.is_set():
                self.__log.trace("Sending data to storage: %s", message)
                self.write_queue.put_nowait(message)
                return True
            else:
                return False
        except Full:
            self.__log.error("Storage queue is full! Failed to send data to storage!")
            return False
        except Exception as e:
            self.__log.exception("Failed to write data to storage! Error: %s", e)

    def stop(self):
        self.stopped.set()
        while not self.db.stopped_flag.is_set() and not self._main_stop_event.is_set():
            self.db.interrupt()
            sleep(.1)
        collect()

    def len(self):
        qsize = self.write_queue.qsize()
        stored_messages = self.db.get_stored_messages_count()
        return qsize + stored_messages

    def update_logger(self):
        self.__log = getLogger("storage")
