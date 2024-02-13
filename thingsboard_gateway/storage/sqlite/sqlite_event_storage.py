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

from time import time

from thingsboard_gateway.storage.event_storage import EventStorage
from thingsboard_gateway.storage.sqlite.database import Database
from queue import Queue
from thingsboard_gateway.storage.sqlite.database_request import DatabaseRequest
from thingsboard_gateway.storage.sqlite.database_action_type import DatabaseActionType
#
#   No need to import DatabaseResponse, responses come to this component to be deconstructed
#
from logging import getLogger

log = getLogger("storage")


class SQLiteEventStorage(EventStorage):
    """
    HIGH level api for thingsboard_gateway main loop
    """

    def __init__(self, config):
        log.info("Sqlite Storage initializing...")
        log.info("Initializing read and process queues")
        self.processQueue = Queue(-1)
        self.db = Database(config, self.processQueue)
        self.db.setProcessQueue(self.processQueue)
        self.db.init_table()
        log.info("Sqlite storage initialized!")
        self.delete_time_point = None
        self.last_read = time()
        self.stopped = False

    def get_event_pack(self):
        if not self.stopped:
            data_from_storage = self.read_data()
            try:                
                event_pack_timestamps, event_pack_messages = zip(*([(item[0],item[1]) for item in data_from_storage]))
            except ValueError as e:
                return []
            self.delete_time_point = max(event_pack_timestamps)            
            return event_pack_messages
        else:
            return []

    def event_pack_processing_done(self):
        if not self.stopped:
            self.delete_data(self.delete_time_point)

    def read_data(self):
        self.db.__stopped = True
        data = self.db.read_data()
        self.db.__stopped = False
        return data

    def delete_data(self, ts):
        return self.db.delete_data(ts)

    def put(self, message):
        try:
            if not self.stopped:
                _type = DatabaseActionType.WRITE_DATA_STORAGE
                request = DatabaseRequest(_type, message)

                log.info("Sending data to storage")
                self.processQueue.put(request)
                return True
            else:
                return False
        except Exception as e:
            log.exception(e)

    def stop(self):
        self.stopped = True
        self.db.__stopped = True
        self.db.closeDB()

    def len(self):
        return self.processQueue.qsize()
