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
import sqlite3
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


class StorageHandler(EventStorage):
    """
    HIGH level api for thingsboard_gateway main loop
    """

    def __init__(self, config):
        log.info("Sqlite Storage initializing...")
        log.info("Initializing read and process queues")
        self.processQueue = Queue(-1)
        self.readQueue = Queue(-1)

        self.db = Database(config, self.processQueue)

        self.db.setReadQueue(self.readQueue)
        self.db.setProcessQueue(self.processQueue)

        self.db.init_table()
        log.info("Sqlite storage initialized!")
        self.delete_time_point = None
        self.last_read = time()
        self.closed = False

    def get_event_pack(self):
        self.delete_time_point = self.last_read
        data_from_storage = self.read_data(self.last_read)
        self.last_read = time()

        return [item[0] for item in data_from_storage or []]

    def event_pack_processing_done(self):
        self.delete_data(self.delete_time_point)

    def read_data(self, ts):
        return self.db.read_data(ts)

    def delete_data(self, ts):
        return self.db.delete_data(ts)

    def put(self, message):
        try:
            _type = DatabaseActionType.WRITE_DATA_STORAGE
            request = DatabaseRequest(_type, message)

            log.info("Sending data to storage")
            self.processQueue.put(request)
            self.db.process()
            return True
        except Exception as e:
            log.exception(e)

    def close_db(self):
        self.db.closeDB()
        self.closed = True
