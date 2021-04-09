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

from queue import Queue, Full, Empty

from thingsboard_gateway.storage.event_storage import EventStorage, log


class MemoryEventStorage(EventStorage):
    def __init__(self, config):
        self.__queue_len = config.get("max_records_count", 10000)
        self.__events_per_time = config.get("read_records_count", 1000)
        self.__events_queue = Queue(self.__queue_len)
        self.__event_pack = []
        log.debug("Memory storage created with following configuration: \nMax size: %i\n Read records per time: %i", self.__queue_len, self.__events_per_time)

    def put(self, event):
        success = False
        try:
            self.__events_queue.put(event)
            success = True
        except Full:
            log.error("Memory storage is full!")
        return success

    def get_event_pack(self):
        try:
            if not self.__event_pack:
                self.__event_pack = [self.__events_queue.get(False) for _ in range(min(self.__events_per_time, self.__events_queue.qsize()))]
        except Empty:
            pass
        return self.__event_pack

    def event_pack_processing_done(self):
        self.__event_pack = []


