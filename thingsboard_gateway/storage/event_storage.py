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

from abc import ABC, abstractmethod
from logging import getLogger

log = getLogger("storage")


class EventStorage(ABC):

    @abstractmethod
    def put(self, event):
        pass

    @abstractmethod
    def get_event_pack(self):
        # Returns max "10" events from pack
        pass

    @abstractmethod
    def event_pack_processing_done(self):
        # Indicates that events from previous "get_event_pack" may be cleared
        pass

    @abstractmethod
    def stop(self):
        # Stop the storage processing
        pass

    @abstractmethod
    def len(self):
        pass
