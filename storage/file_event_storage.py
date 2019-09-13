from storage.event_storage import EventStorage
import queue


class FileEventStorage(EventStorage):
    def put(self, event):
        pass

    def get_event_pack(self):
        # Returns max "10" events from pack
        pass

    def event_pack_processing_done(self):
        # Indicates that events from previous "get_event_pack" may be cleared
        pass
