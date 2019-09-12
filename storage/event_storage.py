import abc

class EventStorage():

    @abc.abstractmethod
    def put(self, event):
        pass

    @abc.abstractmethod
    def get_event_pack(self):
        # Returns max "10" events from pack
        pass

    @abc.abstractmethod
    def event_pack_processing_done(self):
        # Indicates that events from previous "get_event_pack" may be cleared
        pass
