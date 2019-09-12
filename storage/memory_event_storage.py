from storage.event_storage import EventStorage
import queue

class MemoryEventStorage(EventStorage):
    def __init__(self, queue_len, events_per_time):
        self.__events_per_time = events_per_time
        self.__events_queue = queue.LifoQueue(queue_len)
        self.__event_pack = []

    def put(self,event):
        if not self.__events_queue.full():
            self.__events_queue.put(event)
            return True
        else:
            return False

    def get_event_pack(self):
        if not self.__events_queue.empty() and not self.__event_pack:
            current_qsize = self.__events_queue.qsize()
            if current_qsize >= self.__events_per_time:
                for _ in range(self.__events_per_time):
                    self.__event_pack.append(self.__events_queue.get())
            else:
                for _ in range(current_qsize):
                    self.__event_pack.append(self.__events_queue.get())
        return self.__event_pack

    def event_pack_processing_done(self):
        self.__event_pack = []