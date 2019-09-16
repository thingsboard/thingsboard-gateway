from storage.event_storage import EventStorage
from tb_utility.tb_utility import TBUtility
import queue


class FileEventStorage(EventStorage):
    def __init__(self,config):
        self.__queue_len = TBUtility.get_parameter(config, "max_records_count", 100)
        self.__events_per_time = TBUtility.get_parameter(config, "read_records_count", 10)
        self.__records_per_file = TBUtility.get_parameter(config, "records_per_file", 50)
        self.__no_records_sleep_interval = TBUtility.get_parameter(config, "no_records_sleep_interval", 60)
        self.__events_queue = queue.Queue(self.__queue_len)
        self.__event_pack = []

    def put(self, event):
        if not self.__events_queue.full():
            self.__events_queue.put(event)
            return True
        else:
            return False

    def get_event_pack(self):
        if not self.__events_queue.empty() and not self.__event_pack:
            for _ in range(min(self.__events_per_time, self.__events_queue.qsize())):
                self.__event_pack.append(self.__events_queue.get())
        return self.__event_pack

    def event_pack_processing_done(self):
        self.__event_pack = []

