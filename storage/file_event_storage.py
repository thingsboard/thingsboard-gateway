from storage.event_storage import EventStorage
from tb_utility.tb_utility import TBUtility
from storage.file_event_storage_files import FileEventStorageFiles, FileEventStoragePointer
import yaml
import queue


class FileEventStorage(EventStorage):
    def __init__(self, config):
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

    def get_events_queue_size(self):
        return self.__events_queue.qsize()

    def get_event_pack(self):
        if not self.__events_queue.empty() and not self.__event_pack:
            for _ in range(min(self.__events_per_time, self.__events_queue.qsize())):
                self.__event_pack.append(self.__events_queue.get())
        return self.__event_pack

    def event_pack_processing_done(self):
        self.__event_pack = []

    def write_to_storage(self, config):
        data_files = FileEventStorageFiles(config)
        path = data_files.data_folder_path
        files = data_files.init_data_files(path)
        #print(files)
        current_position = yaml.safe_load(open(path + files['state_file']))
        #print(current_position)
        pointer = FileEventStoragePointer(current_position['write_file'], current_position['write_line'])
        #print(type(self.get_events_queue_size()))
        while self.get_events_queue_size():
            print(pointer.get_line())
            print(self.__records_per_file)
            while True:
                if pointer.get_line() < self.__records_per_file:
                    with open(path + pointer.get_file(), 'a') as f:
                        for event in self.get_event_pack():
                            print(event)
                            f.write(str(event) + '\n')
                            data_files.change_state_line(path,files['state_file'], pointer.get_line())
                            pointer.next_line()
                else:
                    pointer.set_file(data_files.create_new_datafile(path))
                    files['data_files'].append(pointer.get_file())
                    pointer.set_line(data_files.change_state_file(path,files['state_file'], pointer.get_file()))




