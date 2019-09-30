import copy

class EventStorageReader:
    def __init__(self, files, settings):
        self.files =files
        self.settings = settings
        self.current_pos = '' # TODO read_state_file()
        self.new_pos = copy.deepcopy(self.current_pos)
