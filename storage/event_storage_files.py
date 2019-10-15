class EventStorageFiles():
    def __init__(self, state_file, data_files):
        self.state_file = state_file
        self.data_files = data_files

    def get_state_file(self):
        return self.state_file

    def get_data_files(self):
        return self.data_files

