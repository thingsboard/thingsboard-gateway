
class FileEventStorageSettings:
    def __init__(self):
        # TODO leave None or paste default values?
        self.data_folder_path = ''
        self.max_files_count = None
        self.max_records_per_file = None
        self.max_records_between_fsync = None
        self.max_read_records_count = None
        self.no_records_sleep_interval = None

    def get_data_folder_path(self):
        return self.data_folder_path

    def get_max_files_count(self):
        return self.max_files_count

    def get_max_records_per_file(self):
        return self.max_records_per_file

    def get_max_records_between_fsync(self):
        return self.max_records_between_fsync

    def get_max_read_records_count(self):
        return self.max_read_records_count

    def get_no_records_sleep_interval(self):
        return self.no_records_sleep_interval
