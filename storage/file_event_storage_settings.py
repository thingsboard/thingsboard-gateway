from tb_utility.tb_utility import TBUtility


class FileEventStorageSettings:
    def __init__(self, config):
        self.data_folder_path = TBUtility.get_parameter(config, "data_folder_path", "./")
        self.max_files_count = TBUtility.get_parameter(config, "max_files_count", "5")
        self.max_records_per_file = TBUtility.get_parameter(config, "max_records_per_file", "3")
        self.max_records_between_fsync = TBUtility.get_parameter(config, "max_records_between_fsync", "1")
        self.max_read_records_count = TBUtility.get_parameter(config, "max_read_records_count", "10")
        self.no_records_sleep_interval = TBUtility.get_parameter(config, "no_records_sleep_interval", "5000")

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
