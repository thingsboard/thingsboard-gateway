import json


class FileEventStorageSettings:

    def __init__(self):
        self.data_folder_path = None
        self.max_file_count = None
        self.max_records_per_file = None
        self.max_records_between_fsync = None
        self.max_read_records_count = None
        self.no_records_sleep_interval = None

    def set_storage_settings(self, data_folder_path, max_file_count, max_records_per_file, max_records_between_fsync, max_read_records_count, no_records_sleep_interval):
        self.data_folder_path = data_folder_path
        self.max_records_per_file = max_records_per_file
        self.max_records_between_fsync = max_records_between_fsync
        self.max_file_count = max_file_count
        self.no_records_sleep_interval = no_records_sleep_interval
        self.max_read_records_count = max_read_records_count


file_event_storage_settings = FileEventStorageSettings()
with open('../config/tb_gateway.json') as settings_file:
    settings = json.load(settings_file)

file_event_storage_settings.set_storage_settings(**settings['storage'])
