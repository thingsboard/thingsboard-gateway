import os
import logging
import time

log = logging.getLogger(__name__)


class FileEventStorageFiles:
    def init_data_folder_if_not_exist(self, settings):
        path = settings.data_folder_path
        if not os.path.exists(path):
            try:
                os.makedirs(path)
            except OSError as e:
                log.error("Failed to create data folder!", e)
                pass

    def init_data_files(self, settings):
        data_files = []
        data_files_size = 0
        state_file = None
        data_dir = settings.data_folder_path
        if os.path.isdir(data_dir):
            for file in os.listdir(data_dir):
                if file.startswith('data_'):
                    data_files.append(file)
                    data_files_size += os.path.getsize(data_dir + file)
                elif file.startswith('state_'):
                    state_file = file
            if data_files_size == 0:
                data_files.append(self.create_new_datafile(settings))
            if not state_file:
                state_file = self.create_file(settings, '/state_', 'file.yaml')
            files = {'state_file': state_file, 'data_files': data_files}
            return files
        else:
            log.error("{} The specified path is not referred to the directory!".format(settings.data_folder_path))
            pass

    def create_new_datafile(self, settings):
        return self.create_file(settings, '/data_', (str(round(time.time() * 1000))) + '.txt')

    def create_file(self, settings, prefix, filename):
        file_path = settings.data_folder_path + prefix + filename
        try:
            file = open(file_path, 'w')
            file.close()
            return file_path
        except IOError as e:
            log.error("Failed to create a new file!", e)
            pass






