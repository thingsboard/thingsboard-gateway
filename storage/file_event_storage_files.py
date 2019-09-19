from tb_utility.tb_utility import TBUtility
import os
import logging
import time
import yaml

log = logging.getLogger(__name__)


class FileEventStorageFiles:
    def __init__(self, config):
        self.data_folder_path = TBUtility.get_parameter(config, "data_folder_path", './storage/data/')

    def init_data_folder_if_not_exist(self, data_folder_path):
        path = data_folder_path
        if not os.path.exists(path):
            try:
                os.makedirs(path)
            except OSError as e:
                log.error("Failed to create data folder!", e)
                pass

    def init_data_files(self, data_folder_path):
        self.init_data_folder_if_not_exist(data_folder_path)
        data_files_size = 0
        data_dir = data_folder_path
        files = {'state_file': None, 'data_files': []}
        if os.path.isdir(data_dir):
            for file in os.listdir(data_dir):
                if file.startswith('data_'):
                    files['data_files'].append(file)
                    data_files_size += os.path.getsize(data_dir + file)
                elif file.startswith('state_'):
                    files['state_file'] = file
            if data_files_size == 0:
                new_data_file = self.create_new_datafile(data_folder_path)
                files['data_files'].append(new_data_file)
            if not files['state_file']:
                new_state_file = self.create_file(data_folder_path, 'state_', 'file.yaml')
                state_data = {"read_file": new_data_file, "read_line": 1,
                              "write_file": new_data_file, "write_line": 1
                              }
                with open(data_folder_path + new_state_file, 'w') as f:
                    yaml.dump(state_data, f, default_flow_style=False)
                files['state_file'] = new_state_file
            return files
        else:
            log.error("{} The specified path is not referred to the directory!".format(data_folder_path))
            pass

    def create_new_datafile(self, data_folder_path):
        return self.create_file(data_folder_path, 'data_', (str(round(time.time() * 1000))) + '.txt')

    def create_file(self, data_folder_path, prefix, filename):
        file_path = data_folder_path + prefix + filename
        try:
            file = open(file_path, 'w')
            file.close()
            return prefix + filename
        except IOError as e:
            log.error("Failed to create a new file!", e)
            pass

    def read_data_files(self, data_folder_path):
        data_dir = data_folder_path
        files = {'state_file': None, 'data_files': []}
        if os.path.isdir(data_dir):
            for file in os.listdir(data_dir):
                if file.startswith('data_'):
                    files['data_files'].append(file)
                elif file.startswith('state_'):
                    files['state_file'] = file
            return files
        else:
            log.error("{} The specified path is not referred to the directory!".format(data_folder_path))
            pass

    def change_state_line(self, data_folder_path, state_file , line):
        with open(data_folder_path + state_file) as f:
            state = yaml.safe_load(f)
            state['write_line'] = line
        with open(data_folder_path + state_file, 'w') as f:
            yaml.dump(state, f)
        return line

    def change_state_file(self, data_folder_path, state_file, filename):
        with open(data_folder_path + state_file) as f:
            state = yaml.safe_load(f)
            state['write_file'] = filename
        with open(data_folder_path + state_file, 'w') as f:
            yaml.dump(state, f)
        return filename

    def delete_file(self, data_folder_path, file_list: list, file):
        full_name = data_folder_path + file
        try:
            file_list.remove(file)
            os.remove(full_name)
        except ValueError as e:
            log.warning("There is no file {} in file list".format(file))
        except OSError as e:
            log.warning("Could not delete file {}".format(file))


class FileEventStoragePointer:
    def __init__(self, path, file, line):
        self.path = path
        self.file = file
        self.line = line

    def __eq__(self, other):
        return self.file == other.file and self.line == other.line

    def get_file(self):
        return self.file

    def get_line(self):
        return self.line

    def set_file(self, file):
        self.file = file

    def set_line(self, line):
        self.line = line

    def get_write_position(self, path, files):
        #position = yaml.safe_load(open(path + files['state_file']))
        #return position['write_file'], position['write_line']
        pass

    def file_is_full(self, path, file, max_lines):
        lines = 0
        with open(path + file) as f:
            for lines, l in enumerate(f):
                pass
        lines += 1
        return False if lines < max_lines else True


    def next_line(self):
        self.line += 1

    def next_read_file(self, file_list):
        return sorted(file_list)[0]
