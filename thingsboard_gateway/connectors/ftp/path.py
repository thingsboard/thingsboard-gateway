import os
from thingsboard_gateway.connectors.connector import log


class Path:
    def __init__(self, path: str, with_sorting_files: bool, poll_period: int):
        self._path = path
        self._with_sorting_files = with_sorting_files
        self._poll_period = poll_period
        self._files = []
        self._last_polled_time = 0

    @staticmethod
    def __is_file(ftp, filename):
        current = ftp.pwd()
        try:
            ftp.cwd(filename)
        except Exception:
            ftp.cwd(current)
            return True
        ftp.cwd(current)
        return False

    @staticmethod
    def __folder_exist(ftp, folder_name):
        current = ftp.pwd()

        try:
            ftp.cwd(folder_name)
        except Exception:
            ftp.cwd(current)
            return False
        ftp.cwd(current)
        return True

    def __get_files(self, ftp, paths, file_name, file_ext):
        kwargs = {}
        for item in paths:
            ftp.cwd(item)

            folder_and_files = ftp.nlst()

            for ff in folder_and_files:
                cur_file_name, cur_file_ext = ff.split('.')
                if self.__is_file(ftp, ff):
                    if (file_name == '*' and file_ext == '*') or (file_ext != '*' and cur_file_ext == file_ext) or (
                            file_name != '*' and cur_file_name == file_name):
                        kwargs[ftp.voidcmd(f"MDTM {ff}")] = (item + '/' + ff)

        return [val for (_, val) in sorted(kwargs.items(), reverse=True)]

    def find_files(self, ftp):
        final_arr = []
        current_dir = ftp.pwd()

        dirname, basename = os.path.split(self._path)
        filename, fileex = basename.split('.')

        for (index, item) in enumerate(dirname.split('/')):
            if item == '*':
                current = ftp.pwd()
                arr = []
                for x in final_arr:
                    ftp.cwd(x)
                    node_paths = ftp.nlst()

                    for node in node_paths:
                        if not self.__is_file(ftp, node):
                            arr.append(ftp.pwd() + node)
                    final_arr = arr
                    ftp.cwd(current)
            else:
                if len(final_arr) > 0:
                    current = ftp.pwd()
                    for (j, k) in enumerate(final_arr):
                        ftp.cwd(k)
                        if self.__folder_exist(ftp, item):
                            final_arr[j] = str(final_arr[j]) + '/' + item
                        else:
                            final_arr = []  # TODO: uncomment it
                        ftp.cwd(current)
                else:
                    if self.__folder_exist(ftp, item):
                        final_arr.append(item)

        final_arr = self.__get_files(ftp, final_arr, filename, fileex)

        ftp.cwd(current_dir)

        log.debug(f'Find files {final_arr}')
        print(final_arr)  # TODO: delete this

        self._files = final_arr
