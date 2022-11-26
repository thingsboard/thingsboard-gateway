import os
import inspect
import re
import subprocess
import sys
from time import time, sleep


class HotReloader:
    def __init__(self, function):
        self._stopped = False
        self._file_pattern = r'^(?!.*.(pyc|log|\d)$).*$'
        self._exclude_files = [
            'connected_devices.json',
            'persistent_keys.json'
        ]
        self._runnable_function = function
        self._poll_interval = 1
        self._process = None

        self._runnable_function_path = os.path.abspath(inspect.getfile(self._runnable_function))
        self._root_path = '/'.join(os.path.abspath(inspect.getfile(self._runnable_function)).split('/')[:-2])
        self._files = self._find_files()

        self._last_poll_time = 0

        self.run()

    def _find_files(self):
        files = []
        for root, _, filenames in os.walk(self._root_path):
            for filename in filenames:
                if re.match(self._file_pattern, filename) and filename not in self._exclude_files:
                    file_path = os.path.join(root, filename)
                    files.append((file_path, os.stat(file_path).st_mtime))

        return files

    def run(self):
        try:
            self.execute()

            while not self._stopped:
                if time() - self._last_poll_time >= self._poll_interval:
                    files = self._find_files()
                    if files != self._files:
                        print('Reloading....')
                        self.execute()
                        self._files = files

                    self._last_poll_time = time()

                sleep(.2)
        except KeyboardInterrupt:
            self._stopped = True

    def execute(self):
        if self._process is not None:
            self._process.kill()
            self._process.wait()

        self._process = subprocess.Popen([sys.executable, self._runnable_function_path])
