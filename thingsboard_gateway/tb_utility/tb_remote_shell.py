#     Copyright 2024. ThingsBoard
#
#     Licensed under the Apache License, Version 2.0 (the "License");
#     you may not use this file except in compliance with the License.
#     You may obtain a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#     Unless required by applicable law or agreed to in writing, software
#     distributed under the License is distributed on an "AS IS" BASIS,
#     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#     See the License for the specific language governing permissions and
#     limitations under the License.
#

from logging import getLogger
from os import chdir, getcwd
from subprocess import PIPE, Popen, STDOUT, TimeoutExpired

log = getLogger("service")


class RemoteShell:
    def __init__(self, platform, release):
        self.__session_active = False
        self.__platform = platform
        self.__release = release
        self.shell_commands = {
            "getTermInfo": self.get_term_info,
            "sendCommand": self.send_command,
            "getCommandStatus": self.get_command_status,
            "terminateCommand": self.terminate_command,
            }
        self.command_in_progress = None
        self.__previous_stdout = b""
        self.__previous_stderr = b""

    def get_term_info(self, *args):
        return {"platform": self.__platform, "release": self.__release, "cwd": str(getcwd())}

    def send_command(self, *args):
        result = {"ok": False, "qos": 0}
        log.debug("Received command to shell with args: %r", args)
        command = args[0]['command']
        cwd = args[0].get('cwd')
        if cwd is not None and str(getcwd()) != cwd:
            chdir(cwd)
        if command.split():
            if self.command_in_progress is not None:
                log.debug("Received a new command: \"%s\", during old command is running, terminating old command...", command)
                old_command = self.command_in_progress.args
                self.terminate_command()
                log.debug("Old command: \"%s\" terminated.", old_command)
            if command.split()[0] in ["quit", "exit"]:
                self.command_in_progress = None
            elif command.split()[0] == "cd":
                chdir(command.split()[1])
                self.command_in_progress = "cd"
            else:
                log.debug("Run command in remote shell: %s", command)
                self.command_in_progress = Popen(command, shell=True, stdout=PIPE, stdin=PIPE, stderr=STDOUT, universal_newlines=True)
            result.update({"ok": True})
        return result

    def get_command_status(self, *args):
        result = {"data": [{"stdout": "",
                            "stderr": ""}],
                  "cwd": str(getcwd()),
                  "done": True,
                  "qos": 0,
                  }
        done = False
        if self.command_in_progress == "cd":
            done = True
        elif self.command_in_progress is not None:
            stdout_value = b""
            stderr_value = b""
            done = True if self.command_in_progress.poll() is not None else False
            try:
                stdout_value, stderr_value = self.command_in_progress.communicate(timeout=.1)
            except TimeoutExpired as e:
                log.debug("Process is run")
                stdout_value = b"" if e.stdout is None else e.stdout.replace(self.__previous_stdout, b"")
                stderr_value = b"" if e.stderr is None else e.stderr.replace(self.__previous_stderr, b"")
                stdout_value = stdout_value[:-1] if len(stdout_value) and stdout_value[-1] == b"\n" else stdout_value
                stderr_value = stderr_value[:-1] if len(stderr_value) and stderr_value[-1] == b"\n" else stderr_value
            self.__previous_stderr = self.__previous_stderr + b"" if stderr_value is None else stderr_value
            self.__previous_stdout = self.__previous_stdout + b"" if stdout_value is None else stdout_value
            str_stdout = str(stdout_value, "UTF-8") if isinstance(stdout_value, bytes) else stdout_value
            str_stderr = str(stderr_value, "UTF-8") if isinstance(stderr_value, bytes) else stderr_value
            result.update({"data": [{"stdout": str_stdout,
                                     "stderr": str_stderr}]
                           })
        result.update({"done": done})
        if done:
            self.command_in_progress = None

        return result

    def terminate_command(self, *args):
        result = {"ok": False}
        if self.command_in_progress is not None:
            try:
                self.command_in_progress.terminate()
                self.__previous_stderr = b""
                self.__previous_stdout = b""
                result.update({"ok": True})
            except Exception as e:
                log.exception(e)
                result["error"] = str(e)
        else:
            result["error"] = "Process for termination not found."
        return result
