#     Copyright 2022. ThingsBoard
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

from importlib.util import module_from_spec, spec_from_file_location
from inspect import getmembers, isclass
from logging import getLogger
from os import listdir, path

log = getLogger("service")

EXTENSIONS_FOLDER = '/extensions'.replace('/', path.sep)
CONNECTORS_FOLDER = '/connectors'.replace('/', path.sep)
GRPC_CONNECTORS_FOLDER = '/grpc_connectors'.replace('/', path.sep)
DEB_INSTALLATION_EXTENSION_PATH = '/var/lib/thingsboard_gateway/extensions'.replace('/', path.sep)


class TBModuleLoader:
    PATHS = []
    LOADED_CONNECTORS = {}

    @staticmethod
    def find_paths():
        root_path = path.abspath(path.dirname(path.dirname(__file__)))
        log.debug("Root path is: " + root_path)
        if path.exists(DEB_INSTALLATION_EXTENSION_PATH):
            log.debug("Debian installation extensions folder exists.")
            TBModuleLoader.PATHS.append(DEB_INSTALLATION_EXTENSION_PATH)
        TBModuleLoader.PATHS.append(root_path + EXTENSIONS_FOLDER)
        TBModuleLoader.PATHS.append(root_path + CONNECTORS_FOLDER)
        TBModuleLoader.PATHS.append(root_path + GRPC_CONNECTORS_FOLDER)

    @staticmethod
    def import_module(extension_type, module_name):
        if len(TBModuleLoader.PATHS) == 0:
            TBModuleLoader.find_paths()
        buffered_module_name = extension_type + module_name
        if TBModuleLoader.LOADED_CONNECTORS.get(buffered_module_name) is not None:
            return TBModuleLoader.LOADED_CONNECTORS[buffered_module_name]
        try:
            for current_path in TBModuleLoader.PATHS:
                current_extension_path = current_path + path.sep + extension_type
                if path.exists(current_extension_path):
                    for file in listdir(current_extension_path):
                        if not file.startswith('__') and file.endswith('.py'):
                            try:
                                module_spec = spec_from_file_location(module_name, current_extension_path + path.sep + file)
                                log.debug(module_spec)

                                if module_spec is None:
                                    continue

                                module = module_from_spec(module_spec)
                                module_spec.loader.exec_module(module)
                                for extension_class in getmembers(module, isclass):
                                    if module_name in extension_class:
                                        log.info("Import %s from %s.", module_name, current_extension_path)
                                        TBModuleLoader.LOADED_CONNECTORS[buffered_module_name] = extension_class[1]
                                        return extension_class[1]
                            except ImportError as e:
                                log.exception(e)
                                continue
        except Exception as e:
            log.exception(e)
