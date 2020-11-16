#     Copyright 2020. ThingsBoard
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

from re import search
from os import path, listdir
from inspect import getmembers, isclass
from importlib import util
from logging import getLogger
from simplejson import dumps, loads, JSONDecodeError
from jsonpath_rw import parse
from platform import system


log = getLogger("service")


class TBUtility:

    # Buffer for connectors/converters
    # key - class name
    # value - loaded class
    loaded_extensions = {}

    @staticmethod
    def decode(message):
        try:
            if isinstance(message.payload, bytes):
                content = loads(message.payload.decode("utf-8", "ignore"))
            else:
                content = loads(message.payload)
        except JSONDecodeError:
            if isinstance(message.payload, bytes):
                content = message.payload.decode("utf-8", "ignore")
            else:
                content = message.payload
        return content

    @staticmethod
    def validate_converted_data(data):
        error = None
        if error is None and not data.get("deviceName"):
            error = 'deviceName is empty in data: '
        if error is None and not data.get("deviceType"):
            error = 'deviceType is empty in data: '
        if error is None and not data.get("attributes") and not data.get("telemetry"):
            error = 'No telemetry and attributes in data: '
        if error is not None:
            json_data = dumps(data)
            if isinstance(json_data, bytes):
                log.error(error+json_data.decode("UTF-8"))
            else:
                log.error(error + json_data)
            return False
        return True

    @staticmethod
    def topic_to_regex(topic):
        return topic.replace("+", "[^/]+").replace("#", ".+")

    @staticmethod
    def regex_to_topic(regex):
        return regex.replace("[^/]+", "+").replace(".+", "#")

    @staticmethod
    def check_and_import(extension_type, module_name):
        if TBUtility.loaded_extensions.get(extension_type + module_name) is None:
            if system() == "Windows":
                extensions_paths = (path.abspath(path.dirname(path.dirname(__file__)) + '/connectors/'.replace('/', path.sep) + extension_type.lower()),
                                    path.abspath(path.dirname(path.dirname(__file__)) + '/extensions/'.replace('/', path.sep) + extension_type.lower()))
            else:
                extensions_paths = tuple(path.abspath(path.dirname(path.dirname(__file__)) + '/connectors/'.replace('/', path.sep) + extension_type.lower()))
                extension_folder_path = '/var/lib/thingsboard_gateway/extensions/'.replace('/', path.sep) + extension_type.lower()
                if path.exists(extension_folder_path):
                    extensions_paths = extensions_paths + tuple(extension_folder_path)
                extensions_paths = extensions_paths + tuple(path.abspath(path.dirname(path.dirname(__file__)) + '/extensions/'.replace('/', path.sep) + extension_type.lower()))
            try:
                for extension_path in extensions_paths:
                    if TBUtility.loaded_extensions.get(extension_type + module_name) is not None:
                        return TBUtility.loaded_extensions[extension_type + module_name]
                    if path.exists(extension_path):
                        for file in listdir(extension_path):
                            if not file.startswith('__') and file.endswith('.py'):
                                try:
                                    module_spec = util.spec_from_file_location(module_name, extension_path + path.sep + file)
                                    log.debug(module_spec)

                                    if module_spec is None:
                                        log.error('Module: %s not found', module_name)
                                        continue

                                    module = util.module_from_spec(module_spec)
                                    log.debug(str(module))
                                    module_spec.loader.exec_module(module)
                                    for extension_class in getmembers(module, isclass):
                                        if module_name in extension_class:
                                            log.debug("Import %s from %s.", module_name, extension_path)
                                            # Save class into buffer
                                            TBUtility.loaded_extensions[extension_type + module_name] = extension_class[1]
                                            return extension_class[1]
                                except ImportError:
                                    continue
                    else:
                        log.error("Import %s failed, path %s doesn't exist", module_name, extension_path)
            except Exception as e:
                log.exception(e)
        else:
            log.debug("Class %s found in TBUtility buffer.", module_name)
            return TBUtility.loaded_extensions[extension_type + module_name]

    @staticmethod
    def get_value(expression, body=None, value_type="string", get_tag=False, expression_instead_none=False):
        if isinstance(body, str):
            body = loads(body)
        if not expression:
            return ''
        positions = search(r'\${(?:(.*))}', expression)
        if positions is not None:
            p1 = positions.regs[-1][0]
            p2 = positions.regs[-1][1]
        else:
            p1 = 0
            p2 = len(expression)
        target_str = str(expression[p1:p2])
        if get_tag:
            return target_str
        full_value = None
        try:
            if isinstance(body, dict) and target_str.split()[0] in body:
                if value_type.lower() == "string":
                    full_value = expression[0: max(abs(p1 - 2), 0)] + body[target_str.split()[0]] + expression[p2 + 1:len(expression)]
                else:
                    full_value = body.get(target_str.split()[0])
            elif isinstance(body, (dict, list)):
                try:
                    jsonpath_expression = parse(target_str)
                    jsonpath_match = jsonpath_expression.find(body)
                    if jsonpath_match:
                        full_value = jsonpath_match[0].value
                except Exception as e:
                    log.debug(e)
            elif isinstance(body, (str, bytes)):
                search_result = search(expression, body)
                if search_result.groups():
                    full_value = search_result.group(0)
            if expression_instead_none and full_value is None:
                full_value = expression
        except Exception as e:
            log.exception(e)
        return full_value

    @staticmethod
    def install_package(package, version="upgrade"):
        from sys import executable
        from subprocess import check_call, CalledProcessError
        result = False
        if version.lower() == "upgrade":
            try:
                result = check_call([executable, "-m", "pip", "install", package, "--upgrade", "--user"])
            except CalledProcessError:
                result = check_call([executable, "-m", "pip", "install", package, "--upgrade"])
        else:
            from pkg_resources import get_distribution
            current_package_version = None
            try:
                current_package_version = get_distribution(package)
            except Exception:
                pass
            if current_package_version is None or current_package_version != version:
                installation_sign = "==" if ">=" not in version else ""
                try:
                    result = check_call([executable, "-m", "pip", "install", package + installation_sign + version, "--user"])
                except CalledProcessError:
                    result = check_call([executable, "-m", "pip", "install", package + installation_sign + version])
        return result
