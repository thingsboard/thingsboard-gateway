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
from simplejson import dumps, loads
from jsonpath_rw import parse


log = getLogger("service")


class TBUtility:

    @staticmethod
    def decode(message):
        content = loads(message.payload.decode("utf-8"))
        return content

    @staticmethod
    def validate_converted_data(data):
        json_data = dumps(data)
        if not data.get("deviceName") or data.get("deviceName") is None:
            log.error('deviceName is empty in data %s', json_data)
            return False
        if not data.get("deviceType") or data.get("deviceType") is None:
            log.error('deviceType is empty in data: %s', json_data)
            return False
        if not data.get("attributes") and not data.get("telemetry"):
            log.error('No telemetry and attributes in data: %s', json_data)
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
        extensions_paths = (path.abspath(path.dirname(path.dirname(__file__)) + '/connectors/'.replace('/', path.sep) + extension_type.lower()),
                            '/var/lib/thingsboard_gateway/'.replace('/', path.sep) + extension_type.lower(),
                            path.abspath(path.dirname(path.dirname(__file__)) + '/extensions/'.replace('/', path.sep) + extension_type.lower()))
        try:
            for extension_path in extensions_paths:
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
                                        return extension_class[1]
                            except ImportError:
                                continue
                else:
                    log.error("Import %s failed, path %s doesn't exist", module_name, extension_path)
        except Exception as e:
            log.exception(e)

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
