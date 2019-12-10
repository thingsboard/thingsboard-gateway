#     Copyright 2019. ThingsBoard
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

from os import path, listdir
from inspect import getmembers, isclass
from importlib import util
import jsonpath_rw_ext as jp
from logging import getLogger
from simplejson import dumps, loads
from re import search
from time import time


log = getLogger("service")


class TBUtility:

    @staticmethod
    def validate_converted_data(data):
        json_data = dumps(data)
        if not data.get("deviceName") or data.get("deviceName") is None:
            log.error('deviceName is empty in data %s', json_data)
            return False
        if not data.get("deviceType") or data.get("deviceType") is None:
            log.error('deviceType is empty in data: %s', json_data)
            return False
        if not data["attributes"] and not data["telemetry"]:
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
        try:
            if path.exists('/var/lib/thingsboard_gateway/'+extension_type.lower()):
                custom_extension_path = '/var/lib/thingsboard_gateway/' + extension_type.lower()
                log.info('Extension %s - looking for class in %s', extension_type, custom_extension_path)
            else:
                custom_extension_path = path.abspath(path.dirname(path.dirname(__file__)) + '/extensions/' + extension_type.lower())
                log.info('Extension %s - looking for class in %s', extension_type, custom_extension_path)
            for file in listdir(custom_extension_path):
                if not file.startswith('__') and file.endswith('.py'):
                    try:
                        module_spec = util.spec_from_file_location(module_name, custom_extension_path + '/' + file)
                        log.debug(module_spec)
                        if module_spec is None:
                            log.error('Module: {} not found'.format(module_name))
                            return None
                        else:
                            module = util.module_from_spec(module_spec)
                            log.debug(module)
                            try:
                                module_spec.loader.exec_module(module)
                            except Exception as e:
                                log.exception(e)
                            for extension_class in getmembers(module, isclass):
                                if module_name in extension_class:
                                    return extension_class[1]
                    except ImportError:
                        continue
                    except Exception as e:
                        log.exception(e)
        except Exception as e:
            log.exception(e)

    @staticmethod
    def get_value(expression, body={}, value_type="string", get_tag=False):
        T0 = time()*1000
        if isinstance(body, str):
            body = loads(body)
        if not expression:
            return ''
        p1 = search(r'\${', expression)
        p2 = search(r'}', expression)
        if p1 is not None and p2 is not None:
            p1 = p1.end()
            p2 = p2.start()
        else:
            p1 = 0
            p2 = len(expression)
        target_str = str(expression[p1:p2])
        if get_tag:
            return target_str
        full_value = None
        try:
            if value_type == "string":
                # tg_sp = target_str.split()[0]
                # log.warning(time() * 1000 - T0)
                # value = jp.match1(tg_sp, body)
                # full_value = expression[0: min(abs(p1 - 2), 0)] + jp.match1(target_str.split()[0], body) + expression[p2 + 1:len(expression)]
                full_value = expression[0: min(abs(p1 - 2), 0)] + body[target_str.split()[0]] + expression[p2 + 1:len(expression)]
                # log.warning(time() * 1000 - T0)
                # if full_value is None:
                #     if body.get(target_str):
                #         full_value = expression[0: min(abs(p1-2), 0)] + body[target_str] + expression[p2+1:len(expression)]
                #     else:
                #         full_value = expression[0: min(abs(p1-2), 0)] + jp.match1(target_str.split()[0], dumps(body)) + expression[p2+1:len(expression)]
            else:
                # full_value = jp.match1(target_str.split()[0], loads(body) if type(body) == str else body)
                full_value = body[target_str.split()[0]]
        except TypeError:
            if full_value is None:
                log.error('Value is None - Cannot find the pattern: %s in %s. Expression will be interpreted as value.', target_str, dumps(body))
                full_value = expression
        except Exception as e:
            log.error(e)
            return None
        return full_value
