import os
import re
import inspect
import importlib
import importlib.util
import jsonpath_rw_ext as jp
from logging import getLogger
from json import dumps, loads
from re import search

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
        for file in os.listdir('./extensions/'+extension_type.lower()):
            if not file.startswith('__') and file.endswith('.py'):
                mod = 'extensions.'+extension_type.lower()+'.'+file.replace('.py', '')
                try:
                    module_spec = importlib.util.find_spec(mod)
                    if module_spec is None:
                        log.error('Module: {} not found'.format(module_name))
                        return None
                    else:
                        module = importlib.util.module_from_spec(module_spec)
                        module_spec.loader.exec_module(module)
                        for converter_class in inspect.getmembers(module, inspect.isclass):
                            if module_name in converter_class:
                                return converter_class[1]
                except ImportError:
                    continue

    @staticmethod
    def get_value(expression, body={}, value_type="string", get_tag=False):
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
        value = True
        try:
            if value_type == "string":
                value = jp.match1(target_str.split()[0], dumps(body))
                if value is None and body.get(target_str):
                    full_value = expression[0: min(abs(p1-2), 0)] + body[target_str] + expression[p2+1:len(expression)]
                elif value is None:
                    full_value = expression[0: min(abs(p1-2), 0)] + jp.match1(target_str.split()[0], loads(body) if type(body) == str else body) + expression[p2+1:len(expression)]
                else:
                    full_value = expression[0: min(abs(p1-2), 0)] + value + expression[p2+1:len(expression)]
            else:
                full_value = jp.match1(target_str.split()[0], loads(body) if type(body) == str else body)

        except TypeError:
            if value is None:
                log.error('Value is None - Cannot find the pattern: %s in %s', target_str, dumps(body))
            return None
        except Exception as e:
            log.error(e)
            return None
        return full_value

    @staticmethod
    def check_logs_directory(conf_file_path):
        try:
            log = getLogger(__name__)
        except Exception:
            pass
        with open(conf_file_path) as conf_file:
            logs_directories = set()
            for line in conf_file.readlines():
                target = re.search(r"[\"|\'](.+[/])(.+\.log)\"|\'", line)
                if target:
                    if target.group(1) is not None:
                        logs_directories.add(target.group(1))
        for logs_dir in logs_directories:
            if not os.path.exists(logs_dir):
                log.error("Logs directory not exists.")
                try:
                    os.mkdir(logs_dir)
                except Exception as e:
                    log.exception(e)
