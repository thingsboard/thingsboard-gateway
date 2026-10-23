#     Copyright 2026. ThingsBoard
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

from datetime import timedelta


class Utils:
    @staticmethod
    def to_json_safe(value):
        if isinstance(value, dict):
            return {str(Utils.to_json_safe(key)): Utils.to_json_safe(item) for key, item in value.items()}
        if isinstance(value, (list, tuple, set)):
            return [Utils.to_json_safe(item) for item in value]
        if isinstance(value, (bytes, bytearray)):
            try:
                text = bytes(value).decode('utf-8')
            except UnicodeDecodeError:
                return bytes(value).hex()
            if all(char.isprintable() or char in '\r\n\t' for char in text):
                return text
            return bytes(value).hex()
        if isinstance(value, timedelta):
            return value.total_seconds()
        if value is None or isinstance(value, (str, int, float, bool)):
            return value
        return str(value)
