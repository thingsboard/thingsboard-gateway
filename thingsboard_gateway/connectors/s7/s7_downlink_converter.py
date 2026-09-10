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

import struct
import snap7.util
from snap7.tags import Tag

from re import search
from datetime import datetime, date, timedelta, time, timezone

from thingsboard_gateway.tb_utility.tb_utility import TBUtility


class S7DownlinkConverter:
    def __init__(self, logger):
        self._log = logger

    def convert(self, config, data):
        request_type = config.get('type')
        if request_type == 'data':
            return self._convert_data_request(config, data)
        elif request_type == 'tag':
            return self._convert_tag_request(config, data)
        elif request_type == 'vm':
            return self._convert_vm_request(data)
        else:
            self._log.error(
                f"Unsupported request type '{request_type}' for downlink conversion.")
            return None

    def _convert_data_request(self, config, data):
        data_type = str(config.get("dataType", "raw")).lower().strip()
        offset = config.get("offset", 0)
        bit_index = config.get("bit", config.get("bitIndex", 0))
        specified_size = config.get("size", 0)

        type_sizes = {
            "bool": 1,
            "boolean": 1,
            "bit": 1,
            "byte": 1,
            "usint": 1,
            "uint8": 1,
            "sint": 1,
            "int8": 1,
            "int": 2,
            "int16": 2,
            "short": 2,
            "uint": 2,
            "uint16": 2,
            "word": 2,
            "dint": 4,
            "int32": 4,
            "udint": 4,
            "uint32": 4,
            "dword": 4,
            "real": 4,
            "float": 4,
            "float32": 4,
            "lreal": 8,
            "double": 8,
            "float64": 8,
            "char": 1,
            "character": 1,
            "wchar": 2,
            "widecharacter": 2,
            "lint": 8,
            "int64": 8,
            "ulint": 8,
            "uint64": 8,
            "lword": 8,
            "time": 4,
            "dint_time": 4,
            "ltime": 8,
            "tod": 4,
            "time_of_day": 4,
            "ltod": 8,
            "date": 2,
            "dt": 8,
            "date_and_time": 8,
            "ldt": 8,
            "date_and_ltime": 8,
            "dtl": 12,
        }

        min_size = type_sizes.get(data_type, 0)

        if data_type in ("string", "str", "s7string"):
            max_len = config.get(
                "maxLength", specified_size - 2 if specified_size > 2 else 254)
            min_size = max_len + 2
        elif data_type in ("raw", "bytes", "bytearray", "array"):
            min_size = len(data)

        buf_size = max(specified_size, offset + min_size)
        buf = bytearray(buf_size)

        if data_type in ("bool", "boolean", "bit"):
            bool_value = TBUtility.str_to_bool(data)
            snap7.util.set_bool(buf, offset, bit_index, bool_value)

        elif data_type in ("byte", "usint", "uint8"):
            snap7.util.set_usint(buf, offset, int(data))

        elif data_type in ("sint", "int8"):
            struct.pack_into(">b", buf, offset, int(data))

        elif data_type in ("int", "int16", "short"):
            snap7.util.set_int(buf, offset, int(data))

        elif data_type in ("uint", "uint16", "word"):
            snap7.util.set_uint(buf, offset, int(data))

        elif data_type in ("dint", "int32"):
            snap7.util.set_dint(buf, offset, int(data))

        elif data_type in ("udint", "uint32", "dword"):
            snap7.util.set_dword(buf, offset, int(data))

        elif data_type in ("real", "float", "float32"):
            snap7.util.set_real(buf, offset, float(data))

        elif data_type in ("lreal", "double", "float64"):
            struct.pack_into(">d", buf, offset, float(data))

        elif data_type in ("string", "str", "s7string"):
            str_val = str(data)
            max_len = config.get(
                "maxLength", specified_size - 2 if specified_size > 2 else 254)
            snap7.util.set_string(buf, offset, str_val, max_len)

        elif data_type in ("char", "character"):
            char_code = ord(str(data)[0]) if isinstance(data, str) else int(data)
            struct.pack_into(">B", buf, offset, char_code)

        elif data_type in ("wchar", "widecharacter"):
            char_code = ord(str(data)[0]) if isinstance(data, str) else int(data)
            struct.pack_into(">H", buf, offset, char_code)

        elif data_type in ("lint", "int64"):
            struct.pack_into(">q", buf, offset, int(data))

        elif data_type in ("ulint", "uint64", "lword"):
            struct.pack_into(">Q", buf, offset, int(data))

        elif data_type in ("time", "dint_time"):
            # Accepts seconds (float) or milliseconds (int) -> converts to int ms
            ms = int(float(data) * 1000) if isinstance(data, (float, str)) else int(data)
            snap7.util.set_dint(buf, offset, ms)

        elif data_type == "ltime":
            # Accepts seconds (float) or nanoseconds (int) -> converts to int ns
            ns = int(float(data) * 1_000_000_000) if isinstance(data, (float, str)) else int(data)
            struct.pack_into(">q", buf, offset, ns)

        elif data_type in ("tod", "time_of_day"):
            # Accepts "HH:MM:SS.mmm" or ms since midnight
            if isinstance(data, str):
                t = time.fromisoformat(data)
                ms = (t.hour * 3600 + t.minute * 60 + t.second) * 1000 + (t.microsecond // 1000)
            else:
                ms = int(data)
            snap7.util.set_dword(buf, offset, ms)

        elif data_type == "ltod":
            # Accepts "HH:MM:SS.ffffff" or ns since midnight
            if isinstance(data, str):
                t = time.fromisoformat(data)
                ns = ((t.hour * 3600 + t.minute * 60 + t.second) * 1_000_000 + t.microsecond) * 1000
            else:
                ns = int(data)
            struct.pack_into(">Q", buf, offset, ns)

        elif data_type == "date":
            if isinstance(data, str):
                d = date.fromisoformat(data)
                days = (d - date(1990, 1, 1)).days
            else:
                days = int(data)
            snap7.util.set_uint(buf, offset, days)

        elif data_type in ("dt", "date_and_time"):
            dt_val = datetime.fromisoformat(str(data)) if isinstance(data, str) else data

            def _int_to_bcd(val):
                return ((val // 10) << 4) | (val % 10)

            year = dt_val.year % 100
            ms = dt_val.microsecond // 1000
            dow = dt_val.isoweekday() % 7 + 1  # 1=Sun, 7=Sat

            buf[offset] = _int_to_bcd(year)
            buf[offset + 1] = _int_to_bcd(dt_val.month)
            buf[offset + 2] = _int_to_bcd(dt_val.day)
            buf[offset + 3] = _int_to_bcd(dt_val.hour)
            buf[offset + 4] = _int_to_bcd(dt_val.minute)
            buf[offset + 5] = _int_to_bcd(dt_val.second)
            buf[offset + 6] = _int_to_bcd(ms // 10)
            buf[offset + 7] = ((ms % 10) << 4) | dow

        elif data_type in ("ldt", "date_and_ltime"):
            if isinstance(data, str):
                dt_val = datetime.fromisoformat(data)
                if dt_val.tzinfo is None:
                    dt_val = dt_val.replace(tzinfo=timezone.utc)
                ns = int(dt_val.timestamp() * 1_000_000_000)
            else:
                ns = int(data)
            struct.pack_into(">q", buf, offset, ns)

        elif data_type == "dtl":
            dt_val = datetime.fromisoformat(str(data)) if isinstance(data, str) else data
            dow = dt_val.isoweekday() % 7 + 1
            ns = dt_val.microsecond * 1000

            struct.pack_into(
                ">HBBBBBB I", buf, offset,
                dt_val.year, dt_val.month, dt_val.day,
                dow, dt_val.hour, dt_val.minute, dt_val.second, ns
            )

        elif data_type.startswith("fstring") or data_type.startswith("char["):
            match = search(r"\[?(\d+)\]?", data_type)
            f_size = int(match.group(1)) if match else min_size

            encoded = str(data).encode("latin-1")[:f_size]
            buf[offset: offset + len(encoded)] = encoded

            for i in range(offset + len(encoded), offset + f_size):
                buf[i] = ord(" ")

        elif data_type in ("raw", "bytes", "bytearray", "array"):
            raw_bytes = bytearray(data)
            buf[offset: offset + len(raw_bytes)] = raw_bytes

        else:
            raise ValueError(f"Unsupported dataType: '{data_type}'")

        return buf

    def _convert_tag_request(self, config, data):
        tag_str = config.get('tag')

        try:
            tag = Tag.from_string(tag_str)
        except Exception as e:
            self._log.error(f"Failed to parse tag '{tag_str}': {e}")
            return None

        if tag.is_symbolic:
            self._log.error(
                f"Failed to process tag '{tag_str}': Symbolic (LID-based) tag access is not supported")
            return None

        datatype = tag.datatype.upper()

        try:
            if tag.count > 1:
                if not isinstance(data, (list, tuple)):
                    self._log.error(
                        f"Failed to convert value for tag '{tag_str}': "
                        f"Expected list/tuple for array (count={tag.count}), got {data!r}")
                    return None
                return [self._convert_scalar(datatype, v, tag_str) for v in data]

            return self._convert_scalar(datatype, data, tag_str)
        except (ValueError, TypeError) as e:
            self._log.error(
                f"Failed to convert value '{data}' to type '{datatype}' for tag '{tag_str}': {e}")
            return None

    def _convert_vm_request(self, data):
        try:
            return int(data)
        except (ValueError, TypeError):
            pass

        try:
            return int(TBUtility.str_to_bool(data))
        except ValueError as e:
            self._log.error(
                f"Failed to convert value '{data}' to int for vm downlink conversion: {e}"
            )
            return None

    def _convert_scalar(self, datatype, value, tag_str):

        if datatype == 'BOOL':
            return TBUtility.str_to_bool(value)

        elif datatype in ('BYTE', 'SINT', 'USINT', 'INT', 'UINT', 'WORD',
                          'DINT', 'UDINT', 'DWORD', 'LINT', 'ULINT', 'LWORD'):
            return int(value)

        elif datatype in ('REAL', 'LREAL'):
            return float(value)

        elif datatype in ('CHAR', 'WCHAR'):
            return str(value)[:1]

        elif datatype.startswith(('STRING', 'FSTRING', 'WSTRING')):
            return str(value)

        elif datatype == 'TIME':
            if isinstance(value, (int, float)) or (isinstance(value, str) and value.replace('.', '', 1).isdigit()):
                ms = int(float(value))
            else:
                # Parse 'HH:MM:SS.mmm' or 'SS.mmm'
                str_val = str(value).lstrip('T#').lstrip('t#')
                parts = str_val.split(':')
                if len(parts) == 3:
                    h, m, s = float(parts[0]), float(parts[1]), float(parts[2])
                    ms = int((h * 3600 + m * 60 + s) * 1000)
                else:
                    ms = int(float(str_val) * 1000)

            td = timedelta(milliseconds=ms)
            days = td.days
            hours, remainder = divmod(td.seconds, 3600)
            minutes, seconds = divmod(remainder, 60)
            milliseconds = td.microseconds // 1000

            # Construct S7 'T#1d2h3m4s500ms' string format
            time_str = "T#"
            if days > 0:
                time_str += f"{days}d"
            if hours > 0:
                time_str += f"{hours}h"
            if minutes > 0:
                time_str += f"{minutes}m"
            if seconds > 0 or milliseconds > 0 or time_str == "T#":
                time_str += f"{seconds}s"
            if milliseconds > 0:
                time_str += f"{milliseconds}ms"

            return time_str

        elif datatype == 'DATE':
            # Convert ISO date string ('YYYY-MM-DD') or days since 1990-01-01 into date object
            if isinstance(value, (int, float)) or (isinstance(value, str) and value.isdigit()):
                return date(1990, 1, 1) + timedelta(days=int(value))
            return date.fromisoformat(str(value))

        elif datatype in ('DT', 'DTL'):
            # Convert ISO datetime string into datetime object
            if isinstance(value, (int, float)):
                return datetime.fromtimestamp(float(value), tz=timezone.utc)
            return datetime.fromisoformat(str(value))

        elif datatype in ('LTOD', 'TOD'):
            # Convert 'HH:MM:SS[.mmm/ns]' or milliseconds/nanoseconds since midnight into time/timedelta object
            if isinstance(value, (int, float)) or (isinstance(value, str) and value.isdigit()):
                # Treat numeric as ms (TOD) or ns (LTOD) since midnight
                unit_sec = int(value) / (1_000_000_000.0 if datatype == 'LTOD' else 1000.0)
                return timedelta(seconds=unit_sec)
            else:
                # Parse ISO time string 'HH:MM:SS.ffffff'
                val_str = str(value)
                t = time.fromisoformat(val_str)
                return timedelta(
                    hours=t.hour,
                    minutes=t.minute,
                    seconds=t.second,
                    microseconds=t.microsecond
                )

        elif datatype == 'LTIME':
            # Convert seconds (float) or nanoseconds (int) or 'LT#...' string into timedelta
            if isinstance(value, (int, float)) or (isinstance(value, str) and value.replace('.', '', 1).isdigit()):
                val_num = float(value)
                seconds = val_num if isinstance(value, float) or '.' in str(value) else val_num / 1_000_000_000.0
                return timedelta(seconds=seconds)
            else:
                str_val = str(value).lstrip('LTIME#').lstrip('ltime#').lstrip('LT#').lstrip('lt#')
                return timedelta(seconds=float(str_val))

        elif datatype == 'LDT':
            # Convert ISO datetime string or nanosecond UNIX epoch into UTC datetime object
            if isinstance(value, (int, float)) or (isinstance(value, str) and value.isdigit()):
                ns = int(value)
                return datetime.fromtimestamp(ns / 1_000_000_000.0, tz=timezone.utc)

            dt_obj = datetime.fromisoformat(str(value))
            if dt_obj.tzinfo is None:
                dt_obj = dt_obj.replace(tzinfo=timezone.utc)
            return dt_obj

        else:
            self._log.warning(
                f"Unsupported datatype '{datatype}' for tag '{tag_str}', passing value as-is")
            return value
