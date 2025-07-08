#     Copyright 2025. ThingsBoard
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

from csv import reader as csv_reader
from base64 import b64decode
from io import StringIO

from thingsboard_gateway.connectors.bacnet.constants import (
    DEVICE_OBJECT_TYPE_CODE,
    EDE_CONFIG_PARAMETER,
    EDE_FILE_PATH_PARAMETER,
    OBJECT_TYPE_PARAMETER,
    OBJECT_INSTANCE_PARAMETER,
    DEVICE_OBJ_INSTANCE_PARAMETER,
    OBJECT_NAME_PARAMETER
)
from thingsboard_gateway.connectors.bacnet.entities.ede_connector_config import EDEConnectorConfig


class EDEParser:
    @staticmethod
    def is_ede_config(config) -> bool:
        return EDE_FILE_PATH_PARAMETER in config or EDE_CONFIG_PARAMETER in config

    @staticmethod
    def parse(config):
        if EDE_FILE_PATH_PARAMETER in config:
            parsed_config = EDEParser.parse_from_file(config[EDE_FILE_PATH_PARAMETER])
            config.pop(EDE_FILE_PATH_PARAMETER, None)
        elif EDE_CONFIG_PARAMETER in config:
            parsed_config = EDEParser.parse_from_base64(config[EDE_CONFIG_PARAMETER])
            config.pop(EDE_CONFIG_PARAMETER, None)

        return {**config, **parsed_config.config}

    @staticmethod
    def parse_from_file(file_path: str) -> str:
        with open(file_path, newline='', encoding='utf-8') as file:
            reader = csv_reader(file, delimiter=';')
            data = [row for row in reader]

        return EDEParser.__parse(data)

    @staticmethod
    def parse_from_base64(base64_string) -> str:
        decoded_data = b64decode(base64_string)
        file = StringIO(decoded_data.decode('utf-8'))
        data = csv_reader(file)

        return EDEParser.__parse(data)

    @staticmethod
    def __parse(data):
        csv_data = EDEParser.__parse_csv(data)
        return EDEParser.__parse_ede(csv_data)

    @staticmethod
    def __parse_csv(data):
        headers = []
        output = []

        for row in data:
            if not row or all(cell.strip() == '' for cell in row):
                continue

            if not row[0].startswith('#') and len(row) == 2:
                continue

            if row[0].startswith('# keyname'):
                headers = [col.strip().lstrip('#').strip() for col in row]
                continue

            if row[0].startswith('#'):
                continue

            if headers:
                entry = {headers[i]: row[i].strip() if i < len(row) else '' for i in range(len(headers))}
                output.append(entry)

        return output

    @staticmethod
    def __parse_ede(csv_data):
        connector_config = EDEConnectorConfig()

        for row in csv_data:
            if row.get(OBJECT_TYPE_PARAMETER) == DEVICE_OBJECT_TYPE_CODE:
                try:
                    device_objects = EDEParser.__find_device_objects(row[OBJECT_INSTANCE_PARAMETER], csv_data)
                    if len(device_objects) == 0:
                        continue

                    connector_config.add_device(device_name=row.get(OBJECT_NAME_PARAMETER, 'Unknown Device'),
                                                device_id=row[OBJECT_INSTANCE_PARAMETER],
                                                device_objects=device_objects)
                except KeyError:
                    continue

        return connector_config

    @staticmethod
    def __find_device_objects(device_id, data):
        return list(filter(
            lambda row: row.get(DEVICE_OBJ_INSTANCE_PARAMETER) == device_id and
            row.get(OBJECT_TYPE_PARAMETER) != DEVICE_OBJECT_TYPE_CODE,
            data
        ))
