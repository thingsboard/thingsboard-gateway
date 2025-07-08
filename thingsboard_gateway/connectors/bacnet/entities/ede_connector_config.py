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

from thingsboard_gateway.connectors.bacnet.constants import (
    DEFAULT_POLL_PERIOD,
    OBJECT_INSTANCE_PARAMETER,
    OBJECT_NAME_PARAMETER,
    OBJECT_TYPE_PARAMETER,
    SUPPORTED_OBJECTS_TYPES
)


class EDEConnectorConfig:
    def __init__(self):
        self._config = {
            "devices": []
        }

    @property
    def config(self):
        return self._config

    def add_device(self, device_id, device_name, device_objects):
        timeseries_config = self._get_device_objects(device_objects)

        self._config['devices'].append({
            "deviceInfo": {
                "deviceNameExpression": f"{device_name}",
                "deviceProfileExpression": "default",
                "deviceNameExpressionSource": "constant",
                "deviceProfileExpressionSource": "constant"
            },
            "deviceId": device_id,
            "host": "*",
            "port": "*",
            "mask": "*",
            "pollPeriod": DEFAULT_POLL_PERIOD,
            "timeseries": timeseries_config
        })

    @staticmethod
    def _get_device_objects(device_objects):
        device_objects_config = []

        for device_obj in device_objects:
            try:
                device_objects_config.append({
                    "key": device_obj.get(OBJECT_NAME_PARAMETER, 'Unknown Object'),
                    "objectType": SUPPORTED_OBJECTS_TYPES[device_obj[OBJECT_TYPE_PARAMETER]],
                    "objectId": device_obj[OBJECT_INSTANCE_PARAMETER],
                    "propertyId": "presentValue"
                })
            except KeyError:
                continue

        return device_objects_config
