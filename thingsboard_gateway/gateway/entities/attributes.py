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

from typing import Dict, Any, Union

from thingsboard_gateway.gateway.entities.datapoint_key import DatapointKey


class Attributes:
    def __init__(self, values: Dict[DatapointKey, Any] = None):
        self.values: Dict[DatapointKey, Any] = values or {}

    def __str__(self):
        return f"Attributes(values={self.values})"

    def __hash__(self):
        return hash(tuple(self.values.items()))

    def to_dict(self) -> Dict[str, Any]:
        return {key.key if isinstance(key, DatapointKey) else key: value for key, value in self.values.items()}

    def __getitem__(self, item: DatapointKey) -> Any:
        return self.values[item]

    def __iter__(self):
        return iter(self.values)

    def __setitem__(self, key: DatapointKey, value):
        self.values[key] = value

    def __len__(self):
        return len(self.values)

    def update(self, attributes: Union[Dict[DatapointKey, Any], 'Attributes']):
        self.values.update(attributes if isinstance(attributes, dict) else attributes.values)

    def items(self):
        return self.values.items()
