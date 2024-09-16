#      Copyright 2024. ThingsBoard
#  #
#      Licensed under the Apache License, Version 2.0 (the "License");
#      you may not use this file except in compliance with the License.
#      You may obtain a copy of the License at
#  #
#          http://www.apache.org/licenses/LICENSE-2.0
#  #
#      Unless required by applicable law or agreed to in writing, software
#      distributed under the License is distributed on an "AS IS" BASIS,
#      WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#      See the License for the specific language governing permissions and
#      limitations under the License.

class Attributes:
    def __init__(self, values=None):
        self.values: dict = values or {}

    def __str__(self):
        return f"Attributes(values={self.values})"

    def __hash__(self):
        return hash(tuple(self.values.items()))

    def to_dict(self):
        return self.values

    def __getitem__(self, item):
        return self.values[item]

    def __setitem__(self, key, value):
        self.values[key] = value

    def __len__(self):
        return len(self.values)

    def update(self, attributes):
        self.values.update(attributes if isinstance(attributes, dict) else attributes.values)