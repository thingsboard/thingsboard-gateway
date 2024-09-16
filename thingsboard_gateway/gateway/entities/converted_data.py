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

from typing import List, Union

from thingsboard_gateway.gateway.constants import ATTRIBUTES_PARAMETER, TELEMETRY_PARAMETER, TIMESERIES_PARAMETER, \
    METADATA_PARAMETER
from thingsboard_gateway.gateway.entities.attributes import Attributes
from thingsboard_gateway.gateway.entities.telemetry_entry import TelemetryEntry
from thingsboard_gateway.tb_utility.tb_utility import TBUtility


def split_large_entries(entries: dict, first_item_max_data_size: int, max_data_size: int, ts=None, ts_size=None):
    split_chunks = []
    split_chunk_sizes = []
    current_chunk = {}
    current_size = 0
    ts_check = False

    for key, value in entries.items():
        entry_size = TBUtility.get_data_size({key: value}) + 1
        if ts is not None and not ts_check:
            entry_size += ts_size
            ts_check = True

        # Recalculate size after each addition and check if it exceeds the max size
        if current_size + entry_size >= (first_item_max_data_size if not split_chunks else max_data_size):
            # Append current chunk if it exceeds the max size
            if current_chunk:
                split_chunks.append(current_chunk)
                split_chunk_sizes.append(current_size)
                ts_check = False
            # Start a new chunk
            current_chunk = {key: value} # New dict is created to avoid modifying the original dict
            current_size = entry_size
        else:
            # Add to current chunk
            current_chunk[key] = value
            current_size += entry_size

    # Add the last chunk if any
    if current_chunk:
        split_chunks.append(current_chunk)
        split_chunk_sizes.append(current_size)

    return zip(split_chunks, split_chunk_sizes)


class ConvertedData:
    def __init__(self, device_name, device_type='default', metadata=None):
        self.device_name = device_name
        self.device_type = device_type
        self.telemetry: List[TelemetryEntry] = []
        self.attributes: Attributes = Attributes()
        self._telemetry_datapoints_count = 0
        self.metadata = metadata or {}
        self.ts_index = {}

    def __str__(self):
        return f"ConvertedData(deviceName={self.device_name}, deviceType={self.device_type}, " \
               f"telemetry={self.telemetry}, attributes={self.attributes}, metadata={self.metadata})"

    def __repr__(self):
        return self.__str__()

    def to_dict(self):
        return {
            "deviceName": self.device_name,
            "deviceType": self.device_type,
            "telemetry": [telemetry_entry.to_dict() for telemetry_entry in self.telemetry],
            "attributes": self.attributes.to_dict(),
            "metadata": self.metadata
        }

    def __getitem__(self, item):
        if item == ATTRIBUTES_PARAMETER:
            return self.attributes
        elif item in {TELEMETRY_PARAMETER, TIMESERIES_PARAMETER}:
            return self.telemetry
        elif item == METADATA_PARAMETER:
            return self.metadata
        else:
            raise KeyError(f"{item} - Item not found!")

    def add_to_telemetry(self, telemetry_entry: Union[dict, TelemetryEntry, List[TelemetryEntry]]):
        if isinstance(telemetry_entry, list):
            for entry in telemetry_entry:
                self._add_single_telemetry_entry(entry)
        else:
            self._add_single_telemetry_entry(telemetry_entry)

    def _add_single_telemetry_entry(self, telemetry_entry: Union[dict, TelemetryEntry]):
        if isinstance(telemetry_entry, dict):
            telemetry_entry = TelemetryEntry(telemetry_entry)

        if telemetry_entry.ts in self.ts_index:
            index = self.ts_index[telemetry_entry.ts]
            existing_values = self.telemetry[index].values

            new_keys = telemetry_entry.values.keys() - existing_values.keys()
            new_values = {key: telemetry_entry.values[key] for key in new_keys}

            self._telemetry_datapoints_count += len(new_values)
            existing_values.update(new_values)
        else:
            self.telemetry.append(telemetry_entry)
            self._telemetry_datapoints_count += len(telemetry_entry.values)
            self.ts_index[telemetry_entry.ts] = len(self.telemetry) - 1

    def add_to_attributes(self, key_or_entry: Union[dict, str, List[dict]], value: str = None):
        if isinstance(key_or_entry, list):
            for entry in key_or_entry:
                if not isinstance(entry, dict):
                    raise ValueError("Batch attribute processing requires a list of dictionaries.")
                self.attributes.update(entry)
        else:
            if isinstance(key_or_entry, str):
                if value is None:
                    raise ValueError("Invalid arguments for add_attribute")
                key_or_entry = {key_or_entry: value}

            self.attributes.update(key_or_entry)

    def add_to_metadata(self, key_value_entry: dict):
        self.metadata.update(key_value_entry)

    def get_size(self):
        return TBUtility.get_data_size(self.to_dict())

    @property
    def telemetry_datapoints_count(self):
        return self._telemetry_datapoints_count

    @property
    def attributes_datapoints_count(self):
        return len(self.attributes)

    # Methods for getting data
    def convert_to_objects_with_maximal_size(self, max_data_size) -> List['ConvertedData']:
        general_info_bytes_size = TBUtility.get_data_size({
            "deviceName": self.device_name,
            "deviceType": self.device_type,
            "metadata": self.metadata,
            "telemetry": [],
            "attributes": {}
        })

        if general_info_bytes_size > max_data_size:
            raise ValueError("Maximal data size is too small even for general info, please adjust maxPayloadSize")

        available_data_size = max_data_size - general_info_bytes_size

        converted_objects = []
        current_data = ConvertedData(self.device_name, self.device_type, self.metadata)
        current_data_size = general_info_bytes_size

        attributes_dict = self.attributes.to_dict()
        if attributes_dict:
            attributes_bytes_size = TBUtility.get_data_size(attributes_dict)
            if current_data_size + attributes_bytes_size <= max_data_size:
                current_data.add_to_attributes(attributes_dict)
                current_data_size += attributes_bytes_size
            else:
                split_attributes_and_sizes = split_large_entries(attributes_dict,
                                                                      max_data_size - current_data_size,
                                                                      available_data_size)
                for data_chunk, chunk_size in split_attributes_and_sizes:
                    if current_data_size + chunk_size >= max_data_size:
                        converted_objects.append(current_data)
                        current_data = ConvertedData(self.device_name, self.device_type, self.metadata)
                        current_data_size = general_info_bytes_size + chunk_size
                    current_data.add_to_attributes(data_chunk)
                    current_data_size += chunk_size

        for telemetry_entry in self.telemetry:
            telemetry_values = telemetry_entry.values
            ts_data_size = TBUtility.get_data_size({"ts": telemetry_entry.ts}) + 1

            telemetry_obj_size = TBUtility.get_data_size(telemetry_values) + ts_data_size

            if telemetry_obj_size <= max_data_size - current_data_size:
                current_data.add_to_telemetry(telemetry_entry)
                current_data_size += telemetry_obj_size
            else:
                split_telemetry_and_sizes = split_large_entries(telemetry_values,
                                                                     max_data_size - current_data_size,
                                                                     available_data_size,
                                                                     telemetry_entry.ts,
                                                                     ts_data_size)
                for telemetry_chunk, chunk_size in split_telemetry_and_sizes:

                    if current_data_size + chunk_size > max_data_size:
                        converted_objects.append(current_data)
                        current_data = ConvertedData(self.device_name, self.device_type, self.metadata)
                        current_data_size = general_info_bytes_size
                    current_data_size += chunk_size
                    if telemetry_entry.ts in current_data.ts_index:
                        current_data_size += ts_data_size
                    current_data.add_to_telemetry(TelemetryEntry({"ts": telemetry_entry.ts, "values": telemetry_chunk}))

        if current_data_size > general_info_bytes_size:
            converted_objects.append(current_data)

        return converted_objects


if __name__ == '__main__':
    data = ConvertedData("device_name", "device_type")
    data.add_to_attributes({"key1": "value1", "key2": "value2"})
    data.add_to_telemetry({"ts": 123456, "values": {"key1": "value1", "key2": "value2"}})
    data.add_to_telemetry({"ts": 123457, "values": {"key3": "value3", "key4": "value4"}})
    data.add_to_telemetry({"ts": 123457, "values": {"key5": "value5", "key4": "value4"}})
    data.add_to_telemetry({"ts": 123457, "values": {"key7": "value5", "key5": "value7"}})
    data.add_to_telemetry({"ts": 123457, "values": {"key6": "value5", "key5": "value7"}})
    print(data.telemetry_datapoints_count)
    print([telemetry_entry.values for telemetry_entry in data.telemetry])