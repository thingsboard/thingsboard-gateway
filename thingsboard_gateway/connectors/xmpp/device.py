from dataclasses import dataclass
from typing import List


@dataclass
class Device:
    jid: str
    device_name_expression: str
    device_type_expression: str
    attributes: List
    timeseries: List
    attribute_updates: List
    server_side_rpc: List
    converter = None

    def set_converter(self, converter):
        self.converter = converter
