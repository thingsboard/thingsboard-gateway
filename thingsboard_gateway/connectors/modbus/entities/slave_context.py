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

from pymodbus.datastore.context import ModbusSlaveContext
from pymodbus.datastore.store import ModbusSequentialDataBlock


class SlaveContext(ModbusSlaveContext):
    """
    Monkey-patched version of ModbusSlaveContext to allow initialization
    with custom data blocks for each type of data.
    Can be removed when the issue with ModbusSlaveContext is fixed in pymodbus.
    """

    def __init__(self, *_args, di=None, co=None, ir=None, hr=None):
        self.store = {}
        self.store["d"] = di if di is not None else ModbusSequentialDataBlock.create()
        self.store["c"] = co if co is not None else ModbusSequentialDataBlock.create()
        self.store["i"] = ir if ir is not None else ModbusSequentialDataBlock.create()
        self.store["h"] = hr if hr is not None else ModbusSequentialDataBlock.create()
