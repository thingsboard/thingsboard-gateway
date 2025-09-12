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

from dataclasses import dataclass
from typing import Dict, List, Optional, Any
from enum import Enum


class DALI2DeviceType(Enum):
    """DALI-2 device types according to IEC 62386"""
    BALLAST = "ballast"
    LED_DRIVER = "led_driver"
    CONTROL_GEAR = "control_gear"
    CONTROL_DEVICE = "control_device"
    APPLICATION_CONTROLLER = "application_controller"
    INPUT_DEVICE = "input_device"
    SENSOR = "sensor"
    PUSH_BUTTON = "push_button"
    SLIDER = "slider"
    UNKNOWN = "unknown"


class DALI2CommandType(Enum):
    """DALI-2 command types"""
    DIRECT_ARC_POWER = "direct_arc_power"
    QUERY_STATUS = "query_status"
    QUERY_BALLAST_FAILURE = "query_ballast_failure"
    QUERY_LAMP_FAILURE = "query_lamp_failure"
    QUERY_POWER_ON = "query_power_on"
    QUERY_ACTUAL_LEVEL = "query_actual_level"
    QUERY_MAX_LEVEL = "query_max_level"
    QUERY_MIN_LEVEL = "query_min_level"
    QUERY_POWER_FAILURE = "query_power_failure"
    QUERY_FADE_RUNNING = "query_fade_running"
    QUERY_FADE_TIME = "query_fade_time"
    QUERY_FADE_RATE = "query_fade_rate"
    QUERY_EXTENDED_FADE_TIME = "query_extended_fade_time"
    QUERY_SCENE_LEVEL = "query_scene_level"
    QUERY_GROUPS_0_7 = "query_groups_0_7"
    QUERY_GROUPS_8_15 = "query_groups_8_15"
    QUERY_RANDOM_ADDRESS_H = "query_random_address_h"
    QUERY_RANDOM_ADDRESS_M = "query_random_address_m"
    QUERY_RANDOM_ADDRESS_L = "query_random_address_l"
    READ_MEMORY_LOCATION = "read_memory_location"
    WRITE_MEMORY_LOCATION = "write_memory_location"
    ENABLE_WRITE_MEMORY = "enable_write_memory"
    SET_SHORT_ADDRESS = "set_short_address"
    ENABLE_DEVICE_TYPE_X = "enable_device_type_x"
    SET_OPERATING_MODE = "set_operating_mode"
    SET_FADE_TIME = "set_fade_time"
    SET_FADE_RATE = "set_fade_rate"
    SET_EXTENDED_FADE_TIME = "set_extended_fade_time"
    SET_SCENE = "set_scene"
    REMOVE_FROM_SCENE = "remove_from_scene"
    ADD_TO_GROUP = "add_to_group"
    REMOVE_FROM_GROUP = "remove_from_group"
    QUERY_SCENE_LEVEL_0 = "query_scene_level_0"
    QUERY_SCENE_LEVEL_1 = "query_scene_level_1"
    QUERY_SCENE_LEVEL_2 = "query_scene_level_2"
    QUERY_SCENE_LEVEL_3 = "query_scene_level_3"
    QUERY_SCENE_LEVEL_4 = "query_scene_level_4"
    QUERY_SCENE_LEVEL_5 = "query_scene_level_5"
    QUERY_SCENE_LEVEL_6 = "query_scene_level_6"
    QUERY_SCENE_LEVEL_7 = "query_scene_level_7"
    QUERY_SCENE_LEVEL_8 = "query_scene_level_8"
    QUERY_SCENE_LEVEL_9 = "query_scene_level_9"
    QUERY_SCENE_LEVEL_10 = "query_scene_level_10"
    QUERY_SCENE_LEVEL_11 = "query_scene_level_11"
    QUERY_SCENE_LEVEL_12 = "query_scene_level_12"
    QUERY_SCENE_LEVEL_13 = "query_scene_level_13"
    QUERY_SCENE_LEVEL_14 = "query_scene_level_14"
    QUERY_SCENE_LEVEL_15 = "query_scene_level_15"


@dataclass
class DALI2Command:
    """Represents a DALI-2 command"""
    command_type: DALI2CommandType
    address: int
    data: Optional[int] = None
    response_expected: bool = True
    timeout_ms: int = 1000


@dataclass
class DALI2DeviceInfo:
    """DALI-2 device information"""
    short_address: int
    device_type: DALI2DeviceType
    manufacturer_id: Optional[int] = None
    product_id: Optional[int] = None
    application_version: Optional[int] = None
    hardware_version: Optional[int] = None
    firmware_version: Optional[int] = None
    random_address: Optional[int] = None
    groups: List[int] = None
    scenes: Dict[int, int] = None  # scene_number -> level
    max_level: Optional[int] = None
    min_level: Optional[int] = None
    actual_level: Optional[int] = None
    power_on: bool = False
    ballast_failure: bool = False
    lamp_failure: bool = False
    power_failure: bool = False
    fade_running: bool = False
    fade_time: Optional[int] = None
    fade_rate: Optional[int] = None
    operating_mode: Optional[int] = None

    def __post_init__(self):
        if self.groups is None:
            self.groups = []
        if self.scenes is None:
            self.scenes = {}


@dataclass
class DALI2Device:
    """Represents a DALI-2 device"""
    device_info: DALI2DeviceInfo
    name: str
    device_type: str = "dali2_device"
    last_seen: Optional[float] = None
    online: bool = False
    configuration: Dict[str, Any] = None

    def __post_init__(self):
        if self.configuration is None:
            self.configuration = {}

    def update_status(self, status_data: Dict[str, Any]):
        """Update device status from received data"""
        if 'actual_level' in status_data:
            self.device_info.actual_level = status_data['actual_level']
        if 'power_on' in status_data:
            self.device_info.power_on = status_data['power_on']
        if 'ballast_failure' in status_data:
            self.device_info.ballast_failure = status_data['ballast_failure']
        if 'lamp_failure' in status_data:
            self.device_info.lamp_failure = status_data['lamp_failure']
        if 'power_failure' in status_data:
            self.device_info.power_failure = status_data['power_failure']
        if 'fade_running' in status_data:
            self.device_info.fade_running = status_data['fade_running']

    def get_telemetry_data(self) -> Dict[str, Any]:
        """Get current telemetry data for ThingsBoard"""
        return {
            'actual_level': self.device_info.actual_level or 0,
            'max_level': self.device_info.max_level or 254,
            'min_level': self.device_info.min_level or 1,
            'power_on': self.device_info.power_on,
            'ballast_failure': self.device_info.ballast_failure,
            'lamp_failure': self.device_info.lamp_failure,
            'power_failure': self.device_info.power_failure,
            'fade_running': self.device_info.fade_running,
            'fade_time': self.device_info.fade_time or 0,
            'fade_rate': self.device_info.fade_rate or 0,
            'operating_mode': self.device_info.operating_mode or 0,
            'online': self.online
        }

    def get_attributes_data(self) -> Dict[str, Any]:
        """Get current attributes data for ThingsBoard"""
        return {
            'short_address': self.device_info.short_address,
            'device_type': self.device_info.device_type.value,
            'manufacturer_id': self.device_info.manufacturer_id or 0,
            'product_id': self.device_info.product_id or 0,
            'application_version': self.device_info.application_version or 0,
            'hardware_version': self.device_info.hardware_version or 0,
            'firmware_version': self.device_info.firmware_version or 0,
            'groups': ','.join(map(str, self.device_info.groups)) if self.device_info.groups else '',
            'scenes_count': len(self.device_info.scenes) if self.device_info.scenes else 0
        }
