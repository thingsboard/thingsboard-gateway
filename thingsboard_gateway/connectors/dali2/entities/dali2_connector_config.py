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


@dataclass
class DALI2BusConfig:
    """DALI-2 bus configuration"""
    bus_id: str
    interface_type: str  # "serial", "tcp", "udp", "usb"
    connection_params: Dict[str, Any]
    poll_interval_ms: int = 5000
    command_timeout_ms: int = 1000
    max_retries: int = 3
    enable_discovery: bool = True
    discovery_interval_ms: int = 30000
    enable_continuous_polling: bool = True
    enable_event_reporting: bool = True


@dataclass
class DALI2DeviceConfig:
    """DALI-2 device configuration"""
    device_name: str
    short_address: int
    device_type: str = "dali2_device"
    bus_id: str = "default"
    poll_interval_ms: Optional[int] = None
    enable_telemetry: bool = True
    enable_attributes: bool = True
    enable_rpc: bool = True
    telemetry_keys: List[str] = None
    attribute_keys: List[str] = None
    rpc_methods: List[str] = None
    scenes: Dict[int, int] = None  # scene_number -> level
    groups: List[int] = None
    custom_attributes: Dict[str, Any] = None

    def __post_init__(self):
        if self.telemetry_keys is None:
            self.telemetry_keys = [
                'actual_level', 'max_level', 'min_level', 'power_on',
                'ballast_failure', 'lamp_failure', 'power_failure',
                'fade_running', 'fade_time', 'fade_rate', 'operating_mode'
            ]
        if self.attribute_keys is None:
            self.attribute_keys = [
                'short_address', 'device_type', 'manufacturer_id',
                'product_id', 'application_version', 'hardware_version',
                'firmware_version', 'groups', 'scenes_count'
            ]
        if self.rpc_methods is None:
            self.rpc_methods = [
                'set_level', 'set_scene', 'add_to_group', 'remove_from_group',
                'set_fade_time', 'set_fade_rate', 'query_status'
            ]
        if self.scenes is None:
            self.scenes = {}
        if self.groups is None:
            self.groups = []
        if self.custom_attributes is None:
            self.custom_attributes = {}


@dataclass
class DALI2ConnectorConfig:
    """Main DALI-2 connector configuration"""
    name: str
    log_level: str = "INFO"
    enable_remote_logging: bool = False
    buses: List[DALI2BusConfig] = None
    devices: List[DALI2DeviceConfig] = None
    global_settings: Dict[str, Any] = None

    def __post_init__(self):
        if self.buses is None:
            self.buses = []
        if self.devices is None:
            self.devices = []
        if self.global_settings is None:
            self.global_settings = {
                'default_poll_interval_ms': 5000,
                'default_command_timeout_ms': 1000,
                'default_max_retries': 3,
                'enable_auto_discovery': True,
                'discovery_interval_ms': 30000,
                'enable_continuous_polling': True,
                'enable_event_reporting': True,
                'max_devices_per_bus': 64,
                'enable_group_control': True,
                'enable_scene_control': True
            }

    def get_bus_config(self, bus_id: str) -> Optional[DALI2BusConfig]:
        """Get bus configuration by ID"""
        for bus in self.buses:
            if bus.bus_id == bus_id:
                return bus
        return None

    def get_device_config(self, device_name: str) -> Optional[DALI2DeviceConfig]:
        """Get device configuration by name"""
        for device in self.devices:
            if device.device_name == device_name:
                return device
        return None

    def get_devices_by_bus(self, bus_id: str) -> List[DALI2DeviceConfig]:
        """Get all devices for a specific bus"""
        return [device for device in self.devices if device.bus_id == bus_id]
