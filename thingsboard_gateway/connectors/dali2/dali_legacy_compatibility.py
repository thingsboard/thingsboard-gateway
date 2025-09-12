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

from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass
from enum import Enum
from .entities.dali2_device import DALI2Command, DALI2CommandType, DALI2DeviceType


class DALIVersion(Enum):
    """DALI version types"""
    DALI_LEGACY = "dali_legacy"  # Original DALI (IEC 60929)
    DALI_2 = "dali_2"           # DALI-2 (IEC 62386)


class DALILegacyCommandType(Enum):
    """DALI Legacy command types (IEC 60929)"""
    # Basic commands (supported by all DALI devices)
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
    
    # Scene commands (basic DALI)
    SET_SCENE = "set_scene"
    QUERY_SCENE_LEVEL = "query_scene_level"
    
    # Group commands (basic DALI)
    ADD_TO_GROUP = "add_to_group"
    REMOVE_FROM_GROUP = "remove_from_group"
    QUERY_GROUPS_0_7 = "query_groups_0_7"
    
    # Address commands (basic DALI)
    SET_SHORT_ADDRESS = "set_short_address"
    
    # Configuration commands (basic DALI)
    SET_FADE_TIME = "set_fade_time"
    SET_FADE_RATE = "set_fade_rate"


@dataclass
class DALILegacyDeviceInfo:
    """DALI Legacy device information"""
    short_address: int
    device_type: DALI2DeviceType  # Use same enum for compatibility
    max_level: Optional[int] = None
    min_level: Optional[int] = None
    actual_level: Optional[int] = None
    power_on: bool = False
    ballast_failure: bool = False
    lamp_failure: bool = False
    power_failure: bool = False
    fade_running: bool = False
    groups: List[int] = None
    scenes: Dict[int, int] = None
    fade_time: Optional[int] = None
    fade_rate: Optional[int] = None
    dali_version: DALIVersion = DALIVersion.DALI_LEGACY
    compatibility_mode: bool = True

    def __post_init__(self):
        if self.groups is None:
            self.groups = []
        if self.scenes is None:
            self.scenes = {}


class DALILegacyCompatibility:
    """Handles compatibility between DALI Legacy and DALI-2 devices"""
    
    def __init__(self, logger):
        self.logger = logger
        self.legacy_commands = self._build_legacy_command_map()
        self.compatibility_matrix = self._build_compatibility_matrix()
        
    def _build_legacy_command_map(self) -> Dict[DALI2CommandType, DALILegacyCommandType]:
        """Map DALI-2 commands to DALI Legacy equivalents"""
        return {
            # Basic commands - fully compatible
            DALI2CommandType.DIRECT_ARC_POWER: DALILegacyCommandType.DIRECT_ARC_POWER,
            DALI2CommandType.QUERY_STATUS: DALILegacyCommandType.QUERY_STATUS,
            DALI2CommandType.QUERY_BALLAST_FAILURE: DALILegacyCommandType.QUERY_BALLAST_FAILURE,
            DALI2CommandType.QUERY_LAMP_FAILURE: DALILegacyCommandType.QUERY_LAMP_FAILURE,
            DALI2CommandType.QUERY_POWER_ON: DALILegacyCommandType.QUERY_POWER_ON,
            DALI2CommandType.QUERY_ACTUAL_LEVEL: DALILegacyCommandType.QUERY_ACTUAL_LEVEL,
            DALI2CommandType.QUERY_MAX_LEVEL: DALILegacyCommandType.QUERY_MAX_LEVEL,
            DALI2CommandType.QUERY_MIN_LEVEL: DALILegacyCommandType.QUERY_MIN_LEVEL,
            DALI2CommandType.QUERY_POWER_FAILURE: DALILegacyCommandType.QUERY_POWER_FAILURE,
            DALI2CommandType.QUERY_FADE_RUNNING: DALILegacyCommandType.QUERY_FADE_RUNNING,
            DALI2CommandType.QUERY_FADE_TIME: DALILegacyCommandType.QUERY_FADE_TIME,
            DALI2CommandType.QUERY_FADE_RATE: DALILegacyCommandType.QUERY_FADE_RATE,
            
            # Scene commands - compatible
            DALI2CommandType.SET_SCENE: DALILegacyCommandType.SET_SCENE,
            DALI2CommandType.QUERY_SCENE_LEVEL: DALILegacyCommandType.QUERY_SCENE_LEVEL,
            
            # Group commands - compatible
            DALI2CommandType.ADD_TO_GROUP: DALILegacyCommandType.ADD_TO_GROUP,
            DALI2CommandType.REMOVE_FROM_GROUP: DALILegacyCommandType.REMOVE_FROM_GROUP,
            DALI2CommandType.QUERY_GROUPS_0_7: DALILegacyCommandType.QUERY_GROUPS_0_7,
            
            # Address commands - compatible
            DALI2CommandType.SET_SHORT_ADDRESS: DALILegacyCommandType.SET_SHORT_ADDRESS,
            
            # Configuration commands - compatible
            DALI2CommandType.SET_FADE_TIME: DALILegacyCommandType.SET_FADE_TIME,
            DALI2CommandType.SET_FADE_RATE: DALILegacyCommandType.SET_FADE_RATE,
        }
        
    def _build_compatibility_matrix(self) -> Dict[DALI2CommandType, bool]:
        """Define which DALI-2 commands are compatible with DALI Legacy"""
        return {
            # Basic commands - fully compatible
            DALI2CommandType.DIRECT_ARC_POWER: True,
            DALI2CommandType.QUERY_STATUS: True,
            DALI2CommandType.QUERY_BALLAST_FAILURE: True,
            DALI2CommandType.QUERY_LAMP_FAILURE: True,
            DALI2CommandType.QUERY_POWER_ON: True,
            DALI2CommandType.QUERY_ACTUAL_LEVEL: True,
            DALI2CommandType.QUERY_MAX_LEVEL: True,
            DALI2CommandType.QUERY_MIN_LEVEL: True,
            DALI2CommandType.QUERY_POWER_FAILURE: True,
            DALI2CommandType.QUERY_FADE_RUNNING: True,
            DALI2CommandType.QUERY_FADE_TIME: True,
            DALI2CommandType.QUERY_FADE_RATE: True,
            
            # Scene commands - compatible
            DALI2CommandType.SET_SCENE: True,
            DALI2CommandType.QUERY_SCENE_LEVEL: True,
            DALI2CommandType.REMOVE_FROM_SCENE: True,
            
            # Group commands - compatible
            DALI2CommandType.ADD_TO_GROUP: True,
            DALI2CommandType.REMOVE_FROM_GROUP: True,
            DALI2CommandType.QUERY_GROUPS_0_7: True,
            DALI2CommandType.QUERY_GROUPS_8_15: False,  # DALI-2 only
            
            # Address commands - compatible
            DALI2CommandType.SET_SHORT_ADDRESS: True,
            
            # Configuration commands - compatible
            DALI2CommandType.SET_FADE_TIME: True,
            DALI2CommandType.SET_FADE_RATE: True,
            
            # Extended commands - DALI-2 only
            DALI2CommandType.QUERY_EXTENDED_FADE_TIME: False,
            DALI2CommandType.SET_EXTENDED_FADE_TIME: False,
            DALI2CommandType.QUERY_RANDOM_ADDRESS_H: False,
            DALI2CommandType.QUERY_RANDOM_ADDRESS_M: False,
            DALI2CommandType.QUERY_RANDOM_ADDRESS_L: False,
            DALI2CommandType.READ_MEMORY_LOCATION: False,
            DALI2CommandType.WRITE_MEMORY_LOCATION: False,
            DALI2CommandType.ENABLE_WRITE_MEMORY: False,
            DALI2CommandType.ENABLE_DEVICE_TYPE_X: False,
            DALI2CommandType.SET_OPERATING_MODE: False,
            
            # Individual scene queries - DALI-2 only
            DALI2CommandType.QUERY_SCENE_LEVEL_0: False,
            DALI2CommandType.QUERY_SCENE_LEVEL_1: False,
            DALI2CommandType.QUERY_SCENE_LEVEL_2: False,
            DALI2CommandType.QUERY_SCENE_LEVEL_3: False,
            DALI2CommandType.QUERY_SCENE_LEVEL_4: False,
            DALI2CommandType.QUERY_SCENE_LEVEL_5: False,
            DALI2CommandType.QUERY_SCENE_LEVEL_6: False,
            DALI2CommandType.QUERY_SCENE_LEVEL_7: False,
            DALI2CommandType.QUERY_SCENE_LEVEL_8: False,
            DALI2CommandType.QUERY_SCENE_LEVEL_9: False,
            DALI2CommandType.QUERY_SCENE_LEVEL_10: False,
            DALI2CommandType.QUERY_SCENE_LEVEL_11: False,
            DALI2CommandType.QUERY_SCENE_LEVEL_12: False,
            DALI2CommandType.QUERY_SCENE_LEVEL_13: False,
            DALI2CommandType.QUERY_SCENE_LEVEL_14: False,
            DALI2CommandType.QUERY_SCENE_LEVEL_15: False,
        }
        
    def is_command_compatible(self, command_type: DALI2CommandType, dali_version: DALIVersion) -> bool:
        """Check if a DALI-2 command is compatible with the device version"""
        if dali_version == DALIVersion.DALI_2:
            return True  # All commands supported
            
        return self.compatibility_matrix.get(command_type, False)
        
    def convert_command_for_legacy(self, command: DALI2Command, dali_version: DALIVersion) -> Optional[DALI2Command]:
        """Convert DALI-2 command to legacy compatible command"""
        if dali_version == DALIVersion.DALI_2:
            return command  # No conversion needed
            
        if not self.is_command_compatible(command.command_type, dali_version):
            self.logger.warning(f"Command {command.command_type.value} not supported by DALI Legacy device")
            return None
            
        # For legacy devices, we might need to modify the command
        legacy_command = DALI2Command(
            command_type=command.command_type,
            address=command.address,
            data=command.data,
            response_expected=command.response_expected,
            timeout_ms=command.timeout_ms
        )
        
        return legacy_command
        
    def detect_dali_version(self, device_responses: Dict[str, Any]) -> DALIVersion:
        """Detect DALI version based on device responses"""
        # Check for DALI-2 specific features
        if device_responses.get('random_address') is not None:
            return DALIVersion.DALI_2
            
        if device_responses.get('extended_fade_time') is not None:
            return DALI2CommandType.QUERY_EXTENDED_FADE_TIME
            
        if device_responses.get('groups_8_15') is not None:
            return DALIVersion.DALI_2
            
        # Check for memory location support
        if device_responses.get('memory_location') is not None:
            return DALIVersion.DALI_2
            
        # Default to legacy if no DALI-2 features detected
        return DALIVersion.DALI_LEGACY
        
    def get_legacy_compatible_commands(self, dali_version: DALIVersion) -> List[DALI2CommandType]:
        """Get list of commands compatible with the device version"""
        if dali_version == DALIVersion.DALI_2:
            return list(DALI2CommandType)
            
        return [cmd for cmd, compatible in self.compatibility_matrix.items() if compatible]
        
    def create_legacy_device_info(self, address: int, responses: Dict[str, Any]) -> DALILegacyDeviceInfo:
        """Create device info for legacy DALI device"""
        device_info = DALILegacyDeviceInfo(
            short_address=address,
            device_type=DALI2DeviceType.BALLAST,  # Default for legacy
            dali_version=DALIVersion.DALI_LEGACY,
            compatibility_mode=True
        )
        
        # Parse basic responses
        if 'actual_level' in responses:
            device_info.actual_level = responses['actual_level']
            
        if 'max_level' in responses:
            device_info.max_level = responses['max_level']
            
        if 'min_level' in responses:
            device_info.min_level = responses['min_level']
            
        if 'status' in responses:
            status = responses['status']
            device_info.power_on = bool(status & 0x01)
            device_info.ballast_failure = bool(status & 0x02)
            device_info.lamp_failure = bool(status & 0x04)
            device_info.power_failure = bool(status & 0x08)
            device_info.fade_running = bool(status & 0x10)
            
        if 'groups' in responses:
            device_info.groups = responses['groups']
            
        if 'scenes' in responses:
            device_info.scenes = responses['scenes']
            
        if 'fade_time' in responses:
            device_info.fade_time = responses['fade_time']
            
        if 'fade_rate' in responses:
            device_info.fade_rate = responses['fade_rate']
            
        return device_info
        
    def get_legacy_discovery_commands(self) -> List[DALI2CommandType]:
        """Get commands to use for legacy device discovery"""
        return [
            DALI2CommandType.QUERY_STATUS,
            DALI2CommandType.QUERY_ACTUAL_LEVEL,
            DALI2CommandType.QUERY_MAX_LEVEL,
            DALI2CommandType.QUERY_MIN_LEVEL,
            DALI2CommandType.QUERY_GROUPS_0_7,
            DALI2CommandType.QUERY_FADE_TIME,
            DALI2CommandType.QUERY_FADE_RATE
        ]
        
    def get_legacy_telemetry_commands(self) -> List[DALI2CommandType]:
        """Get commands to use for legacy device telemetry"""
        return [
            DALI2CommandType.QUERY_STATUS,
            DALI2CommandType.QUERY_ACTUAL_LEVEL,
            DALI2CommandType.QUERY_POWER_ON,
            DALI2CommandType.QUERY_FADE_RUNNING
        ]
        
    def get_legacy_attribute_commands(self) -> List[DALI2CommandType]:
        """Get commands to use for legacy device attributes"""
        return [
            DALI2CommandType.QUERY_MAX_LEVEL,
            DALI2CommandType.QUERY_MIN_LEVEL,
            DALI2CommandType.QUERY_GROUPS_0_7,
            DALI2CommandType.QUERY_FADE_TIME,
            DALI2CommandType.QUERY_FADE_RATE
        ]
        
    def get_legacy_rpc_commands(self) -> List[DALI2CommandType]:
        """Get commands to use for legacy device RPC"""
        return [
            DALI2CommandType.DIRECT_ARC_POWER,
            DALI2CommandType.SET_SCENE,
            DALI2CommandType.ADD_TO_GROUP,
            DALI2CommandType.REMOVE_FROM_GROUP,
            DALI2CommandType.SET_FADE_TIME,
            DALI2CommandType.SET_FADE_RATE
        ]
        
    def log_compatibility_info(self, device_address: int, dali_version: DALIVersion):
        """Log compatibility information for a device"""
        compatible_commands = self.get_legacy_compatible_commands(dali_version)
        
        self.logger.info(f"Device {device_address} - DALI Version: {dali_version.value}")
        self.logger.info(f"Device {device_address} - Compatible commands: {len(compatible_commands)}")
        
        if dali_version == DALIVersion.DALI_LEGACY:
            self.logger.info(f"Device {device_address} - Running in legacy compatibility mode")
            self.logger.info(f"Device {device_address} - Advanced features not available")
        else:
            self.logger.info(f"Device {device_address} - Full DALI-2 feature set available")
