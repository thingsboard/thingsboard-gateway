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

import asyncio
import time
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass
from .entities.dali2_device import DALI2DeviceInfo, DALI2DeviceType, DALI2Command, DALI2CommandType
from .dali_legacy_compatibility import DALILegacyCompatibility, DALIVersion, DALILegacyDeviceInfo


@dataclass
class DALI2DeviceDiscovery:
    """Information about a discovered DALI-2 device"""
    short_address: int
    device_type: DALI2DeviceType
    manufacturer_id: Optional[int] = None
    product_id: Optional[int] = None
    application_version: Optional[int] = None
    hardware_version: Optional[int] = None
    firmware_version: Optional[int] = None
    random_address: Optional[int] = None
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
    response_time_ms: Optional[float] = None
    last_seen: Optional[float] = None
    discovery_confidence: float = 0.0
    dali_version: DALIVersion = DALIVersion.DALI_2
    compatibility_mode: bool = False

    def __post_init__(self):
        if self.groups is None:
            self.groups = []
        if self.scenes is None:
            self.scenes = {}


class DALI2BusMapper:
    """Advanced DALI-2 bus mapping and device discovery"""
    
    def __init__(self, bus_interface, logger):
        self.bus_interface = bus_interface
        self.logger = logger
        self.discovered_devices: Dict[int, DALI2DeviceDiscovery] = {}
        self.bus_map: Dict[str, Any] = {}
        self.legacy_compatibility = DALILegacyCompatibility(logger)
        
    async def map_entire_bus(self, detailed_scan: bool = True) -> Dict[int, DALI2DeviceDiscovery]:
        """
        Map the entire DALI-2 bus and identify all connected devices
        
        Args:
            detailed_scan: If True, performs detailed device interrogation
            
        Returns:
            Dictionary of discovered devices by address
        """
        self.logger.info("Starting comprehensive DALI-2 bus mapping...")
        
        # Step 1: Quick discovery scan
        await self._quick_discovery_scan()
        
                # Step 2: Detect DALI version for each device
        await self._detect_dali_versions()
        
        # Step 3: Detailed device interrogation (if requested)
        if detailed_scan:
            await self._detailed_device_interrogation()
            
        # Step 4: Build bus topology
        await self._build_bus_topology()
        
        # Step 5: Generate bus map report
        self._generate_bus_map_report()
        
        self.logger.info(f"Bus mapping completed. Found {len(self.discovered_devices)} devices")
        return self.discovered_devices
        
    async def _quick_discovery_scan(self):
        """Perform quick discovery scan of all possible addresses"""
        self.logger.info("Performing quick discovery scan...")
        
        # Scan all possible short addresses (0-63)
        for address in range(64):
            try:
                start_time = time.time()
                
                # Send QUERY STATUS command
                command = DALI2Command(
                    command_type=DALI2CommandType.QUERY_STATUS,
                    address=address,
                    response_expected=True,
                    timeout_ms=100  # Quick timeout for discovery
                )
                
                response = await self.bus_interface.send_command(command)
                response_time = (time.time() - start_time) * 1000
                
                if response is not None and response != 0xFFFF:
                    # Device found - create discovery record
                    device_discovery = DALI2DeviceDiscovery(
                        short_address=address,
                        device_type=DALI2DeviceType.UNKNOWN,
                        response_time_ms=response_time,
                        last_seen=time.time(),
                        discovery_confidence=1.0
                    )
                    
                    # Parse basic status
                    self._parse_status_response(device_discovery, response)
                    
                    self.discovered_devices[address] = device_discovery
                    self.logger.debug(f"Discovered device at address {address} (response time: {response_time:.1f}ms)")
                    
            except Exception as e:
                self.logger.debug(f"Error scanning address {address}: {e}")
                continue
                
        self.logger.info(f"Quick scan completed. Found {len(self.discovered_devices)} devices")
        
    async def _detect_dali_versions(self):
        """Detect DALI version for each discovered device"""
        self.logger.info("Detecting DALI versions for discovered devices...")
        
        for address, device in self.discovered_devices.items():
            try:
                # Try DALI-2 specific commands to detect version
                dali_version = await self._test_dali_version(address)
                device.dali_version = dali_version
                device.compatibility_mode = (dali_version == DALIVersion.DALI_LEGACY)
                
                self.logger.debug(f"Device {address} - DALI Version: {dali_version.value}")
                
            except Exception as e:
                self.logger.error(f"Error detecting DALI version for device {address}: {e}")
                device.dali_version = DALIVersion.DALI_LEGACY  # Default to legacy
                device.compatibility_mode = True
                
    async def _test_dali_version(self, address: int) -> DALIVersion:
        """Test device to determine DALI version"""
        try:
            # Test for DALI-2 specific features
            
            # Test 1: Try to query groups 8-15 (DALI-2 only)
            command = DALI2Command(
                command_type=DALI2CommandType.QUERY_GROUPS_8_15,
                address=address,
                response_expected=True,
                timeout_ms=100
            )
            response = await self.bus_interface.send_command(command)
            if response is not None and response != 0xFFFF:
                return DALIVersion.DALI_2
                
            # Test 2: Try to query extended fade time (DALI-2 only)
            command = DALI2Command(
                command_type=DALI2CommandType.QUERY_EXTENDED_FADE_TIME,
                address=address,
                response_expected=True,
                timeout_ms=100
            )
            response = await self.bus_interface.send_command(command)
            if response is not None and response != 0xFFFF:
                return DALIVersion.DALI_2
                
            # Test 3: Try to query random address (DALI-2 only)
            command = DALI2Command(
                command_type=DALI2CommandType.QUERY_RANDOM_ADDRESS_H,
                address=address,
                response_expected=True,
                timeout_ms=100
            )
            response = await self.bus_interface.send_command(command)
            if response is not None and response != 0xFFFF:
                return DALIVersion.DALI_2
                
            # If none of the DALI-2 specific commands work, it's likely legacy
            return DALIVersion.DALI_LEGACY
            
        except Exception as e:
            self.logger.debug(f"Error testing DALI version for device {address}: {e}")
            return DALIVersion.DALI_LEGACY
            
    async def _detailed_device_interrogation(self):
        """Perform detailed interrogation of discovered devices"""
        self.logger.info("Performing detailed device interrogation...")
        
        for address, device in self.discovered_devices.items():
            try:
                self.logger.debug(f"Interrogating device at address {address}...")
                
                # Query device type and capabilities
                await self._query_device_capabilities(device)
                
                # Query device information
                await self._query_device_information(device)
                
                # Query configuration
                await self._query_device_configuration(device)
                
                # Query scenes
                await self._query_device_scenes(device)
                
                # Query groups
                await self._query_device_groups(device)
                
                # Update confidence score
                device.discovery_confidence = self._calculate_discovery_confidence(device)
                
                self.logger.debug(f"Device {address} interrogation completed (confidence: {device.discovery_confidence:.2f})")
                
            except Exception as e:
                self.logger.error(f"Error interrogating device {address}: {e}")
                device.discovery_confidence = 0.5  # Reduce confidence on error
                
    async def _query_device_capabilities(self, device: DALI2DeviceDiscovery):
        """Query device capabilities and levels"""
        try:
            # Query actual level
            command = DALI2Command(
                command_type=DALI2CommandType.QUERY_ACTUAL_LEVEL,
                address=device.short_address,
                response_expected=True
            )
            response = await self.bus_interface.send_command(command)
            if response is not None:
                device.actual_level = response
                
            # Query max level
            command = DALI2Command(
                command_type=DALI2CommandType.QUERY_MAX_LEVEL,
                address=device.short_address,
                response_expected=True
            )
            response = await self.bus_interface.send_command(command)
            if response is not None:
                device.max_level = response
                
            # Query min level
            command = DALI2Command(
                command_type=DALI2CommandType.QUERY_MIN_LEVEL,
                address=device.short_address,
                response_expected=True
            )
            response = await self.bus_interface.send_command(command)
            if response is not None:
                device.min_level = response
                
            # Determine device type based on capabilities
            device.device_type = self._determine_device_type(device)
            
        except Exception as e:
            self.logger.error(f"Error querying capabilities for device {device.short_address}: {e}")
            
    async def _query_device_information(self, device: DALI2DeviceDiscovery):
        """Query device information (manufacturer, product, versions)"""
        try:
            # Query random address (for device identification)
            command = DALI2Command(
                command_type=DALI2CommandType.QUERY_RANDOM_ADDRESS_H,
                address=device.short_address,
                response_expected=True
            )
            response = await self.bus_interface.send_command(command)
            if response is not None:
                device.random_address = response << 16
                
            command = DALI2Command(
                command_type=DALI2CommandType.QUERY_RANDOM_ADDRESS_M,
                address=device.short_address,
                response_expected=True
            )
            response = await self.bus_interface.send_command(command)
            if response is not None:
                device.random_address = (device.random_address or 0) | (response << 8)
                
            command = DALI2Command(
                command_type=DALI2CommandType.QUERY_RANDOM_ADDRESS_L,
                address=device.short_address,
                response_expected=True
            )
            response = await self.bus_interface.send_command(command)
            if response is not None:
                device.random_address = (device.random_address or 0) | response
                
        except Exception as e:
            self.logger.error(f"Error querying device information for device {device.short_address}: {e}")
            
    async def _query_device_configuration(self, device: DALI2DeviceDiscovery):
        """Query device configuration"""
        try:
            # Query fade time
            command = DALI2Command(
                command_type=DALI2CommandType.QUERY_FADE_TIME,
                address=device.short_address,
                response_expected=True
            )
            response = await self.bus_interface.send_command(command)
            # Store in device info if needed
            
            # Query fade rate
            command = DALI2Command(
                command_type=DALI2CommandType.QUERY_FADE_RATE,
                address=device.short_address,
                response_expected=True
            )
            response = await self.bus_interface.send_command(command)
            # Store in device info if needed
            
        except Exception as e:
            self.logger.error(f"Error querying device configuration for device {device.short_address}: {e}")
            
    async def _query_device_scenes(self, device: DALI2DeviceDiscovery):
        """Query device scene levels"""
        try:
            for scene in range(16):
                command = DALI2Command(
                    command_type=getattr(DALI2CommandType, f'QUERY_SCENE_LEVEL_{scene}'),
                    address=device.short_address,
                    response_expected=True
                )
                response = await self.bus_interface.send_command(command)
                if response is not None and response > 0:
                    device.scenes[scene] = response
                    
        except Exception as e:
            self.logger.error(f"Error querying scenes for device {device.short_address}: {e}")
            
    async def _query_device_groups(self, device: DALI2DeviceDiscovery):
        """Query device group assignments"""
        try:
            # Query groups 0-7
            command = DALI2Command(
                command_type=DALI2CommandType.QUERY_GROUPS_0_7,
                address=device.short_address,
                response_expected=True
            )
            response = await self.bus_interface.send_command(command)
            if response is not None:
                for i in range(8):
                    if response & (1 << i):
                        device.groups.append(i)
                        
            # Query groups 8-15
            command = DALI2Command(
                command_type=DALI2CommandType.QUERY_GROUPS_8_15,
                address=device.short_address,
                response_expected=True
            )
            response = await self.bus_interface.send_command(command)
            if response is not None:
                for i in range(8):
                    if response & (1 << i):
                        device.groups.append(i + 8)
                        
        except Exception as e:
            self.logger.error(f"Error querying groups for device {device.short_address}: {e}")
            
    def _parse_status_response(self, device: DALI2DeviceDiscovery, response: int):
        """Parse DALI-2 status response"""
        device.power_on = bool(response & 0x01)
        device.ballast_failure = bool(response & 0x02)
        device.lamp_failure = bool(response & 0x04)
        device.power_failure = bool(response & 0x08)
        device.fade_running = bool(response & 0x10)
        
    def _determine_device_type(self, device: DALI2DeviceDiscovery) -> DALI2DeviceType:
        """Determine device type based on capabilities"""
        try:
            # Basic heuristics for device type determination
            if device.max_level is not None and device.max_level > 0:
                if device.max_level >= 254:
                    return DALI2DeviceType.BALLAST
                elif device.max_level >= 200:
                    return DALI2DeviceType.LED_DRIVER
                else:
                    return DALI2DeviceType.CONTROL_GEAR
            else:
                # No level control - likely a sensor or control device
                return DALI2DeviceType.SENSOR
                
        except Exception:
            return DALI2DeviceType.UNKNOWN
            
    def _calculate_discovery_confidence(self, device: DALI2DeviceDiscovery) -> float:
        """Calculate confidence score for device discovery"""
        confidence = 1.0
        
        # Reduce confidence for devices with failures
        if device.ballast_failure:
            confidence -= 0.2
        if device.lamp_failure:
            confidence -= 0.2
        if device.power_failure:
            confidence -= 0.3
            
        # Reduce confidence for devices with no level control
        if device.max_level is None or device.max_level == 0:
            confidence -= 0.1
            
        # Reduce confidence for slow response times
        if device.response_time_ms and device.response_time_ms > 100:
            confidence -= 0.1
            
        return max(0.0, min(1.0, confidence))
        
    async def _build_bus_topology(self):
        """Build bus topology information"""
        self.bus_map = {
            'total_devices': len(self.discovered_devices),
            'device_types': {},
            'groups': {},
            'scenes': {},
            'bus_health': self._calculate_bus_health(),
            'discovery_timestamp': time.time()
        }
        
        # Count device types
        for device in self.discovered_devices.values():
            device_type = device.device_type.value
            self.bus_map['device_types'][device_type] = self.bus_map['device_types'].get(device_type, 0) + 1
            
        # Map groups
        for device in self.discovered_devices.values():
            for group in device.groups:
                if group not in self.bus_map['groups']:
                    self.bus_map['groups'][group] = []
                self.bus_map['groups'][group].append(device.short_address)
                
        # Map scenes
        for device in self.discovered_devices.values():
            for scene, level in device.scenes.items():
                if scene not in self.bus_map['scenes']:
                    self.bus_map['scenes'][scene] = {}
                self.bus_map['scenes'][scene][device.short_address] = level
                
    def _calculate_bus_health(self) -> float:
        """Calculate overall bus health score"""
        if not self.discovered_devices:
            return 0.0
            
        total_confidence = sum(device.discovery_confidence for device in self.discovered_devices.values())
        return total_confidence / len(self.discovered_devices)
        
    def _generate_bus_map_report(self):
        """Generate comprehensive bus map report"""
        self.logger.info("=== DALI BUS MAP REPORT ===")
        self.logger.info(f"Total devices discovered: {len(self.discovered_devices)}")
        self.logger.info(f"Bus health score: {self.bus_map['bus_health']:.2f}")
        
        # Count DALI versions
        dali_2_count = sum(1 for d in self.discovered_devices.values() if d.dali_version == DALIVersion.DALI_2)
        dali_legacy_count = sum(1 for d in self.discovered_devices.values() if d.dali_version == DALIVersion.DALI_LEGACY)
        
        self.logger.info(f"\nDALI Versions:")
        self.logger.info(f"  DALI-2 devices: {dali_2_count}")
        self.logger.info(f"  DALI Legacy devices: {dali_legacy_count}")
        
        self.logger.info("\nDevice Types:")
        for device_type, count in self.bus_map['device_types'].items():
            self.logger.info(f"  {device_type}: {count}")
            
        self.logger.info("\nGroups:")
        for group, devices in self.bus_map['groups'].items():
            self.logger.info(f"  Group {group}: {devices}")
            
        self.logger.info("\nScenes:")
        for scene, devices in self.bus_map['scenes'].items():
            self.logger.info(f"  Scene {scene}: {len(devices)} devices")
            
        self.logger.info("\nDevice Details:")
        for address, device in self.discovered_devices.items():
            compatibility_mode = "Legacy" if device.compatibility_mode else "Full"
            self.logger.info(f"  Address {address}: {device.device_type.value} "
                           f"({device.dali_version.value}, {compatibility_mode}) "
                           f"(confidence: {device.discovery_confidence:.2f}, "
                           f"response: {device.response_time_ms:.1f}ms)")
            
    def get_bus_map(self) -> Dict[str, Any]:
        """Get the complete bus map"""
        return self.bus_map
        
    def get_device_by_address(self, address: int) -> Optional[DALI2DeviceDiscovery]:
        """Get device discovery information by address"""
        return self.discovered_devices.get(address)
        
    def get_devices_by_type(self, device_type: DALI2DeviceType) -> List[DALI2DeviceDiscovery]:
        """Get all devices of a specific type"""
        return [device for device in self.discovered_devices.values() 
                if device.device_type == device_type]
                
    def get_devices_by_group(self, group: int) -> List[DALI2DeviceDiscovery]:
        """Get all devices in a specific group"""
        return [device for device in self.discovered_devices.values() 
                if group in device.groups]
                
    def export_bus_map_to_json(self) -> Dict[str, Any]:
        """Export bus map to JSON format"""
        return {
            'bus_map': self.bus_map,
            'devices': {
                str(address): {
                    'short_address': device.short_address,
                    'device_type': device.device_type.value,
                    'manufacturer_id': device.manufacturer_id,
                    'product_id': device.product_id,
                    'max_level': device.max_level,
                    'min_level': device.min_level,
                    'actual_level': device.actual_level,
                    'power_on': device.power_on,
                    'ballast_failure': device.ballast_failure,
                    'lamp_failure': device.lamp_failure,
                    'power_failure': device.power_failure,
                    'fade_running': device.fade_running,
                    'groups': device.groups,
                    'scenes': device.scenes,
                    'response_time_ms': device.response_time_ms,
                    'discovery_confidence': device.discovery_confidence
                }
                for address, device in self.discovered_devices.items()
            }
        }
