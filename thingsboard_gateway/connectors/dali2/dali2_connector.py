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
import json
import socket
import threading
import time
from queue import Queue, Empty
from typing import Dict, List, Optional, Any, Union
from random import choice
from string import ascii_lowercase

# Try to import serial library
try:
    import serial
except ImportError:
    print("pyserial library not found - installing...")
    from thingsboard_gateway.tb_utility.tb_utility import TBUtility
    TBUtility.install_package("pyserial")
    import serial

from thingsboard_gateway.connectors.connector import Connector
from thingsboard_gateway.gateway.constants import CONNECTOR_PARAMETER, DEVICE_SECTION_PARAMETER, DATA_PARAMETER, \
    RPC_METHOD_PARAMETER, RPC_PARAMS_PARAMETER
from thingsboard_gateway.gateway.entities.converted_data import ConvertedData
from thingsboard_gateway.gateway.statistics.statistics_service import StatisticsService
from thingsboard_gateway.tb_utility.tb_logger import init_logger
from thingsboard_gateway.tb_utility.tb_utility import TBUtility

from .entities.dali2_device import DALI2Device, DALI2DeviceInfo, DALI2DeviceType, DALI2Command, DALI2CommandType
from .entities.dali2_connector_config import DALI2ConnectorConfig, DALI2BusConfig, DALI2DeviceConfig
from .dali2_converter import DALI2Converter
from .dali2_bus_mapper import DALI2BusMapper


class DALI2BusInterface:
    """Base class for DALI-2 bus interfaces"""
    
    def __init__(self, config: DALI2BusConfig, logger):
        self.config = config
        self.logger = logger
        self.connected = False
        
    async def connect(self) -> bool:
        """Connect to the DALI-2 bus"""
        raise NotImplementedError
        
    async def disconnect(self):
        """Disconnect from the DALI-2 bus"""
        raise NotImplementedError
        
    async def send_command(self, command: DALI2Command) -> Optional[int]:
        """Send a DALI-2 command and return response"""
        raise NotImplementedError
        
    async def discover_devices(self) -> List[int]:
        """Discover devices on the bus"""
        raise NotImplementedError


class DALI2SerialInterface(DALI2BusInterface):
    """Serial interface for DALI-2 bus"""
    
    def __init__(self, config: DALI2BusConfig, logger):
        super().__init__(config, logger)
        self.serial_connection: Optional[serial.Serial] = None
        
    async def connect(self) -> bool:
        """Connect to serial DALI-2 bus"""
        try:
            connection_params = self.config.connection_params
            self.serial_connection = serial.Serial(
                port=connection_params.get('port', '/dev/ttyUSB0'),
                baudrate=connection_params.get('baudrate', 9600),
                bytesize=connection_params.get('bytesize', 8),
                parity=connection_params.get('parity', 'N'),
                stopbits=connection_params.get('stopbits', 1),
                timeout=connection_params.get('timeout', 1.0),
                xonxoff=connection_params.get('xonxoff', False),
                rtscts=connection_params.get('rtscts', False),
                dsrdtr=connection_params.get('dsrdtr', False)
            )
            
            if self.serial_connection.is_open:
                self.connected = True
                self.logger.info(f"Connected to DALI-2 serial bus: {connection_params.get('port')}")
                return True
            else:
                self.logger.error(f"Failed to open serial port: {connection_params.get('port')}")
                return False
                
        except Exception as e:
            self.logger.error(f"Error connecting to serial DALI-2 bus: {e}")
            return False
            
    async def disconnect(self):
        """Disconnect from serial DALI-2 bus"""
        if self.serial_connection and self.serial_connection.is_open:
            self.serial_connection.close()
            self.connected = False
            self.logger.info("Disconnected from DALI-2 serial bus")
            
    async def send_command(self, command: DALI2Command) -> Optional[int]:
        """Send DALI-2 command via serial interface"""
        if not self.connected or not self.serial_connection:
            self.logger.error("Serial interface not connected")
            return None
            
        try:
            # Convert DALI-2 command to bytes
            command_bytes = self._encode_command(command)
            
            # Send command
            self.serial_connection.write(command_bytes)
            
            # Wait for response if expected
            if command.response_expected:
                response_bytes = self.serial_connection.read(2)  # DALI-2 response is typically 2 bytes
                if len(response_bytes) >= 2:
                    return self._decode_response(response_bytes)
                    
            return None
            
        except Exception as e:
            self.logger.error(f"Error sending DALI-2 command: {e}")
            return None
            
    def _encode_command(self, command: DALI2Command) -> bytes:
        """Encode DALI-2 command to bytes"""
        # DALI-2 command format: [Address][Command][Data]
        address_byte = command.address & 0xFF
        command_byte = self._get_command_byte(command.command_type)
        data_byte = command.data if command.data is not None else 0x00
        
        return bytes([address_byte, command_byte, data_byte])
        
    def _decode_response(self, response_bytes: bytes) -> int:
        """Decode DALI-2 response from bytes"""
        if len(response_bytes) >= 2:
            return (response_bytes[0] << 8) | response_bytes[1]
        return 0
        
    def _get_command_byte(self, command_type: DALI2CommandType) -> int:
        """Get command byte for DALI-2 command type"""
        command_map = {
            DALI2CommandType.DIRECT_ARC_POWER: 0x00,
            DALI2CommandType.QUERY_STATUS: 0x90,
            DALI2CommandType.QUERY_BALLAST_FAILURE: 0x91,
            DALI2CommandType.QUERY_LAMP_FAILURE: 0x92,
            DALI2CommandType.QUERY_POWER_ON: 0x93,
            DALI2CommandType.QUERY_ACTUAL_LEVEL: 0xA0,
            DALI2CommandType.QUERY_MAX_LEVEL: 0xA1,
            DALI2CommandType.QUERY_MIN_LEVEL: 0xA2,
            DALI2CommandType.QUERY_POWER_FAILURE: 0xA3,
            DALI2CommandType.QUERY_FADE_RUNNING: 0xA4,
            DALI2CommandType.QUERY_FADE_TIME: 0xA5,
            DALI2CommandType.QUERY_FADE_RATE: 0xA6,
            DALI2CommandType.QUERY_EXTENDED_FADE_TIME: 0xA7,
            DALI2CommandType.QUERY_SCENE_LEVEL: 0xB0,
            DALI2CommandType.QUERY_GROUPS_0_7: 0xC0,
            DALI2CommandType.QUERY_GROUPS_8_15: 0xC1,
            DALI2CommandType.QUERY_RANDOM_ADDRESS_H: 0xC2,
            DALI2CommandType.QUERY_RANDOM_ADDRESS_M: 0xC3,
            DALI2CommandType.QUERY_RANDOM_ADDRESS_L: 0xC4,
            DALI2CommandType.READ_MEMORY_LOCATION: 0xC5,
            DALI2CommandType.WRITE_MEMORY_LOCATION: 0xC6,
            DALI2CommandType.ENABLE_WRITE_MEMORY: 0xC7,
            DALI2CommandType.SET_SHORT_ADDRESS: 0xC8,
            DALI2CommandType.ENABLE_DEVICE_TYPE_X: 0xC9,
            DALI2CommandType.SET_OPERATING_MODE: 0xCA,
            DALI2CommandType.SET_FADE_TIME: 0xCB,
            DALI2CommandType.SET_FADE_RATE: 0xCC,
            DALI2CommandType.SET_EXTENDED_FADE_TIME: 0xCD,
            DALI2CommandType.SET_SCENE: 0xE0,
            DALI2CommandType.REMOVE_FROM_SCENE: 0xE1,
            DALI2CommandType.ADD_TO_GROUP: 0xE2,
            DALI2CommandType.REMOVE_FROM_GROUP: 0xE3,
        }
        return command_map.get(command_type, 0x00)
        
    async def discover_devices(self) -> List[int]:
        """Discover devices on the DALI-2 bus"""
        discovered_addresses = []
        
        # Try to query each possible address (0-63)
        for address in range(64):
            try:
                command = DALI2Command(
                    command_type=DALI2CommandType.QUERY_STATUS,
                    address=address,
                    response_expected=True,
                    timeout_ms=self.config.command_timeout_ms
                )
                
                response = await self.send_command(command)
                if response is not None and response != 0xFFFF:  # 0xFFFF means no response
                    discovered_addresses.append(address)
                    self.logger.debug(f"Discovered DALI-2 device at address {address}")
                    
            except Exception as e:
                self.logger.debug(f"Error discovering device at address {address}: {e}")
                continue
                
        self.logger.info(f"Discovered {len(discovered_addresses)} DALI-2 devices: {discovered_addresses}")
        return discovered_addresses


class DALI2TCPInterface(DALI2BusInterface):
    """TCP interface for DALI-2 bus (for DALI-2 gateways)"""
    
    def __init__(self, config: DALI2BusConfig, logger):
        super().__init__(config, logger)
        self.socket: Optional[socket.socket] = None
        
    async def connect(self) -> bool:
        """Connect to TCP DALI-2 gateway"""
        try:
            connection_params = self.config.connection_params
            host = connection_params.get('host', 'localhost')
            port = connection_params.get('port', 8080)
            
            self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.socket.settimeout(connection_params.get('timeout', 5.0))
            self.socket.connect((host, port))
            
            self.connected = True
            self.logger.info(f"Connected to DALI-2 TCP gateway: {host}:{port}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error connecting to TCP DALI-2 gateway: {e}")
            return False
            
    async def disconnect(self):
        """Disconnect from TCP DALI-2 gateway"""
        if self.socket:
            self.socket.close()
            self.connected = False
            self.logger.info("Disconnected from DALI-2 TCP gateway")
            
    async def send_command(self, command: DALI2Command) -> Optional[int]:
        """Send DALI-2 command via TCP interface"""
        if not self.connected or not self.socket:
            self.logger.error("TCP interface not connected")
            return None
            
        try:
            # Create JSON command
            command_data = {
                'address': command.address,
                'command': command.command_type.value,
                'data': command.data,
                'timeout': command.timeout_ms
            }
            
            # Send command
            message = json.dumps(command_data) + '\n'
            self.socket.send(message.encode('utf-8'))
            
            # Wait for response if expected
            if command.response_expected:
                response_data = self.socket.recv(1024).decode('utf-8')
                response = json.loads(response_data)
                return response.get('value')
                
            return None
            
        except Exception as e:
            self.logger.error(f"Error sending DALI-2 TCP command: {e}")
            return None
            
    async def discover_devices(self) -> List[int]:
        """Discover devices via TCP gateway"""
        try:
            # Send discovery command
            discovery_data = {'command': 'discover', 'timeout': 10000}
            message = json.dumps(discovery_data) + '\n'
            self.socket.send(message.encode('utf-8'))
            
            # Receive discovery results
            response_data = self.socket.recv(4096).decode('utf-8')
            response = json.loads(response_data)
            
            discovered_addresses = response.get('devices', [])
            self.logger.info(f"Discovered {len(discovered_addresses)} DALI-2 devices via TCP: {discovered_addresses}")
            return discovered_addresses
            
        except Exception as e:
            self.logger.error(f"Error discovering devices via TCP: {e}")
            return []


class DALI2Connector(Connector, threading.Thread):
    """DALI-2 Connector for ThingsBoard Gateway"""
    
    def __init__(self, gateway, config, connector_type):
        self.statistics = {'MessagesReceived': 0, 'MessagesSent': 0}
        super().__init__()
        
        self.__gateway = gateway
        self._connector_type = connector_type
        self.__config = config
        self.__id = config.get('id')
        self.name = config.get("name", 'DALI-2 Connector ' + ''.join(choice(ascii_lowercase) for _ in range(5)))
        
        # Initialize logger
        self.__log = init_logger(
            self.__gateway, 
            self.name,
            config.get('logLevel', 'INFO'),
            enable_remote_logging=config.get('enableRemoteLogging', False),
            is_connector_logger=True
        )
        
        self.__converter_log = init_logger(
            self.__gateway, 
            self.name + '_converter',
            config.get('logLevel', 'INFO'),
            enable_remote_logging=config.get('enableRemoteLogging', False),
            is_converter_logger=True, 
            attr_name=self.name
        )
        
        # Connection state
        self.__connected = False
        self.__stopped = False
        self.daemon = True
        
        # Initialize configuration
        self.__dali2_config = self._parse_config(config)
        
        # Bus interfaces
        self.__bus_interfaces: Dict[str, DALI2BusInterface] = {}
        
        # Bus mappers for advanced discovery
        self.__bus_mappers: Dict[str, DALI2BusMapper] = {}
        
        # Devices
        self.__devices: Dict[str, DALI2Device] = {}
        
        # Data queues
        self.__data_to_convert = Queue(-1)
        self.__data_to_save = Queue(-1)
        
        # Converter
        self.__converter = DALI2Converter(self.__converter_log)
        
        # Event loop for async operations
        self.__loop = None
        self.__loop_thread = None
        
        self.__log.info('DALI-2 Connector initialized')
        
    def _parse_config(self, config) -> DALI2ConnectorConfig:
        """Parse connector configuration"""
        try:
            # Parse buses
            buses = []
            for bus_config in config.get('buses', []):
                bus = DALI2BusConfig(
                    bus_id=bus_config['bus_id'],
                    interface_type=bus_config['interface_type'],
                    connection_params=bus_config['connection_params'],
                    poll_interval_ms=bus_config.get('poll_interval_ms', 5000),
                    command_timeout_ms=bus_config.get('command_timeout_ms', 1000),
                    max_retries=bus_config.get('max_retries', 3),
                    enable_discovery=bus_config.get('enable_discovery', True),
                    discovery_interval_ms=bus_config.get('discovery_interval_ms', 30000),
                    enable_continuous_polling=bus_config.get('enable_continuous_polling', True),
                    enable_event_reporting=bus_config.get('enable_event_reporting', True)
                )
                buses.append(bus)
                
            # Parse devices
            devices = []
            for device_config in config.get('devices', []):
                device = DALI2DeviceConfig(
                    device_name=device_config['device_name'],
                    short_address=device_config['short_address'],
                    device_type=device_config.get('device_type', 'dali2_device'),
                    bus_id=device_config.get('bus_id', 'default'),
                    poll_interval_ms=device_config.get('poll_interval_ms'),
                    enable_telemetry=device_config.get('enable_telemetry', True),
                    enable_attributes=device_config.get('enable_attributes', True),
                    enable_rpc=device_config.get('enable_rpc', True),
                    telemetry_keys=device_config.get('telemetry_keys'),
                    attribute_keys=device_config.get('attribute_keys'),
                    rpc_methods=device_config.get('rpc_methods'),
                    scenes=device_config.get('scenes', {}),
                    groups=device_config.get('groups', []),
                    custom_attributes=device_config.get('custom_attributes', {})
                )
                devices.append(device)
                
            return DALI2ConnectorConfig(
                name=config.get('name', 'DALI-2 Connector'),
                log_level=config.get('logLevel', 'INFO'),
                enable_remote_logging=config.get('enableRemoteLogging', False),
                buses=buses,
                devices=devices,
                global_settings=config.get('global_settings', {})
            )
            
        except Exception as e:
            self.__log.error(f"Error parsing DALI-2 configuration: {e}")
            raise
            
    def open(self):
        """Start the DALI-2 connector"""
        self.__stopped = False
        self.start()
        
    def close(self):
        """Stop the DALI-2 connector"""
        self.__stopped = True
        self.__connected = False
        
        # Disconnect from all buses
        for bus_interface in self.__bus_interfaces.values():
            if hasattr(bus_interface, 'disconnect'):
                asyncio.run_coroutine_threadsafe(bus_interface.disconnect(), self.__loop)
                
        # Stop event loop
        if self.__loop and not self.__loop.is_closed():
            self.__loop.call_soon_threadsafe(self.__loop.stop)
            
        self.__log.info('%s has been stopped', self.get_name())
        self.__log.stop()
        
    def run(self):
        """Main connector thread"""
        try:
            # Create event loop for this thread
            self.__loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self.__loop)
            
            # Run the main async loop
            self.__loop.run_until_complete(self._main_loop())
            
        except Exception as e:
            self.__log.exception(f"Error in DALI-2 connector main loop: {e}")
        finally:
            if self.__loop and not self.__loop.is_closed():
                self.__loop.close()
                
    async def _main_loop(self):
        """Main async loop for DALI-2 connector"""
        try:
            # Initialize bus interfaces
            await self._initialize_bus_interfaces()
            
            # Discover devices
            await self._discover_devices()
            
            # Start data processing tasks
            tasks = [
                asyncio.create_task(self._poll_devices()),
                asyncio.create_task(self._convert_data()),
                asyncio.create_task(self._save_data())
            ]
            
            # Wait for all tasks
            await asyncio.gather(*tasks)
            
        except Exception as e:
            self.__log.exception(f"Error in DALI-2 main loop: {e}")
            
    async def _initialize_bus_interfaces(self):
        """Initialize all bus interfaces"""
        for bus_config in self.__dali2_config.buses:
            try:
                if bus_config.interface_type == 'serial':
                    bus_interface = DALI2SerialInterface(bus_config, self.__log)
                elif bus_config.interface_type == 'tcp':
                    bus_interface = DALI2TCPInterface(bus_config, self.__log)
                else:
                    self.__log.error(f"Unsupported bus interface type: {bus_config.interface_type}")
                    continue
                    
                # Connect to bus
                if await bus_interface.connect():
                    self.__bus_interfaces[bus_config.bus_id] = bus_interface
                    
                    # Create bus mapper for advanced discovery
                    bus_mapper = DALI2BusMapper(bus_interface, self.__log)
                    self.__bus_mappers[bus_config.bus_id] = bus_mapper
                    
                    self.__log.info(f"Connected to DALI-2 bus: {bus_config.bus_id}")
                else:
                    self.__log.error(f"Failed to connect to DALI-2 bus: {bus_config.bus_id}")
                    
            except Exception as e:
                self.__log.error(f"Error initializing bus interface {bus_config.bus_id}: {e}")
                
        self.__connected = len(self.__bus_interfaces) > 0
        
    async def _discover_devices(self):
        """Discover devices on all buses using advanced bus mapping"""
        for bus_id, bus_mapper in self.__bus_mappers.items():
            try:
                self.__log.info(f"Starting comprehensive bus mapping for bus: {bus_id}")
                
                # Perform comprehensive bus mapping
                discovered_devices = await bus_mapper.map_entire_bus(detailed_scan=True)
                
                # Create device objects for discovered devices
                for address, device_discovery in discovered_devices.items():
                    device_name = f"dali2_device_{bus_id}_{address}"
                    
                    # Check if device is configured
                    device_config = self.__dali2_config.get_device_config(device_name)
                    if not device_config:
                        # Create default configuration based on discovery
                        device_config = DALI2DeviceConfig(
                            device_name=device_name,
                            short_address=address,
                            bus_id=bus_id,
                            device_type=device_discovery.device_type.value,
                            custom_attributes={
                                'discovery_confidence': device_discovery.discovery_confidence,
                                'response_time_ms': device_discovery.response_time_ms,
                                'manufacturer_id': device_discovery.manufacturer_id,
                                'product_id': device_discovery.product_id
                            }
                        )
                        
                    # Create device object with detailed information
                    device_info = DALI2DeviceInfo(
                        short_address=address,
                        device_type=device_discovery.device_type,
                        manufacturer_id=device_discovery.manufacturer_id,
                        product_id=device_discovery.product_id,
                        application_version=device_discovery.application_version,
                        hardware_version=device_discovery.hardware_version,
                        firmware_version=device_discovery.firmware_version,
                        max_level=device_discovery.max_level,
                        min_level=device_discovery.min_level,
                        actual_level=device_discovery.actual_level,
                        power_on=device_discovery.power_on,
                        ballast_failure=device_discovery.ballast_failure,
                        lamp_failure=device_discovery.lamp_failure,
                        power_failure=device_discovery.power_failure,
                        fade_running=device_discovery.fade_running,
                        groups=device_discovery.groups,
                        scenes=device_discovery.scenes
                    )
                    
                    device = DALI2Device(
                        device_info=device_info,
                        name=device_name,
                        device_type=device_config.device_type,
                        configuration=device_config.custom_attributes
                    )
                    
                    self.__devices[device_name] = device
                    
                    # Add device to ThingsBoard
                    self.__gateway.add_device(device_name, {CONNECTOR_PARAMETER: self}, 
                                            device_type=device_config.device_type)
                    
                    self.__log.info(f"Added DALI-2 device: {device_name} "
                                  f"(type: {device_discovery.device_type.value}, "
                                  f"confidence: {device_discovery.discovery_confidence:.2f})")
                    
                # Export bus map for debugging
                bus_map = bus_mapper.export_bus_map_to_json()
                self.__log.debug(f"Bus map for {bus_id}: {bus_map}")
                    
            except Exception as e:
                self.__log.error(f"Error discovering devices on bus {bus_id}: {e}")
                
    async def _poll_devices(self):
        """Poll all devices for data"""
        while not self.__stopped:
            try:
                for device_name, device in self.__devices.items():
                    if self.__stopped:
                        break
                        
                    try:
                        # Get device configuration
                        device_config = self.__dali2_config.get_device_config(device_name)
                        if not device_config:
                            continue
                            
                        # Get bus interface
                        bus_interface = self.__bus_interfaces.get(device_config.bus_id)
                        if not bus_interface:
                            continue
                            
                        # Poll device data
                        await self._poll_device_data(device, device_config, bus_interface)
                        
                    except Exception as e:
                        self.__log.error(f"Error polling device {device_name}: {e}")
                        
                # Wait before next poll cycle
                await asyncio.sleep(1.0)  # Poll every second
                
            except Exception as e:
                self.__log.error(f"Error in device polling loop: {e}")
                await asyncio.sleep(5.0)
                
    async def _poll_device_data(self, device: DALI2Device, device_config: DALI2DeviceConfig, 
                               bus_interface: DALI2BusInterface):
        """Poll data from a specific device"""
        try:
            # Query device status
            status_command = DALI2Command(
                command_type=DALI2CommandType.QUERY_STATUS,
                address=device.device_info.short_address,
                response_expected=True,
                timeout_ms=device_config.poll_interval_ms or 1000
            )
            
            response = await bus_interface.send_command(status_command)
            if response is not None:
                # Parse status response
                status_data = self._parse_status_response(response)
                device.update_status(status_data)
                device.online = True
                device.last_seen = time.time()
                
                # Query additional data if needed
                if device_config.enable_telemetry:
                    await self._query_telemetry_data(device, device_config, bus_interface)
                    
                if device_config.enable_attributes:
                    await self._query_attribute_data(device, device_config, bus_interface)
                    
                # Add data to conversion queue
                self.__data_to_convert.put((device, device_config))
                
            else:
                device.online = False
                self.__log.warning(f"No response from device {device.name}")
                
        except Exception as e:
            self.__log.error(f"Error polling device {device.name}: {e}")
            device.online = False
            
    async def _query_telemetry_data(self, device: DALI2Device, device_config: DALI2DeviceConfig,
                                   bus_interface: DALI2BusInterface):
        """Query telemetry data from device"""
        try:
            # Query actual level
            level_command = DALI2Command(
                command_type=DALI2CommandType.QUERY_ACTUAL_LEVEL,
                address=device.device_info.short_address,
                response_expected=True
            )
            
            response = await bus_interface.send_command(level_command)
            if response is not None:
                device.device_info.actual_level = response
                
            # Query max level
            max_level_command = DALI2Command(
                command_type=DALI2CommandType.QUERY_MAX_LEVEL,
                address=device.device_info.short_address,
                response_expected=True
            )
            
            response = await bus_interface.send_command(max_level_command)
            if response is not None:
                device.device_info.max_level = response
                
            # Query min level
            min_level_command = DALI2Command(
                command_type=DALI2CommandType.QUERY_MIN_LEVEL,
                address=device.device_info.short_address,
                response_expected=True
            )
            
            response = await bus_interface.send_command(min_level_command)
            if response is not None:
                device.device_info.min_level = response
                
        except Exception as e:
            self.__log.error(f"Error querying telemetry data from {device.name}: {e}")
            
    async def _query_attribute_data(self, device: DALI2Device, device_config: DALI2DeviceConfig,
                                   bus_interface: DALI2BusInterface):
        """Query attribute data from device"""
        try:
            # Query groups
            groups_command = DALI2Command(
                command_type=DALI2CommandType.QUERY_GROUPS_0_7,
                address=device.device_info.short_address,
                response_expected=True
            )
            
            response = await bus_interface.send_command(groups_command)
            if response is not None:
                # Parse groups response
                groups = []
                for i in range(8):
                    if response & (1 << i):
                        groups.append(i)
                device.device_info.groups = groups
                
        except Exception as e:
            self.__log.error(f"Error querying attribute data from {device.name}: {e}")
            
    def _parse_status_response(self, response: int) -> Dict[str, Any]:
        """Parse DALI-2 status response"""
        return {
            'power_on': bool(response & 0x01),
            'ballast_failure': bool(response & 0x02),
            'lamp_failure': bool(response & 0x04),
            'power_failure': bool(response & 0x08),
            'fade_running': bool(response & 0x10)
        }
        
    async def _convert_data(self):
        """Convert device data to ThingsBoard format"""
        while not self.__stopped:
            try:
                if not self.__data_to_convert.empty():
                    device, device_config = self.__data_to_convert.get_nowait()
                    
                    # Convert device data
                    converted_data = self.__converter.convert(device_config, device)
                    
                    if converted_data:
                        self.__data_to_save.put(converted_data)
                        
                else:
                    await asyncio.sleep(0.1)
                    
            except Empty:
                await asyncio.sleep(0.1)
            except Exception as e:
                self.__log.error(f"Error converting data: {e}")
                
    async def _save_data(self):
        """Save converted data to ThingsBoard"""
        while not self.__stopped:
            try:
                if not self.__data_to_save.empty():
                    converted_data = self.__data_to_save.get_nowait()
                    
                    # Send to ThingsBoard
                    StatisticsService.count_connector_message(self.get_name(), stat_parameter_name='storageMsgPushed')
                    self.__gateway.send_to_storage(self.get_name(), self.get_id(), converted_data)
                    self.statistics['MessagesSent'] += 1
                    
                else:
                    await asyncio.sleep(0.1)
                    
            except Empty:
                await asyncio.sleep(0.1)
            except Exception as e:
                self.__log.error(f"Error saving data: {e}")
                
    def on_attributes_update(self, content):
        """Handle attribute updates from ThingsBoard"""
        self.__log.debug('Got attributes update: %s', content)
        
        try:
            device_name = content[DEVICE_SECTION_PARAMETER]
            device = self.__devices.get(device_name)
            
            if not device:
                self.__log.error(f'Device {device_name} not found for attributes update')
                return
                
            # Get device configuration
            device_config = self.__dali2_config.get_device_config(device_name)
            if not device_config:
                self.__log.error(f'Configuration not found for device {device_name}')
                return
                
            # Get bus interface
            bus_interface = self.__bus_interfaces.get(device_config.bus_id)
            if not bus_interface:
                self.__log.error(f'Bus interface not found for device {device_name}')
                return
                
            # Process attribute updates
            for attribute_key, attribute_value in content[DATA_PARAMETER].items():
                self._process_attribute_update(device, device_config, bus_interface, 
                                             attribute_key, attribute_value)
                
        except Exception as e:
            self.__log.exception(f'Error processing attributes update: {e}')
            
    def _process_attribute_update(self, device: DALI2Device, device_config: DALI2DeviceConfig,
                                 bus_interface: DALI2BusInterface, attribute_key: str, attribute_value: Any):
        """Process a single attribute update"""
        try:
            if attribute_key == 'fade_time' and isinstance(attribute_value, int):
                # Set fade time
                command = DALI2Command(
                    command_type=DALI2CommandType.SET_FADE_TIME,
                    address=device.device_info.short_address,
                    data=attribute_value,
                    response_expected=False
                )
                asyncio.run_coroutine_threadsafe(bus_interface.send_command(command), self.__loop)
                
            elif attribute_key == 'fade_rate' and isinstance(attribute_value, int):
                # Set fade rate
                command = DALI2Command(
                    command_type=DALI2CommandType.SET_FADE_RATE,
                    address=device.device_info.short_address,
                    data=attribute_value,
                    response_expected=False
                )
                asyncio.run_coroutine_threadsafe(bus_interface.send_command(command), self.__loop)
                
            elif attribute_key.startswith('scene_') and isinstance(attribute_value, int):
                # Set scene level
                scene_number = int(attribute_key.split('_')[1])
                command = DALI2Command(
                    command_type=DALI2CommandType.SET_SCENE,
                    address=device.device_info.short_address,
                    data=scene_number,
                    response_expected=False
                )
                asyncio.run_coroutine_threadsafe(bus_interface.send_command(command), self.__loop)
                
            self.__log.info(f'Updated attribute {attribute_key} for device {device.name}')
            
        except Exception as e:
            self.__log.error(f'Error processing attribute update {attribute_key}: {e}')
            
    def server_side_rpc_handler(self, content):
        """Handle RPC requests from ThingsBoard"""
        self.__log.info('Received server side rpc request: %r', content)
        
        try:
            device_name = content.get('device')
            method = content.get('data', {}).get('method')
            params = content.get('data', {}).get('params', {})
            request_id = content.get('data', {}).get('id')
            
            if not device_name or not method:
                self.__log.error('Invalid RPC request: missing device or method')
                return {'error': 'Invalid RPC request', 'success': False}
                
            device = self.__devices.get(device_name)
            if not device:
                self.__log.error(f'Device {device_name} not found for RPC request')
                return {'error': f'Device {device_name} not found', 'success': False}
                
            # Get device configuration
            device_config = self.__dali2_config.get_device_config(device_name)
            if not device_config:
                self.__log.error(f'Configuration not found for device {device_name}')
                return {'error': 'Device configuration not found', 'success': False}
                
            # Get bus interface
            bus_interface = self.__bus_interfaces.get(device_config.bus_id)
            if not bus_interface:
                self.__log.error(f'Bus interface not found for device {device_name}')
                return {'error': 'Bus interface not found', 'success': False}
                
            # Process RPC method
            result = self._process_rpc_method(device, device_config, bus_interface, method, params)
            
            # Send response back to ThingsBoard
            if request_id:
                self.__gateway.send_rpc_reply(device_name, request_id, {'result': result})
                
            return {'result': result, 'success': True}
            
        except Exception as e:
            self.__log.exception(f'Error processing RPC request: {e}')
            return {'error': str(e), 'success': False}
            
    def _process_rpc_method(self, device: DALI2Device, device_config: DALI2DeviceConfig,
                           bus_interface: DALI2BusInterface, method: str, params: Dict[str, Any]) -> Any:
        """Process a specific RPC method"""
        try:
            if method == 'set_level':
                level = params.get('level', 0)
                if 0 <= level <= 254:
                    command = DALI2Command(
                        command_type=DALI2CommandType.DIRECT_ARC_POWER,
                        address=device.device_info.short_address,
                        data=level,
                        response_expected=False
                    )
                    asyncio.run_coroutine_threadsafe(bus_interface.send_command(command), self.__loop)
                    return {'level': level}
                else:
                    return {'error': 'Level must be between 0 and 254'}
                    
            elif method == 'set_scene':
                scene_number = params.get('scene', 0)
                if 0 <= scene_number <= 15:
                    command = DALI2Command(
                        command_type=DALI2CommandType.SET_SCENE,
                        address=device.device_info.short_address,
                        data=scene_number,
                        response_expected=False
                    )
                    asyncio.run_coroutine_threadsafe(bus_interface.send_command(command), self.__loop)
                    return {'scene': scene_number}
                else:
                    return {'error': 'Scene number must be between 0 and 15'}
                    
            elif method == 'query_status':
                command = DALI2Command(
                    command_type=DALI2CommandType.QUERY_STATUS,
                    address=device.device_info.short_address,
                    response_expected=True
                )
                response = asyncio.run_coroutine_threadsafe(bus_interface.send_command(command), self.__loop).result()
                if response is not None:
                    status = self._parse_status_response(response)
                    return status
                else:
                    return {'error': 'No response from device'}
                    
            else:
                return {'error': f'Unknown RPC method: {method}'}
                
        except Exception as e:
            self.__log.error(f'Error processing RPC method {method}: {e}')
            return {'error': str(e)}
            
    # Required getter methods
    def get_id(self):
        return self.__id
        
    def get_name(self):
        return self.name
        
    def get_type(self):
        return self._connector_type
        
    def get_config(self):
        return self.__config
        
    def is_connected(self):
        return self.__connected
        
    def is_stopped(self):
        return self.__stopped
