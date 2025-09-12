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
from thingsboard_gateway.connectors.converter import Converter
from thingsboard_gateway.gateway.entities.converted_data import ConvertedData
from .entities.dali2_device import DALI2Device
from .entities.dali2_connector_config import DALI2DeviceConfig


class DALI2Converter(Converter):
    """DALI-2 data converter for ThingsBoard Gateway"""
    
    def __init__(self, logger):
        self.logger = logger
        
    def convert(self, config: DALI2DeviceConfig, data: Union[DALI2Device, Dict[str, Any]]) -> ConvertedData:
        """
        Convert DALI-2 device data to ThingsBoard format
        
        Args:
            config: DALI-2 device configuration
            data: DALI-2 device object or raw data dictionary
            
        Returns:
            ConvertedData object for ThingsBoard
        """
        try:
            if isinstance(data, DALI2Device):
                return self._convert_device_data(config, data)
            elif isinstance(data, dict):
                return self._convert_raw_data(config, data)
            else:
                self.logger.error(f"Unsupported data type for conversion: {type(data)}")
                return None
                
        except Exception as e:
            self.logger.exception(f"Error converting DALI-2 data: {e}")
            return None
            
    def _convert_device_data(self, config: DALI2DeviceConfig, device: DALI2Device) -> ConvertedData:
        """Convert DALI-2 device object to ThingsBoard format"""
        try:
            # Create converted data object
            converted_data = ConvertedData(
                device_name=device.name,
                device_type=device.device_type
            )
            
            # Add telemetry data if enabled
            if config.enable_telemetry:
                telemetry_data = self._extract_telemetry_data(device, config)
                if telemetry_data:
                    converted_data.add_to_telemetry(telemetry_data)
                    
            # Add attributes data if enabled
            if config.enable_attributes:
                attributes_data = self._extract_attributes_data(device, config)
                if attributes_data:
                    converted_data.add_to_attributes(attributes_data)
                    
            return converted_data
            
        except Exception as e:
            self.logger.exception(f"Error converting device data: {e}")
            return None
            
    def _convert_raw_data(self, config: DALI2DeviceConfig, data: Dict[str, Any]) -> ConvertedData:
        """Convert raw data dictionary to ThingsBoard format"""
        try:
            device_name = data.get('device_name', config.device_name)
            device_type = data.get('device_type', config.device_type)
            
            # Create converted data object
            converted_data = ConvertedData(
                device_name=device_name,
                device_type=device_type
            )
            
            # Add telemetry data if enabled
            if config.enable_telemetry:
                telemetry_data = self._extract_telemetry_from_raw_data(data, config)
                if telemetry_data:
                    converted_data.add_to_telemetry(telemetry_data)
                    
            # Add attributes data if enabled
            if config.enable_attributes:
                attributes_data = self._extract_attributes_from_raw_data(data, config)
                if attributes_data:
                    converted_data.add_to_attributes(attributes_data)
                    
            return converted_data
            
        except Exception as e:
            self.logger.exception(f"Error converting raw data: {e}")
            return None
            
    def _extract_telemetry_data(self, device: DALI2Device, config: DALI2DeviceConfig) -> Dict[str, Any]:
        """Extract telemetry data from DALI-2 device"""
        try:
            telemetry_data = {}
            
            # Get device telemetry data
            device_telemetry = device.get_telemetry_data()
            
            # Filter based on configuration
            for key in config.telemetry_keys:
                if key in device_telemetry:
                    value = device_telemetry[key]
                    
                    # Apply data type conversion if needed
                    converted_value = self._convert_telemetry_value(key, value)
                    if converted_value is not None:
                        telemetry_data[key] = converted_value
                        
            # Add custom telemetry data from configuration
            if config.custom_attributes:
                for key, value in config.custom_attributes.items():
                    if key.startswith('telemetry_'):
                        telemetry_key = key.replace('telemetry_', '')
                        telemetry_data[telemetry_key] = value
                        
            # Add calculated telemetry values
            telemetry_data.update(self._calculate_derived_telemetry(device))
            
            self.logger.debug(f"Extracted telemetry data for {device.name}: {telemetry_data}")
            return telemetry_data
            
        except Exception as e:
            self.logger.exception(f"Error extracting telemetry data: {e}")
            return {}
            
    def _extract_attributes_data(self, device: DALI2Device, config: DALI2DeviceConfig) -> Dict[str, Any]:
        """Extract attributes data from DALI-2 device"""
        try:
            attributes_data = {}
            
            # Get device attributes data
            device_attributes = device.get_attributes_data()
            
            # Filter based on configuration
            for key in config.attribute_keys:
                if key in device_attributes:
                    value = device_attributes[key]
                    
                    # Apply data type conversion if needed
                    converted_value = self._convert_attribute_value(key, value)
                    if converted_value is not None:
                        attributes_data[key] = converted_value
                        
            # Add custom attributes data from configuration
            if config.custom_attributes:
                for key, value in config.custom_attributes.items():
                    if key.startswith('attribute_'):
                        attribute_key = key.replace('attribute_', '')
                        attributes_data[attribute_key] = value
                        
            # Add device configuration as attributes
            attributes_data.update({
                'bus_id': config.bus_id,
                'short_address': config.short_address,
                'device_type': config.device_type,
                'poll_interval_ms': config.poll_interval_ms or 0,
                'enable_telemetry': config.enable_telemetry,
                'enable_attributes': config.enable_attributes,
                'enable_rpc': config.enable_rpc
            })
            
            self.logger.debug(f"Extracted attributes data for {device.name}: {attributes_data}")
            return attributes_data
            
        except Exception as e:
            self.logger.exception(f"Error extracting attributes data: {e}")
            return {}
            
    def _extract_telemetry_from_raw_data(self, data: Dict[str, Any], config: DALI2DeviceConfig) -> Dict[str, Any]:
        """Extract telemetry data from raw data dictionary"""
        try:
            telemetry_data = {}
            
            # Filter based on configuration
            for key in config.telemetry_keys:
                if key in data:
                    value = data[key]
                    converted_value = self._convert_telemetry_value(key, value)
                    if converted_value is not None:
                        telemetry_data[key] = converted_value
                        
            return telemetry_data
            
        except Exception as e:
            self.logger.exception(f"Error extracting telemetry from raw data: {e}")
            return {}
            
    def _extract_attributes_from_raw_data(self, data: Dict[str, Any], config: DALI2DeviceConfig) -> Dict[str, Any]:
        """Extract attributes data from raw data dictionary"""
        try:
            attributes_data = {}
            
            # Filter based on configuration
            for key in config.attribute_keys:
                if key in data:
                    value = data[key]
                    converted_value = self._convert_attribute_value(key, value)
                    if converted_value is not None:
                        attributes_data[key] = converted_value
                        
            return attributes_data
            
        except Exception as e:
            self.logger.exception(f"Error extracting attributes from raw data: {e}")
            return {}
            
    def _convert_telemetry_value(self, key: str, value: Any) -> Any:
        """Convert telemetry value to appropriate type"""
        try:
            if value is None:
                return None
                
            # Handle specific telemetry keys
            if key in ['actual_level', 'max_level', 'min_level', 'fade_time', 'fade_rate', 'operating_mode']:
                return int(value) if value is not None else 0
                
            elif key in ['power_on', 'ballast_failure', 'lamp_failure', 'power_failure', 'fade_running', 'online']:
                return bool(value) if value is not None else False
                
            elif key in ['brightness_percentage']:
                # Calculate brightness percentage from actual level
                if isinstance(value, (int, float)) and value >= 0:
                    return min(100.0, max(0.0, (value / 254.0) * 100.0))
                return 0.0
                
            else:
                # Default conversion
                if isinstance(value, (int, float)):
                    return value
                elif isinstance(value, bool):
                    return value
                elif isinstance(value, str):
                    try:
                        # Try to convert string to number
                        if '.' in value:
                            return float(value)
                        else:
                            return int(value)
                    except ValueError:
                        return value
                else:
                    return str(value)
                    
        except Exception as e:
            self.logger.error(f"Error converting telemetry value {key}: {e}")
            return None
            
    def _convert_attribute_value(self, key: str, value: Any) -> Any:
        """Convert attribute value to appropriate type"""
        try:
            if value is None:
                return None
                
            # Handle specific attribute keys
            if key in ['short_address', 'manufacturer_id', 'product_id', 'application_version', 
                      'hardware_version', 'firmware_version', 'scenes_count']:
                return int(value) if value is not None else 0
                
            elif key in ['device_type']:
                return str(value) if value is not None else 'unknown'
                
            elif key in ['groups']:
                if isinstance(value, str):
                    return value
                elif isinstance(value, list):
                    return ','.join(map(str, value))
                else:
                    return str(value)
                    
            else:
                # Default conversion
                if isinstance(value, (int, float)):
                    return value
                elif isinstance(value, bool):
                    return value
                else:
                    return str(value)
                    
        except Exception as e:
            self.logger.error(f"Error converting attribute value {key}: {e}")
            return None
            
    def _calculate_derived_telemetry(self, device: DALI2Device) -> Dict[str, Any]:
        """Calculate derived telemetry values"""
        try:
            derived_data = {}
            
            # Calculate brightness percentage
            if device.device_info.actual_level is not None:
                brightness_percentage = (device.device_info.actual_level / 254.0) * 100.0
                derived_data['brightness_percentage'] = round(brightness_percentage, 2)
                
            # Calculate power consumption (estimated based on level)
            if device.device_info.actual_level is not None and device.device_info.max_level is not None:
                if device.device_info.max_level > 0:
                    power_factor = device.device_info.actual_level / device.device_info.max_level
                    # Assume max power of 50W for LED driver
                    estimated_power = power_factor * 50.0
                    derived_data['estimated_power_watts'] = round(estimated_power, 2)
                    
            # Calculate device health score
            health_score = 100
            if device.device_info.ballast_failure:
                health_score -= 30
            if device.device_info.lamp_failure:
                health_score -= 30
            if device.device_info.power_failure:
                health_score -= 20
            if not device.device_info.power_on:
                health_score -= 10
            if not device.online:
                health_score = 0
                
            derived_data['health_score'] = max(0, health_score)
            
            # Calculate uptime (if last_seen is available)
            if device.last_seen:
                import time
                uptime_seconds = time.time() - device.last_seen
                derived_data['uptime_seconds'] = int(uptime_seconds)
                
            return derived_data
            
        except Exception as e:
            self.logger.error(f"Error calculating derived telemetry: {e}")
            return {}
            
    def convert_downlink(self, config: DALI2DeviceConfig, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Convert ThingsBoard data to DALI-2 command format
        
        Args:
            config: DALI-2 device configuration
            data: Data from ThingsBoard (RPC or attribute update)
            
        Returns:
            Dictionary with DALI-2 command information
        """
        try:
            command_data = {
                'address': config.short_address,
                'command_type': None,
                'data': None,
                'response_expected': False
            }
            
            # Handle different types of downlink data
            if 'method' in data:
                # RPC command
                method = data['method']
                params = data.get('params', {})
                
                if method == 'set_level':
                    level = params.get('level', 0)
                    if 0 <= level <= 254:
                        command_data.update({
                            'command_type': 'direct_arc_power',
                            'data': level
                        })
                        
                elif method == 'set_scene':
                    scene = params.get('scene', 0)
                    if 0 <= scene <= 15:
                        command_data.update({
                            'command_type': 'set_scene',
                            'data': scene
                        })
                        
                elif method == 'set_fade_time':
                    fade_time = params.get('fade_time', 0)
                    if 0 <= fade_time <= 15:
                        command_data.update({
                            'command_type': 'set_fade_time',
                            'data': fade_time
                        })
                        
                elif method == 'set_fade_rate':
                    fade_rate = params.get('fade_rate', 0)
                    if 0 <= fade_rate <= 15:
                        command_data.update({
                            'command_type': 'set_fade_rate',
                            'data': fade_rate
                        })
                        
            elif 'attribute' in data:
                # Attribute update
                attribute = data['attribute']
                value = data.get('value')
                
                if attribute == 'fade_time' and isinstance(value, int):
                    command_data.update({
                        'command_type': 'set_fade_time',
                        'data': value
                    })
                    
                elif attribute == 'fade_rate' and isinstance(value, int):
                    command_data.update({
                        'command_type': 'set_fade_rate',
                        'data': value
                    })
                    
            return command_data
            
        except Exception as e:
            self.logger.exception(f"Error converting downlink data: {e}")
            return {}
