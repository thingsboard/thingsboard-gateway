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

from thingsboard_gateway.connectors.dali2.dali2_converter import DALI2Converter


class DALI2UplinkConverter(DALI2Converter):
    """
    DALI-2 Uplink Converter for ThingsBoard Gateway Extensions
    
    This converter extends the base DALI2Converter to provide
    additional functionality for uplink data processing.
    """
    
    def __init__(self, config, logger):
        super().__init__(logger)
        self.config = config
        
    def convert(self, config, data):
        """
        Convert DALI-2 uplink data with additional processing
        
        Args:
            config: Device configuration
            data: DALI-2 device data
            
        Returns:
            ConvertedData object for ThingsBoard
        """
        try:
            # Call parent converter
            converted_data = super().convert(config, data)
            
            if converted_data:
                # Add additional uplink-specific processing
                self._add_uplink_metadata(converted_data, config)
                self._add_timestamp_data(converted_data)
                self._add_quality_indicators(converted_data, data)
                
            return converted_data
            
        except Exception as e:
            self.logger.exception(f"Error in DALI-2 uplink conversion: {e}")
            return None
            
    def _add_uplink_metadata(self, converted_data, config):
        """Add uplink-specific metadata"""
        try:
            # Add connector information
            converted_data.add_to_metadata({
                'connector_type': 'dali2',
                'bus_id': getattr(config, 'bus_id', 'unknown'),
                'short_address': getattr(config, 'short_address', 0),
                'device_type': getattr(config, 'device_type', 'unknown')
            })
            
        except Exception as e:
            self.logger.error(f"Error adding uplink metadata: {e}")
            
    def _add_timestamp_data(self, converted_data):
        """Add timestamp information"""
        try:
            import time
            current_time = int(time.time() * 1000)
            
            converted_data.add_to_metadata({
                'conversion_timestamp': current_time,
                'data_source': 'dali2_uplink'
            })
            
        except Exception as e:
            self.logger.error(f"Error adding timestamp data: {e}")
            
    def _add_quality_indicators(self, converted_data, data):
        """Add data quality indicators"""
        try:
            quality_score = 100
            
            # Check data completeness
            if hasattr(data, 'device_info'):
                device_info = data.device_info
                
                # Reduce quality if critical data is missing
                if device_info.actual_level is None:
                    quality_score -= 20
                if device_info.max_level is None:
                    quality_score -= 10
                if device_info.min_level is None:
                    quality_score -= 10
                    
                # Reduce quality if device has failures
                if device_info.ballast_failure:
                    quality_score -= 30
                if device_info.lamp_failure:
                    quality_score -= 30
                if device_info.power_failure:
                    quality_score -= 20
                    
                # Reduce quality if device is offline
                if hasattr(data, 'online') and not data.online:
                    quality_score = 0
                    
            # Add quality indicators to telemetry
            converted_data.add_to_telemetry({
                'data_quality_score': max(0, quality_score),
                'data_completeness': 'complete' if quality_score >= 80 else 'partial' if quality_score >= 50 else 'poor'
            })
            
        except Exception as e:
            self.logger.error(f"Error adding quality indicators: {e}")
