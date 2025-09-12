# DALI-2 Connector for ThingsBoard Gateway

## Overview

The DALI-2 Connector is a custom connector for the ThingsBoard IoT Gateway that enables integration with DALI-2 (Digital Addressable Lighting Interface) lighting control systems. DALI-2 is an international standard (IEC 62386) for digital lighting control that supports various device types including ballasts, LED drivers, sensors, and control devices.

## Features

### Supported Device Types
- **Ballasts/LED Drivers** - Light sources with dimming capabilities
- **Control Devices** - Sensors, switches, buttons
- **Application Controllers** - Central control units
- **Input Devices** - Push buttons, sliders, sensors

### Communication Interfaces
- **Serial Interface** - Direct connection to DALI-2 bus via serial port
- **TCP Interface** - Connection to DALI-2 gateway devices via TCP/IP

### Data Collection
- **Telemetry Data** - Real-time lighting levels, power status, device health
- **Attributes** - Device configuration, manufacturer info, group assignments
- **RPC Commands** - Remote control of lighting levels, scenes, and settings

### Advanced Features
- **Complete Bus Mapping** - Comprehensive scanning and identification of all DALI-2 devices
- **Auto-Discovery** - Automatic detection without manual configuration
- **Device Type Detection** - Automatic identification of device types (ballasts, sensors, etc.)
- **Topology Mapping** - Complete bus topology with groups and scenes
- **Health Monitoring** - Device status monitoring and failure detection
- **Power Calculation** - Estimated power consumption based on dimming levels
- **Scene Management** - Support for DALI-2 scene control
- **Group Control** - Management of device groups
- **Fade Control** - Configurable fade times and rates

## Installation

### Prerequisites
- ThingsBoard Gateway installed and configured
- DALI-2 compatible hardware (ballasts, drivers, sensors)
- Serial port or TCP connection to DALI-2 bus

### Files to Add
1. Copy the connector files to the gateway:
   ```
   thingsboard_gateway/connectors/dali2/
   ├── __init__.py
   ├── dali2_connector.py
   ├── dali2_converter.py
   └── entities/
       ├── __init__.py
       ├── dali2_device.py
       └── dali2_connector_config.py
   ```

2. Copy the extension files:
   ```
   thingsboard_gateway/extensions/dali2/
   ├── __init__.py
   └── dali2_uplink_converter.py
   ```

3. Copy the configuration files:
   ```
   thingsboard_gateway/config/
   ├── dali2.json
   └── dali2_test.json
   ```

## Configuration

### Main Gateway Configuration

Add the DALI-2 connector to your `tb_gateway.json`:

```json
{
  "thingsboard": {
    // ... existing configuration
  },
  "connectors": [
    {
      "name": "DALI-2 Connector",
      "type": "dali2",
      "class": "DALI2Connector",
      "configuration": "dali2.json"
    }
  ]
}
```

### Auto-Discovery Configuration (Recommended)

For automatic device discovery without manual configuration:

```json
{
  "name": "DALI-2 Auto-Discovery Connector",
  "type": "dali2",
  "buses": [
    {
      "bus_id": "main_bus",
      "interface_type": "serial",
      "connection_params": {
        "port": "/dev/ttyUSB0",
        "baudrate": 9600
      },
      "enable_discovery": true,
      "discovery_interval_ms": 30000
    }
  ],
  "devices": [],
  "global_settings": {
    "enable_auto_discovery": true,
    "enable_comprehensive_bus_mapping": true,
    "detailed_device_interrogation": true,
    "auto_configure_discovered_devices": true
  }
}
```

### DALI-2 Connector Configuration

The `dali2.json` configuration file contains:

#### Bus Configuration
```json
{
  "buses": [
    {
      "bus_id": "main_bus",
      "interface_type": "serial",
      "connection_params": {
        "port": "/dev/ttyUSB0",
        "baudrate": 9600,
        "bytesize": 8,
        "parity": "N",
        "stopbits": 1,
        "timeout": 1.0
      },
      "poll_interval_ms": 5000,
      "command_timeout_ms": 1000,
      "max_retries": 3,
      "enable_discovery": true,
      "discovery_interval_ms": 30000
    }
  ]
}
```

#### Device Configuration
```json
{
  "devices": [
    {
      "device_name": "dali2_light_1",
      "short_address": 1,
      "device_type": "dali2_ballast",
      "bus_id": "main_bus",
      "poll_interval_ms": 2000,
      "enable_telemetry": true,
      "enable_attributes": true,
      "enable_rpc": true,
      "telemetry_keys": [
        "actual_level",
        "max_level",
        "min_level",
        "power_on",
        "ballast_failure",
        "lamp_failure",
        "brightness_percentage",
        "estimated_power_watts",
        "health_score"
      ],
      "rpc_methods": [
        "set_level",
        "set_scene",
        "set_fade_time",
        "set_fade_rate",
        "query_status"
      ],
      "scenes": {
        "0": 254,
        "1": 200,
        "2": 150,
        "3": 100
      },
      "groups": [0, 1],
      "custom_attributes": {
        "location": "Office Room 1",
        "zone": "Work Area",
        "fixture_type": "LED Panel",
        "wattage": 36
      }
    }
  ]
}
```

## Bus Mapping & Auto-Discovery

### Complete Bus Mapping

The DALI-2 connector includes advanced bus mapping capabilities that automatically discover and identify all devices on the DALI-2 bus:

#### **Automatic Discovery Process:**

1. **Quick Scan** - Scans all addresses 0-63 for device presence
2. **Device Interrogation** - Queries each discovered device for detailed information
3. **Type Detection** - Automatically determines device type (ballast, sensor, etc.)
4. **Capability Mapping** - Identifies device capabilities and features
5. **Topology Building** - Maps groups, scenes, and device relationships

#### **Discovery Features:**

- **No Manual Configuration Required** - Devices are discovered automatically
- **Device Type Detection** - Automatic identification of ballasts, sensors, control devices
- **Capability Assessment** - Determines dimming ranges, scene support, group assignments
- **Health Scoring** - Calculates device health and response quality
- **Topology Mapping** - Complete bus structure with groups and scenes

#### **Bus Map Output:**

```
=== DALI-2 BUS MAP REPORT ===
Total devices discovered: 8
Bus health score: 0.95

Device Types:
  ballast: 5
  led_driver: 2
  sensor: 1

Groups:
  Group 0: [1, 2, 3, 4, 5]
  Group 1: [1, 2]
  Group 2: [6, 7]

Scenes:
  Scene 0: 8 devices
  Scene 1: 6 devices
  Scene 2: 4 devices

Device Details:
  Address 1: ballast (confidence: 1.00, response: 15.2ms)
  Address 2: ballast (confidence: 1.00, response: 14.8ms)
  Address 3: led_driver (confidence: 0.95, response: 18.1ms)
  Address 10: sensor (confidence: 0.90, response: 22.3ms)
```

#### **Configuration for Auto-Discovery:**

```json
{
  "buses": [
    {
      "bus_id": "main_bus",
      "interface_type": "serial",
      "connection_params": {
        "port": "/dev/ttyUSB0",
        "baudrate": 9600
      },
      "enable_discovery": true,
      "discovery_interval_ms": 30000
    }
  ],
  "devices": [],
  "global_settings": {
    "enable_auto_discovery": true,
    "enable_comprehensive_bus_mapping": true,
    "detailed_device_interrogation": true,
    "auto_configure_discovered_devices": true
  }
}
```

### Manual Configuration (Optional)

You can still manually configure specific devices if needed:

```json
{
  "devices": [
    {
      "device_name": "office_light_1",
      "short_address": 1,
      "device_type": "dali2_ballast",
      "bus_id": "main_bus",
      "custom_attributes": {
        "location": "Office Room 1",
        "zone": "Work Area"
      }
    }
  ]
}
```

## Usage

### Telemetry Data

The connector collects the following telemetry data:

- **actual_level** - Current dimming level (0-254)
- **max_level** - Maximum dimming level
- **min_level** - Minimum dimming level
- **power_on** - Power status (true/false)
- **ballast_failure** - Ballast failure status
- **lamp_failure** - Lamp failure status
- **power_failure** - Power failure status
- **fade_running** - Fade operation status
- **fade_time** - Current fade time setting
- **fade_rate** - Current fade rate setting
- **operating_mode** - Device operating mode
- **brightness_percentage** - Calculated brightness percentage
- **estimated_power_watts** - Estimated power consumption
- **health_score** - Device health score (0-100)
- **uptime_seconds** - Device uptime

### Attributes

Device attributes include:

- **short_address** - DALI-2 short address
- **device_type** - Device type (ballast, sensor, etc.)
- **manufacturer_id** - Manufacturer identifier
- **product_id** - Product identifier
- **application_version** - Application version
- **hardware_version** - Hardware version
- **firmware_version** - Firmware version
- **groups** - Assigned groups
- **scenes_count** - Number of configured scenes

### RPC Commands

Available RPC commands:

#### set_level
Set the dimming level of a device:
```json
{
  "method": "set_level",
  "params": {
    "level": 128
  }
}
```

#### set_scene
Activate a scene:
```json
{
  "method": "set_scene",
  "params": {
    "scene": 1
  }
}
```

#### set_fade_time
Set fade time:
```json
{
  "method": "set_fade_time",
  "params": {
    "fade_time": 5
  }
}
```

#### set_fade_rate
Set fade rate:
```json
{
  "method": "set_fade_rate",
  "params": {
    "fade_rate": 3
  }
}
```

#### query_status
Query device status:
```json
{
  "method": "query_status",
  "params": {}
}
```

### Attribute Updates

You can update device attributes from ThingsBoard:

- **fade_time** - Update fade time setting
- **fade_rate** - Update fade rate setting
- **scene_X** - Update scene levels (where X is scene number 0-15)

## Testing

### Test Configuration

Use the `dali2_test.json` configuration for testing:

```json
{
  "name": "DALI-2 Test Connector",
  "type": "dali2",
  "logLevel": "DEBUG",
  "enableRemoteLogging": false,
  "buses": [
    {
      "bus_id": "test_bus",
      "interface_type": "serial",
      "connection_params": {
        "port": "/dev/ttyUSB0",
        "baudrate": 9600
      },
      "poll_interval_ms": 2000,
      "command_timeout_ms": 500
    }
  ],
  "devices": [
    {
      "device_name": "test_light_1",
      "short_address": 1,
      "device_type": "dali2_ballast",
      "bus_id": "test_bus",
      "poll_interval_ms": 1000
    }
  ]
}
```

### Testing Steps

1. **Hardware Setup**
   - Connect DALI-2 devices to the bus
   - Connect serial port or TCP gateway to the system
   - Ensure proper power supply and bus termination

2. **Configuration**
   - Update the configuration file with correct port/connection details
   - Set appropriate device addresses
   - Configure polling intervals

3. **Start Gateway**
   - Start the ThingsBoard Gateway
   - Check logs for successful connector initialization
   - Verify device discovery

4. **Verify Data Flow**
   - Check ThingsBoard for incoming telemetry data
   - Test RPC commands
   - Verify attribute updates

## Troubleshooting

### Common Issues

1. **Connection Failed**
   - Check serial port permissions
   - Verify port configuration (baudrate, parity, etc.)
   - Ensure DALI-2 bus is properly terminated

2. **No Device Discovery**
   - Check device power supply
   - Verify DALI-2 bus wiring
   - Check device addresses and configuration

3. **RPC Commands Not Working**
   - Verify device supports the command
   - Check device address configuration
   - Ensure device is online and responding

4. **Data Not Appearing in ThingsBoard**
   - Check gateway logs for errors
   - Verify device configuration
   - Check ThingsBoard connection

### Log Analysis

Enable DEBUG logging to troubleshoot issues:

```json
{
  "logLevel": "DEBUG",
  "enable_command_logging": true,
  "enable_response_logging": true,
  "enable_error_logging": true
}
```

### Performance Tuning

- Adjust `poll_interval_ms` based on update frequency needs
- Increase `command_timeout_ms` for slow devices
- Use `max_retries` to handle temporary communication issues
- Enable `enable_continuous_polling` for real-time updates

## DALI-2 Protocol Details

### Command Types

The connector supports standard DALI-2 commands:

- **Direct Arc Power** (0x00-0xEF) - Set dimming level
- **Query Commands** (0x90-0xEF) - Read device status
- **Configuration Commands** (0xC0-0xEF) - Configure device settings
- **Scene Commands** (0xE0-0xEF) - Scene control
- **Group Commands** (0xE0-0xEF) - Group management

### Device Addressing

- **Short Addresses** - 0-63 for individual devices
- **Broadcast** - Address 255 for all devices
- **Groups** - 0-15 for group addressing
- **Scenes** - 0-15 for scene control

### Data Formats

- **Level Values** - 0-254 (0 = off, 1-254 = dimming levels)
- **Status Responses** - Bit-mapped status information
- **Configuration Data** - Device-specific parameters

## Support

For issues and questions:

1. Check the ThingsBoard Gateway documentation
2. Review DALI-2 standard (IEC 62386)
3. Check device manufacturer documentation
4. Enable debug logging for detailed troubleshooting

## License

This connector is licensed under the Apache License 2.0, same as the ThingsBoard Gateway.
