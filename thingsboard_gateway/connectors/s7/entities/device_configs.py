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

from abc import ABC, abstractmethod

from thingsboard_gateway.connectors.s7.constants import (
    DEFAULT_POLL_PERIOD,
    DEFAULT_RETRY_DELAY,
    DEFAULT_MAX_DELAY,
    DEFAULT_HEARTBEAT_INTERVAL,
    DEFAULT_MAX_RETRIES,
    DEFAULT_AUTO_RECONNECT
)
from thingsboard_gateway.connectors.s7.entities.device_types import DeviceType


class DeviceConfigValidationError(Exception):
    def __init__(self, message: str) -> None:
        super().__init__(message)


class DeviceConfig(ABC):
    def __init__(self, logger, config: dict) -> None:
        self._log = logger
        self._format_base_config(config)
        self.device_name = config['deviceName']
        self.device_profile_name = config['deviceProfileName']
        self.address = config['address']
        self.port = config['port']
        self.poll_period = config['pollPeriod'] / 1000
        self.report_strategy_config = config.get('reportStrategy', {})
        self.uplink_converter_config = config.get('uplinkConverter')
        self.downlink_converter_config = config.get('downlinkConverter')
        self._format_config(config)
        self.attributes = config['attributes']
        self.timeseries = config['timeseries']
        self.datapoints = config['datapoints']
        self.server_side_rpc = config.get('serverSideRpc', [])
        self.attributes_updates = config.get('attributeUpdates', [])

    def _format_base_config(self, config: dict) -> None:
        """
        Validate base config field:
            - Device name
            - Device profile name
            - etc.
        """

        address = config.get('address')
        if not isinstance(address, str) or not address.strip():
            raise DeviceConfigValidationError('Address is invalid')

        try:
            port = int(config.get('port', -1))
            if not (0 <= port <= 65535):
                raise DeviceConfigValidationError(
                    'Port must be between 0 and 65535')
        except (ValueError, TypeError):
            raise DeviceConfigValidationError('Port must be an integer')

        device_name = config.get("deviceName")
        if not isinstance(device_name, str) or not device_name.strip():
            raise DeviceConfigValidationError(
                "Device name is missing or invalid")

        device_profile_name = config.get("deviceProfileName")
        if not isinstance(device_profile_name, str) or not device_profile_name.strip():
            raise DeviceConfigValidationError(
                "Device profile name is missing or invalid")

        if config.get('pollPeriod', DEFAULT_POLL_PERIOD) <= 0:
            self._log.warning(
                f"Invalid pollPeriod value for device '{device_name}'. Using default value: {DEFAULT_POLL_PERIOD} ms")
            config['pollPeriod'] = DEFAULT_POLL_PERIOD

    def _validate_datapoint_config_section(self, config_section, config_section_type):
        result = []

        if not isinstance(config_section, list):
            self._log.warning(
                f"Invalid config section for device '{self.device_name}'. Expected a list, got {type(config_section).__name__}. Skipping section.")  # noqa: E501
            return result

        for datapoint_config in config_section:
            if not isinstance(datapoint_config, dict):
                raise DeviceConfigValidationError(
                    'Each attribute and timeseries item must be a dictionary')

            if self._is_datapoint_valid(datapoint_config):
                datapoint_config['type_'] = config_section_type
                result.append(datapoint_config)

        return result

    def is_readable(self):
        return len(self.datapoints) > 0

    @abstractmethod
    def _format_config(self, config: dict) -> None:
        pass

    @abstractmethod
    def _is_datapoint_valid(self, datapoint_config) -> bool:
        pass


class PlcConfig(DeviceConfig):
    ALLOWED_DATAPOINT_TYPES = ('tag', 'data')

    def __init__(self, logger, config: dict) -> None:
        super().__init__(logger, config)
        self.device_type = DeviceType.PCL

        self.rack = config['rack']
        self.slot = config['slot']
        self.auto_reconnect = config['autoReconnect']
        self.max_retries = config['maxRetries']
        self.retry_delay = config['retryDelay']
        self.max_delay = config['maxDelay']
        self.heartbeat_interval = config['heartbeatInterval']

    def _format_config(self, config: dict) -> None:
        self._format_base_config_section(config)
        self._format_attributes_and_timeseries_config_section(config)

    def _format_base_config_section(self, config: dict) -> None:
        rack = config.get('rack')
        if not isinstance(rack, int) or rack < 0:
            raise DeviceConfigValidationError(
                'Rack must be a non-negative integer')

        slot = config.get('slot')
        if not isinstance(slot, int) or slot < 0:
            raise DeviceConfigValidationError(
                'Slot must be a non-negative integer')

        auto_reconnect = config.get('autoReconnect')
        if not isinstance(auto_reconnect, bool):
            self._log.warning(
                f"Invalid autoReconnect value for device '{self.device_name}'. Using default value: {DEFAULT_AUTO_RECONNECT}")  # noqa: E501
            config['autoReconnect'] = DEFAULT_AUTO_RECONNECT

        max_retries = config.get('maxRetries')
        if not isinstance(max_retries, int) or max_retries < 0:
            self._log.warning(
                f"Invalid maxRetries value for device '{self.device_name}'. Using default value: {DEFAULT_MAX_RETRIES}")  # noqa: E501
            config['maxRetries'] = DEFAULT_MAX_RETRIES

        retry_delay = config.get('retryDelay')
        if not isinstance(retry_delay, (int, float)) or retry_delay < 0:
            self._log.warning(
                f"Invalid retryDelay value for device '{self.device_name}'. Using default value: {DEFAULT_RETRY_DELAY}")  # noqa: E501
            config['retryDelay'] = DEFAULT_RETRY_DELAY

        max_delay = config.get('maxDelay')
        if not isinstance(max_delay, (int, float)) or max_delay < 0:
            self._log.warning(
                f"Invalid maxDelay value for device '{self.device_name}'. Using default value: {DEFAULT_MAX_DELAY}")  # noqa: E501
            config['maxDelay'] = DEFAULT_MAX_DELAY

        heartbeat_interval = config.get('heartbeatInterval')
        if not isinstance(heartbeat_interval, (int, float)) or heartbeat_interval < 0:
            self._log.warning(
                f"Invalid heartbeatInterval value for device '{self.device_name}'. Using default value: {DEFAULT_HEARTBEAT_INTERVAL}")  # noqa: E501
            config['heartbeatInterval'] = DEFAULT_HEARTBEAT_INTERVAL

    def _format_attributes_and_timeseries_config_section(self, config: dict) -> None:
        config['attributes'] = self._validate_datapoint_config_section(
            config.get('attributes'), 'attributes')
        config['timeseries'] = self._validate_datapoint_config_section(
            config.get('timeseries'), 'timeseries')
        config['datapoints'] = config['attributes'] + config['timeseries']

    def _is_datapoint_valid(self, datapoint_config):
        if 'key' not in datapoint_config:
            self._log.warning(
                f"Missing 'key' field for device '{self.device_name}'. Skipping datapoint: {datapoint_config}")  # noqa: E501
            return False

        datapoint_type = datapoint_config.get('type', 'none')
        if datapoint_type not in self.ALLOWED_DATAPOINT_TYPES:
            self._log.warning(
                f"Invalid datapoint type for device '{self.device_name}'. Skipping datapoint: {datapoint_config}. Allowed types: {self.ALLOWED_DATAPOINT_TYPES}")  # noqa: E501
            return False

        if datapoint_type == 'tag':
            if 'tag' not in datapoint_config:
                self._log.warning(
                    f"Missing 'tag' field for device '{self.device_name}'. Skipping datapoint: {datapoint_config}")  # noqa: E501
                return False

        if datapoint_type == 'data':
            if 'dbNumber' not in datapoint_config:
                self._log.warning(
                    f"Missing 'dbNumber' field for device '{self.device_name}'. Skipping datapoint: {datapoint_config}")  # noqa: E501
                return False

            if 'start' not in datapoint_config:
                self._log.warning(
                    f"Missing 'start' field for device '{self.device_name}'. Skipping datapoint: {datapoint_config}")  # noqa: E501
                return False

            if 'size' not in datapoint_config:
                self._log.warning(
                    f"Missing 'size' field for device '{self.device_name}'. Skipping datapoint: {datapoint_config}")  # noqa: E501
                return False

            # TODO: Add validation for 'dataType' field

        return True


class LogoConfig(DeviceConfig):
    ALLOWED_DATAPOINT_TYPES = ('vm')

    def __init__(self, logger, config: dict) -> None:
        super().__init__(logger, config)
        self.device_type = DeviceType.LOGO
        self.tsap_snap7 = config['tsapSnap7']
        self.tsap_logo = config['tsapLogo']
        # TODO: refactor this
        self.auto_reconnect = config.get('autoReconnect', DEFAULT_AUTO_RECONNECT)
        self.max_retries = config.get('maxRetries', DEFAULT_MAX_RETRIES)
        self.retry_delay = config.get('retryDelay', DEFAULT_RETRY_DELAY)
        self.max_delay = config.get('maxDelay', DEFAULT_MAX_DELAY)
        self.heartbeat_interval = config.get('heartbeatInterval', DEFAULT_HEARTBEAT_INTERVAL)

    def _format_config(self, config: dict) -> None:
        self._format_base_config_section(config)
        self._format_attributes_and_timeseries_config_section(config)

    def _format_base_config_section(self, config: dict) -> None:
        tsap_snap7 = config.get('tsapSnap7')
        if not isinstance(tsap_snap7, (int, float)) or tsap_snap7 < 0:
            raise DeviceConfigValidationError(
                'tsapSnap7 must be a non-negative number')

        tsap_logo = config.get('tsapLogo')
        if not isinstance(tsap_logo, (int, float)) or tsap_logo < 0:
            raise DeviceConfigValidationError(
                'tsapLogo must be a non-negative number')

    def _format_attributes_and_timeseries_config_section(self, config: dict) -> None:
        config['attributes'] = self._validate_datapoint_config_section(
            config.get('attributes'), 'attributes')
        config['timeseries'] = self._validate_datapoint_config_section(
            config.get('timeseries'), 'timeseries')
        config['datapoints'] = config['attributes'] + config['timeseries']

    def _is_datapoint_valid(self, datapoint_config):
        if 'key' not in datapoint_config:
            self._log.warning(
                f"Missing 'key' field for device '{self.device_name}'. Skipping datapoint: {datapoint_config}")  # noqa: E501
            return False

        datapoint_type = datapoint_config.get('type', 'none')
        if datapoint_type not in self.ALLOWED_DATAPOINT_TYPES:
            self._log.warning(
                f"Invalid datapoint type for device '{self.device_name}'. Skipping datapoint: {datapoint_config}. Allowed types: {self.ALLOWED_DATAPOINT_TYPES}")  # noqa: E501
            return False

        if 'vmAddress' not in datapoint_config:
            self._log.warning(
                f"Missing 'vmAddress' field for device '{self.device_name}'. Skipping datapoint: {datapoint_config}")  # noqa: E501
            return False

        return True
