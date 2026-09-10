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

from abc import abstractmethod
from asyncio import Queue, sleep
from time import monotonic

from thingsboard_gateway.connectors.s7.entities.device_configs import (
    DeviceConfig,
    LogoConfig,
    PlcConfig,
    DeviceConfigValidationError
)
from thingsboard_gateway.connectors.s7.entities.device_types import (
    DeviceType,
    DeviceTypes,
)
from thingsboard_gateway.connectors.s7.s7_downlink_converter import S7DownlinkConverter
from thingsboard_gateway.connectors.s7.s7_uplink_converter import S7UplinkConverter
from thingsboard_gateway.tb_utility.tb_loader import TBModuleLoader
from thingsboard_gateway.connectors.s7.constants import (
    UPLINK_PREFIX,
    DOWNLINK_PREFIX,
    CONNECTOR_TYPE,
)
from thingsboard_gateway.connectors.s7.entities.logo_client import LogoClient

from snap7 import Client as PlcClient


class Device:
    def __init__(self, logger, converter_logger, config: 'DeviceConfig', reading_request_queue: Queue) -> None:
        self.config = config
        self.stopped = False
        self._reading_request_queue = reading_request_queue
        self._log = logger
        self.uplink_converter = self._load_converter(UPLINK_PREFIX, converter_logger)
        self.downlink_converter = self._load_converter(DOWNLINK_PREFIX, converter_logger)
        self._client: PlcClient | LogoClient = None

    def _load_converter(self, converter_type: str, converter_logger):
        try:
            config_attr = converter_type + '_converter_config'
            custom_converter = getattr(self.config, config_attr, None)

            if isinstance(custom_converter, str):
                converter_class = TBModuleLoader.import_module(CONNECTOR_TYPE, custom_converter)
                if converter_type == DOWNLINK_PREFIX:
                    return converter_class(converter_logger)
                return converter_class(converter_logger, self.config)

            if converter_type == DOWNLINK_PREFIX:
                return S7DownlinkConverter(converter_logger)
            return S7UplinkConverter(converter_logger, self.config)
        except Exception as e:
            self._log.exception(
                'Failed to load %s converter for %s device: %s',
                converter_type, self.config.device_name, e)

    @staticmethod
    def create_device_from_config(logger, converter_logger, config: dict, reading_request_queue) -> 'Device':
        device_type: str = config.get('type', '').upper()

        if device_type == DeviceType.PCL.value:
            device_config = PlcConfig(logger, config)
            return PLC(logger, converter_logger, device_config, reading_request_queue)

        if device_type == DeviceType.LOGO.value:
            device_config = LogoConfig(logger, config)
            return Logo(logger, converter_logger, device_config, reading_request_queue)

        raise DeviceConfigValidationError(
            f'Invalid device type. Available: {DeviceTypes}')

    async def run(self) -> None:
        if not self.config.is_readable():
            self._log.warning(
                f"Device '{self.config.device_name}' has no readable attributes or timeseries. Skipping polling.")
            self.stop()
            return

        next_poll_time = 0

        while not self.stopped:
            current_time = monotonic()
            if current_time >= next_poll_time:
                self._reading_request_queue.put_nowait(self)
                next_poll_time = current_time + self.config.poll_period

            sleep_time = max(0.0, next_poll_time - current_time)
            await sleep(sleep_time)

    def stop(self) -> None:
        try:
            self.stopped = True
            self._client.disconnect()
            self._log.info(
                f"Disconnected from Logo device '{self.config.device_name}' at {self.config.address}:{self.config.port}")  # noqa: E501
        except Exception as e:
            self._log.error(
                f"Failed to disconnect from Logo device '{self.config.device_name}' at {self.config.address}:{self.config.port}: {e}")  # noqa: E501

    @abstractmethod
    async def connect(self) -> None:
        pass

    @abstractmethod
    async def read_configured_data(self):
        pass

    @abstractmethod
    async def read(self, datapoint_config: dict):
        """
        datapoint_config: {
            "type": "vm|data|tag",
            "vmAddress": "VW64",
            "tag": "DB1.DBW6:INT",
            "dbNumber": 1,
            "start": 0,
            "size": 2,
        }
        """

        pass


class PLC(Device):
    def __init__(self, logger, converter_logger, config: 'PlcConfig', reading_request_queue: Queue) -> None:
        super().__init__(logger, converter_logger, config, reading_request_queue)
        self._client = PlcClient(
            auto_reconnect=config.auto_reconnect,
            max_retries=config.max_retries,
            retry_delay=config.retry_delay,
            max_delay=config.max_delay,
            heartbeat_interval=config.heartbeat_interval
        )

    async def connect(self) -> None:
        try:
            self._client.connect(
                self.config.address,
                rack=self.config.rack,
                slot=self.config.slot,
                tcp_port=self.config.port
            )
            self._log.info(
                f"Connected to PLC device '{self.config.device_name}' at {self.config.address}:{self.config.port}")
        except Exception as e:
            self._log.error(
                f"Failed to connect to PLC device '{self.config.device_name}' at {self.config.address}:{self.config.port}: {e}")  # noqa: E501
            raise

    def write(self, config, data):
        request_type = config.get('type')
        if request_type == 'data':
            return self._client.db_write(config['dbNumber'], config['start'], data)
        elif request_type == 'tag':
            return self._client.write_tag(config['tag'], data)
        else:
            raise ValueError(
                f"Unsupported request type '{request_type}' for writing data to PLC device '{self.config.device_name}'")

    async def read_configured_data(self):
        results = []

        for datapoint in self.config.datapoints:
            value = None

            try:
                value = self.read(datapoint)
            except Exception as e:
                self._log.error(
                    f"Error reading datapoint '{datapoint['key']}' from PLC device '{self.config.device_name}': {e}")  # noqa: E501
            finally:
                results.append(value)

        return results

    def read(self, config):
        request_type = config.get('type')
        if request_type == 'data':
            return self._client.db_read(config['dbNumber'], config['start'], config['size'])
        elif request_type == 'tag':
            return self._client.read_tag(config['tag'])
        else:
            raise ValueError(
                f"Unsupported request type '{request_type}' for reading data from PLC device '{self.config.device_name}'")  # noqa: E501


class Logo(Device):
    def __init__(self, logger, converter_logger, config: 'LogoConfig', reading_request_queue: Queue) -> None:
        super().__init__(logger, converter_logger, config, reading_request_queue)
        self._client = LogoClient(
            auto_reconnect=config.auto_reconnect,
            max_retries=config.max_retries,
            retry_delay=config.retry_delay,
            max_delay=config.max_delay,
            heartbeat_interval=config.heartbeat_interval
        )

    async def connect(self) -> None:
        try:
            self._client.connect(
                self.config.address,
                tsap_snap7=self.config.tsap_snap7,
                tsap_logo=self.config.tsap_logo,
                tcp_port=self.config.port
            )
            self.stopped = False
            self._log.info(
                f"Connected to Logo device '{self.config.device_name}' at {self.config.address}:{self.config.port}")
        except Exception as e:
            self._log.error(
                f"Failed to connect to Logo device '{self.config.device_name}' at {self.config.address}:{self.config.port}: {e}")  # noqa: E501
            raise

    async def read_configured_data(self):
        results = []

        for datapoint in self.config.datapoints:
            try:
                value = self.read(datapoint)
                results.append(value)
            except Exception as e:
                self._log.error(
                    f"Error reading datapoint '{datapoint['key']}' from Logo device '{self.config.device_name}': {e}")  # noqa: E501
                results.append(None)

        return results

    def read(self, config):
        return self._client.read(config['vmAddress'])

    def write(self, config, data):
        return self._client.write(config['vmAddress'], data)
