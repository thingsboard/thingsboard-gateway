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
from logging import getLogger
from threading import Thread
from time import sleep


class Connector(ABC):

    @abstractmethod
    def open(self):
        pass

    @abstractmethod
    def close(self):
        pass

    @abstractmethod
    def get_id(self):
        pass

    @abstractmethod
    def get_name(self):
        pass

    @abstractmethod
    def get_type(self):
        pass

    @abstractmethod
    def get_config(self):
        pass

    @abstractmethod
    def is_connected(self):
        pass

    @abstractmethod
    def is_stopped(self):
        pass

    @abstractmethod
    def on_attributes_update(self, content):
        pass

    @abstractmethod
    def server_side_rpc_handler(self, content):
        pass


class DummyCustomConnector(Connector, Thread):
    """Placeholder connector started instead of a custom connector whose class could not be
    loaded (e.g. wrong or missing "class" name in the configuration), so the gateway still
    starts and keeps reporting the reason in the logs instead of silently dropping it."""

    WARNING_PERIOD_SECONDS = 30

    def __init__(self, gateway, config, connector_type):
        super().__init__()
        self.daemon = True
        self._gateway = gateway
        self._config = config
        self._connector_type = connector_type
        self.name = 'Dummy Custom Connector'
        self.__log = getLogger('service')
        self.__stopped = True
        self.__connected = False

    def open(self):
        self.__stopped = False
        self.start()

    def _get_requested_class_name(self):
        for connector_config in self._gateway.config.get('connectors', []):
            if connector_config.get('name') == self.get_name():
                return connector_config.get('class')
        return None

    def run(self):
        self.__connected = True
        while not self.__stopped:
            self.__log.warning(
                "Connector '%s' is not implemented: class '%s' was not found. "
                "Implement the real custom connector class to start receiving/sending data.",
                self.get_name(), self._get_requested_class_name())
            sleep(self.WARNING_PERIOD_SECONDS)

    def close(self):
        self.__stopped = True
        self.__connected = False

    def get_id(self):
        return self._config.get('id')

    def get_name(self):
        return self._config.get('name')

    def get_type(self):
        return self._connector_type

    def get_config(self):
        return self._config

    def is_connected(self):
        return self.__connected

    def is_stopped(self):
        return self.__stopped

    def on_attributes_update(self, content):
        pass

    def server_side_rpc_handler(self, content):
        self.__log.warning(
            "Cannot process RPC request for connector '%s': real custom connector class '%s' is not implemented.",
            self.get_name(), self._get_requested_class_name())
        return {"error": f"Custom connector '{self.get_name()}' is not implemented."}

