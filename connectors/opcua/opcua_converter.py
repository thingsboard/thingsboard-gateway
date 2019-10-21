import logging
from abc import ABC, abstractmethod

log = logging.getLogger("extension")


class OpcUaConverter(ABC):
    @abstractmethod
    def convert(self, path, data):
        pass
