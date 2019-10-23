import logging
from abc import ABC, abstractmethod

log = logging.getLogger('converter')


class ModbusConverter(ABC):
    @abstractmethod
    def convert(self, data, config):
        pass
