from abc import ABC, abstractmethod


class ModbusConverter(ABC):
    @abstractmethod
    def convert(self, data, config):
        pass
