from abc import ABC, abstractmethod


class ModbusUplinkConverter(ABC):

    @abstractmethod
    def convert(self, input_bytes):
        pass
