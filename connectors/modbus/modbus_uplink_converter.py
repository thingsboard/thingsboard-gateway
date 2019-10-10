from abc import ABC, abstractmethod


class ModbusUplinkConverter(ABC):

    @abstractmethod
    def convert(self, device_responses):
        pass
