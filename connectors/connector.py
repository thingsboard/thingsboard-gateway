from abc import ABC,abstractmethod


class Connector(ABC):

    @abstractmethod
    def open(self, gateway, config):
        pass

    @abstractmethod
    def close(self, config):
        pass

