from abc import ABC,abstractmethod


class Connector(ABC):

    @abstractmethod
    def open(self):
        pass

    @abstractmethod
    def close(self):
        pass

    @abstractmethod
    def getName(self):
        pass
