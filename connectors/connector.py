from abc import ABC, abstractmethod


class Connector(ABC):

    @abstractmethod
    def open(self):
        pass

    @abstractmethod
    def close(self):
        pass

    @abstractmethod
    def get_name(self):
        pass

    @abstractmethod
    def on_attributes_update(self):
        pass

    @abstractmethod
    def server_side_rpc_handler(self, content):
        pass
