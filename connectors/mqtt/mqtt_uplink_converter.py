import logging
from abc import ABC, abstractmethod

log = logging.getLogger("extension")


class MqttUplinkConverter(ABC):

    @abstractmethod
    def convert(self, topic, body):
        pass
